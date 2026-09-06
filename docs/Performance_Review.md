# Performance Review

This note tracks repository-level performance and validation findings from the
July 2026 review.

## Findings

- GitHub Actions sanitizer jobs used lowercase matrix values to build CMake
  options, so `ENABLE_ASAN` and `ENABLE_UBSAN` were not actually set.
- `t_map` used open addressing with tombstones but did not track tombstone
  pressure. Repeated remove-heavy workloads could accumulate dead slots and
  lengthen future probe chains.
- TTL expiry removes entries in batches and is a remove-heavy caller of `t_map`,
  so it benefits directly from explicit map compaction after successful expiry.

## Applied Plan

- Make the sanitizer matrix carry exact CMake option names.
- Track `t_map` tombstones, clean them during resize/clear/destroy, and expose
  `t_map_compact()` for callers that know a remove-heavy phase just completed.
- Compact the TTL map after expiry batches while preserving the lazy stale-heap
  strategy that avoids per-update heap deletion.
- Add unit coverage for map replacement, removal, tombstone compaction, and
  post-compaction insertion.
- Keep CI on Linux, macOS, and Windows (default VS generator, x64). Leftover POSIX
  helpers (`t_config`, `t_log`, `t_ratelimit`, `t_flowcontrol`) now use
  `t_file` / `t_mutex`. Tests use `t_thread` and `t_socket_pair`.
  Windows evloop is WSAPoll readiness (O(n) in registered fds, default
  cap 1024) so `t_conn` keeps the same recv/send path as epoll. Coroutine
  SysV assembly is not used on Windows; `t_coro_create` returns NULL.
- Client ACK status/seq are `t_atomic_int`. Tests poll `ack_seq` from
  the harness thread; a new seq is the fail-closed signal that an ACK
  arrived. No extra copy on the I/O path.
- Windows WSAPoll snapshots the fd table under `t_mutex` and waits
  without the lock. The hot path is still one `recv`/`send` per
  readiness event; the lock is O(n) copy of registered fds, not per
  byte.
- Wire protocol decode aliases the frame (no extra payload copy). The protocol
  server rate-limits in O(1) before broker work and caps accept fan-in.
- Durable WAL is append-only (PUT/DEL). Default fsync every 32 records so the
  hot path is not a syscall per message; tests use `sync_every=1`.
  Clustered brokers use the Raft log as the WAL (one append, no
  per-queue double-write on apply). Majority commit is O(peers).
  Applied prefix is snapshotted (`TRFS`) and dropped only after every
  peer has the prefix, so restart is O(live messages + tail). A peer
  behind the snapshot gets one `InstallSnapshot` instead of the prefix.
- Protocol `PUSH` takes a per-session credit (`t_flowcontrol`, default 64,
  no timed refill). FIFO/priority consume into inflight; `CONFIRM` acks and
  `REJECT` requeues. `CONFIRM`/`REJECT` skip the token bucket so
  backpressure cannot deadlock.

## Verification

Run these commands before publishing changes:

```bash
cmake --build build -j
ctest --test-dir build --output-on-failure -j
cmake -B /tmp/opencode/transit-verify -S . -DCMAKE_EXPORT_COMPILE_COMMANDS=ON -DBUILD_EXAMPLES=OFF
cmake --build /tmp/opencode/transit-verify -j
ctest --test-dir /tmp/opencode/transit-verify --output-on-failure -j
cmake -B /tmp/opencode/transit-asan -S . -DENABLE_ASAN=ON -DBUILD_EXAMPLES=OFF
cmake --build /tmp/opencode/transit-asan -j
ctest --test-dir /tmp/opencode/transit-asan --output-on-failure -j
cmake -B /tmp/opencode/transit-ubsan -S . -DENABLE_UBSAN=ON -DBUILD_EXAMPLES=OFF
cmake --build /tmp/opencode/transit-ubsan -j
ctest --test-dir /tmp/opencode/transit-ubsan --output-on-failure -j
```
