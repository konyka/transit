# Windows Port and CI

Transit’s remaining production gap was not a new queue feature. The
library already had Windows mmap, Winsock, WAL, Raft, signals, the
thread pool, and admin HTTP. What still blocked a trustworthy Windows
build was leftover POSIX in helpers and tests, plus an event loop that
could not deliver socket readiness.

This note is the design that closed that gap. Goals, in order:
**performance first**, then **fail closed**, then **TDD**.

## Remaining work (before this change)

1. `t_config` file I/O used POSIX `open`/`read`.
2. `t_log`, `t_ratelimit`, and `t_flowcontrol` used `pthread_mutex`.
3. Tests used `pthread`, `socketpair(AF_UNIX)`, `usleep`, `mkstemp`,
   `pipe`, and `arpa/inet.h`.
4. The IOCP evloop only woke via `PostQueuedCompletionStatus`. It never
   posted overlapped I/O, so `t_conn` / `t_server` could not see reads.
5. SysV coroutine assembly is the wrong ABI on Windows x64.
6. GitHub Actions had no Windows job.

Non-goals stay the same: TLS, extra SDKs, compression, WAN mesh.

## Design

### Threads (`t_thread`)

Thin spawn/join/yield over `CreateThread` / `pthread_create`. Stack
allocated, no hidden heap besides the one Windows start thunk. Tests
and benches use this instead of `pthread_t`. The work-stealing pool
keeps its own internals so the hot path does not grow another hop.

### Connected sockets (`t_socket_pair`)

POSIX keeps `socketpair(AF_UNIX)` (no extra copy, no TIME_WAIT).
Windows binds **127.0.0.1** only, accepts one connection, then checks:

- peer address is loopback
- an 8-byte self-token sent on the client is received on the accepted
  fd (rejects a stolen local accept)

Both ends are non-blocking so they match `t_socket_create`. Used for
tests, evloop wakeup, and the former `pipe()` I/O test.

### Event loop (WSAPoll readiness)

The rest of Transit is readiness-based: epoll/kqueue mark a fd readable,
then `t_conn` calls `recv`/`send`. Real IOCP would need overlapped
`WSARecv`/`WSASend` in every I/O path. That is a large, copy-prone
rewrite.

Windows therefore uses **WSAPoll** with the same `T_EV_READ` /
`T_EV_WRITE` contract. Wakeup is a loopback `t_socket_pair` (same
shape as the POSIX pipe). Cost is O(n) in registered fds, capped by
`max_conns` (default 1024). That is cheaper than a wrong IOCP layer
that never completes.

`t_conn_create` / `t_conn_send` add and mod fds from the harness
thread while `poll` runs. The registration table is an SRWLOCK-guarded
array. `poll` copies `fd` + `t_evio*` under the lock, then
`WSAPoll`s the snapshot (lock not held during the wait). Iterating
`st->n` after a concurrent add used to read uninitialized `revents`
and close the new connection. `add`/`mod`/`del` write the wakeup
byte so a blocked poll rebuilds the snapshot.

### Leftover helpers

| Helper | Change | Safety |
|--------|--------|--------|
| `t_config_parse_file` | `t_file` read, 16 MiB cap | Fail closed on missing/oversize |
| `t_log` | `t_mutex` + `GetLocalTime` | Same serialized stderr |
| `t_ratelimit` / `t_flowcontrol` | `t_mutex` (SRWLOCK) | Same token/credit rules |
| Coroutine | `t_coro_create` returns NULL | No wrong-ABI stack smash |

`T_MUTEX_INIT` lets the log lock exist before `t_log_init`. `T_PRINTF`
and `T_PREFETCH` are no-ops on MSVC. `strdup` maps to `_strdup`.

### Tests

One portable test harness: `t_thread`, `t_socket_pair`, `t_file`,
`t_time_sleep_us`. MSVC registers `T_TEST` via `.CRT$XCU` so
constructors still run. Coroutine tests assert `T_HAVE_CORO_ASM == 0`
when create returns NULL.

## TDD

1. `test_thread` — null args fail closed; spawn/join increments; yield.
2. `test_net_tcp` / `socket_pair_echo` — pair writes 2 bytes, other end
   reads them.
3. `test_config` — parse a file written with `t_file`; missing file
   returns -1.
4. `client_ack_seq_starts_zero` — `t_client_ack_seq` is 0 until a real
   `ACK` is decoded. `test_server` waits on that sequence, not on
   `last_status == 0` (which is the create-time default).
5. Existing suites stay the contract: same ACK/AUTH/WAL/credit
   behaviour, now compiled without POSIX.

## CI

`windows-latest` lets CMake pick the installed Visual Studio generator
(`-A x64`, Release) and runs `ctest -C Release`. Do not pin
`Visual Studio 17 2022`: as of June 2026 that image is VS 2026.
Sanitizer jobs stay Linux-only. Loopback bind and PSK rules are
unchanged.

CMake is `project(transit C)` on every host. `enable_language(ASM)`
runs only when `NOT WIN32`. Listing `ASM` in `project()` makes CMake 4
on MSVC fail configure (`CMAKE_ASM_COMPILER` unset; CMP0194). The
coroutine `.S` files are already skipped on Windows.

Real `cl.exe` always uses `_Interlocked*` from `<intrin.h>` (no
`windows.h`). `/std:c11` sets `T_C11`, and VS 2026 may drop
`__STDC_NO_ATOMICS__` while `<stdatomic.h>` still wants
`/experimental:c11atomics`. clang-cl and GCC keep stdatomic or
`__sync`. `T_LOG_*` take `__VA_ARGS__` so a format-only call has no
trailing comma (MSVC's default preprocessor rejects GNU
`##__VA_ARGS__`). `/Zc:preprocessor` is on for `cl` (not clang-cl).
`/W4 /WX` stays; C4204/C4221
(C99 aggregates) and C4244/C4267 (size_t narrowing Clang already
allows) are disabled.

## Verification

```bash
cmake -B build -DBUILD_EXAMPLES=ON
cmake --build build
ctest --test-dir build --output-on-failure -j
```

On Windows use `cmake -B build -A x64` (default VS generator) and
`ctest -C Release`.
