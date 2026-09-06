# Transit

High-performance distributed message queue in pure C (C99-C23 compatible).

Reimplements Bloomberg's [BlazingMQ](https://github.com/bloomberg/blazingmq) feature set with zero external dependencies.

## Architecture

```
src/
├── core/        compiler, atomics, time, mutex, spinlock, rwlock, error, log,
│                version, signal, config, thread, graceful shutdown
├── memory/      tiered pool, slab allocator, refcount buffer, arena
├── collections/ vec, map, list, SPSC ringbuf, Vyukov MPMC, priority queue
├── event/       epoll / kqueue / IOCP backends (vtable), min-heap timer
├── threadpool/  work-stealing thread pool with per-worker MPMC queues
├── coroutine/   x86_64 and AArch64 assembly context switch
├── net/         non-blocking socket, TCP, framed conn, protocol server,
│                admin HTTP, ratelimit
├── protocol/    binary wire protocol (16-byte header), CRC32C, HMAC-SHA256 AUTH, JOIN, typed payloads
├── storage/     in-memory + file-backed hashmap, mmap (POSIX/Windows), WAL
├── queue/       FIFO/priority/broadcast, topic router (* #), flowcontrol,
│                DLQ, message TTL (heap+map+compact), consumer groups
├── session/     session lifecycle, activity tracking, timeout detection
├── client/      in-process stub + TCP dial (`t_client_dial`)
├── broker/      domain management, publish/subscribe, dispatcher
└── cluster/     Raft RPCs + persistent log, peer transport, leader-only publish
```

## Build

```bash
cmake -B build -DBUILD_EXAMPLES=ON
cmake --build build
ctest --test-dir build --output-on-failure
```

Requires GCC or Clang with C99+ support. Builds with `-Wall -Wextra -Werror`.

### Sanitizers

```bash
cmake -B build-asan -DENABLE_ASAN=ON -DBUILD_EXAMPLES=OFF
cmake --build build-asan && cd build-asan && ctest

cmake -B build-ubsan -DENABLE_UBSAN=ON -DBUILD_EXAMPLES=OFF
cmake --build build-ubsan && cd build-ubsan && ctest
```

## Test Results

41 test executables (36 unit + 2 integration + 3 benchmark), 100% pass rate
(regular + ASan + UBSan clean):

| Test | Description |
|------|-------------|
| test_admin | Admin HTTP `/stats`, `/health`, `/ready` |
| test_broker | Broker domain/queue/pubsub management |
| test_cgroup | Consumer group round-robin dispatch |
| test_cluster | Raft consensus + cluster membership |
| test_peer | Cluster peer listen, election, log replicate |
| test_collections_lockfree | SPSC ringbuf + MPMC Vyukov queue |
| test_config | INI-style configuration parser |
| test_conn | TCP connection with protocol framing |
| test_core_primitives | Compiler, atomics, time, mutex, spinlock, rwlock |
| test_coroutine | Assembly context switch + resume/yield |
| test_dispatch | Dispatcher session-aware message routing |
| test_dlq | Dead letter queue for failed messages |
| test_error_log | Error handling + logging |
| test_event_timer | Event loop + min-heap timer |
| test_flowcontrol | Credit-based flow control / backpressure |
| test_map | Hash map replacement, removal, and tombstone compaction |
| test_memory_buf | Tiered pool, slab, refcount buffer, arena |
| test_memory_pool | Memory pool allocation patterns |
| test_mpmc_stress | MPMC 2P/2C concurrent stress |
| test_net_tcp | Non-blocking socket + TCP server/client |
| test_proto | Binary wire protocol + CRC32C |
| test_hmac | SHA-256 / HMAC-SHA256 + AUTH MAC |
| test_wire | Typed payload encode/decode + name rules |
| test_queue | FIFO/priority/broadcast queues + router |
| test_ratelimit | Per-connection token bucket rate limiter |
| test_server | Protocol server: bind, pub/sub, JOIN groups, rate limit, PUSH credits |
| test_session | Session lifecycle + activity tracking |
| test_shutdown | Graceful shutdown (signal → evloop stop) |
| test_signal | SIGPIPE/SIGINT/SIGTERM handling |
| test_storage | In-memory/file storage + mmap |
| test_wal | Durable WAL put/del, replay, exclusive, broker datadir |
| test_sync_primitives | Thread synchronization primitives |
| test_thread | Portable spawn/join/yield |
| test_test_framework | Self-built test framework |
| test_threadpool | Work-stealing thread pool |
| test_ttl | Message TTL with heap+map expiry |
| test_integration | Cross-module: broker+router+cluster+proto+storage+coro+tpool |
| test_conn_integration | socketpair TCP + evloop + protocol frames |
| bench_queue | Queue throughput benchmarks |
| bench_collections | Map/vec/MPMC performance benchmarks |
| bench_memory | Pool/arena/buf allocation benchmarks |

## Examples

```bash
./build/examples/demo_broker      # Broker publish/subscribe demo
./build/examples/demo_cluster     # Raft consensus + cluster demo
./build/examples/demo_full        # Comprehensive demo (all subsystems)
./build/examples/transit-server   # Integrated server (config+admin+broker+evloop)
                                  # -d <dir> or [storage] datadir= for durable queues
                                  # -C <port> or [cluster] port= for peer listen
                                  # -n <id> / [cluster] id= and
                                  # peers=id@host:peer[/client],...
                                  # -k <psk> or [auth] psk= for client AUTH
```

## Benchmarks (Linux, GCC)

| Benchmark | Throughput |
|-----------|-----------|
| FIFO queue (1M msgs) | 250M ops/sec |
| Memory pool (400K allocs) | 200M ops/sec |
| Vec push (1M) | 333M ops/sec |
| Map insert/lookup (100K) | 6.7M ops/sec |
| MPMC queue (1P/1C) | 5M ops/sec |

### Performance Notes

- `t_map` tracks tombstones and compacts when removals dominate, keeping
  remove-heavy paths such as TTL expiry from degrading into long probe chains.
- The CI sanitizer matrix maps directly to `ENABLE_ASAN` and `ENABLE_UBSAN`, so
  sanitizer jobs exercise instrumented builds instead of plain Release builds.
- Wire decode aliases the frame buffer (no payload copy). The protocol server
  applies O(1) token-bucket checks before touching the broker. Per-session
  PUSH credits (default 64) stop a slow consumer from filling the 64 MiB
  send buffer; FIFO/priority `CONFIRM` acks inflight and `REJECT` requeues.
- `transit-server` and `t_server` default to loopback. Admin HTTP stays on
  `127.0.0.1`. Oversize names, trailing junk, and unknown frame types close the
  socket. Durable `OPEN` without `-d`/`[storage] datadir` fails closed (`T_ERR_IO`).
  Cluster peer listen is opt-in (`-C` / `[cluster] port=`).
  Client AUTH (`-k` / `[auth] psk=`) is required off loopback.

## Cross-Platform

- **Linux**: epoll backend (primary, fully tested)
- **macOS**: kqueue backend (implemented, conditional via `T_HAVE_KQUEUE`)
- **AArch64**: coroutine switch matches the x86_64 assembly path
- **Windows**: mmap, Winsock2, WSAPoll readiness, `t_conn`/`t_tcp`, durable
  WAL, Raft log, file-backed storage dumps, shutdown signals, the thread
  pool, admin HTTP, and leftover helpers (`t_config` / `t_log` /
  `t_ratelimit` / `t_flowcontrol`). Coroutine create returns NULL (SysV
  assembly is the wrong ABI).

CI runs Linux, macOS, and Windows (default Visual Studio generator, x64)
via GitHub Actions, plus
Linux ASan/UBSan jobs. See [docs/Windows_CI.md](docs/Windows_CI.md).

## Quick Start

```c
#include "transit.h"

int main(void) {
    t_evloop *loop = t_evloop_create();
    t_broker *broker = t_broker_create("my-broker");
    t_broker_start(broker);

    t_server_config cfg;
    t_server_config_init(&cfg);   /* 127.0.0.1:4222 */
    cfg.port = 0;                 /* ephemeral for tests */
    t_server *srv = t_server_create(loop, broker, &cfg);
    t_server_start(srv);

    t_client *c = t_client_create("worker");
    t_client_dial(c, loop, "127.0.0.1", t_server_port(srv));
    t_client_open_follow(c, "my.queue", T_CLIENT_OPEN_PRODUCER, 500);
    t_client_post(c, "my.queue", (const uint8_t *)"hello", 5, 0);

    t_client_destroy(c);
    t_server_destroy(srv);
    t_broker_destroy(broker);
    t_evloop_destroy(loop);
    return 0;
}
```

In-process (no TCP) still works via `t_broker_publish` / `t_broker_subscribe`
or the stub `t_client_connect`.

See [docs/Wire_Protocol.md](docs/Wire_Protocol.md),
[docs/Windows_CI.md](docs/Windows_CI.md), and
[docs/ROADMAP.md](docs/ROADMAP.md).

## Project Stats

- 53 source files (`.c`/`.S`), 54 headers
- ~6,400 LOC (source), ~1,900 LOC (headers)
- ~3,500 LOC (tests)
- Zero external dependencies
- Single `#include "transit.h"` for all APIs

## License

GPLv3 — see [LICENSE](LICENSE).
