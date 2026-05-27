# Transit

High-performance distributed message queue in pure C (C99-C23 compatible).

Reimplements Bloomberg's [BlazingMQ](https://github.com/bloomberg/blazingmq) feature set with zero external dependencies.

## Architecture

```
src/
├── core/        compiler, atomics, time, mutex, spinlock, rwlock, error, log, version
├── memory/      tiered pool, slab allocator, refcount buffer, arena
├── collections/ vec, map, list, SPSC ringbuf, Vyukov MPMC, priority queue
├── event/       epoll event loop (Linux) + kqueue backend (macOS), min-heap timer
├── threadpool/  work-stealing thread pool with per-worker MPMC queues
├── coroutine/   pure x86-64 assembly context switch (6 callee-saved regs)
├── net/         non-blocking socket, TCP server, protocol-framed connection
├── protocol/    binary wire protocol (16-byte header), CRC32C Castagnoli
├── storage/     in-memory + file-backed hashmap, POSIX mmap wrapper
├── queue/       FIFO/priority/broadcast queues, topic router (wildcard * #)
├── session/     session lifecycle, activity tracking, timeout detection
├── client/      queue registry, publish/subscribe, connection management
├── broker/      domain management, publish/subscribe, dispatcher
└── cluster/     simplified Raft consensus, node membership, leader election
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

31 test suites, 100% pass rate (regular + ASan + UBSan clean):

| Test | Description |
|------|-------------|
| test_broker | Broker domain/queue/pubsub management |
| test_cluster | Raft consensus + cluster membership |
| test_collections_lockfree | SPSC ringbuf + MPMC Vyukov queue |
| test_conn | TCP connection with protocol framing |
| test_core_primitives | Compiler, atomics, time, mutex, spinlock, rwlock |
| test_coroutine | Assembly context switch + resume/yield |
| test_dispatch | Dispatcher session-aware message routing |
| test_error_log | Error handling + logging |
| test_event_timer | Epoll event loop + min-heap timer |
| test_memory_buf | Tiered pool, slab, refcount buffer, arena |
| test_memory_pool | Memory pool allocation patterns |
| test_net_tcp | Non-blocking socket + TCP server/client |
| test_proto | Binary wire protocol + CRC32C |
| test_queue | FIFO/priority/broadcast queues + router |
| test_session | Session lifecycle + activity tracking |
| test_storage | In-memory/file storage + mmap |
| test_sync_primitives | Thread synchronization primitives |
| test_test_framework | Self-built test framework |
| test_threadpool | Work-stealing thread pool |
| test_integration | Cross-module: broker+router+cluster+proto+storage+coro+tpool |
| bench_queue | Queue throughput benchmarks |
| bench_collections | Map/vec/MPMC performance benchmarks |
| bench_memory | Pool/arena/buf allocation benchmarks |

## Examples

```bash
./build/examples/demo_broker    # Broker publish/subscribe demo
./build/examples/demo_cluster   # Raft consensus + cluster demo
./build/examples/demo_full      # Comprehensive demo (all subsystems)
```

## Benchmarks (Linux, GCC)

| Benchmark | Throughput |
|-----------|-----------|
| FIFO queue (1M msgs) | 250M ops/sec |
| Memory pool (400K allocs) | 200M ops/sec |
| Vec push (1M) | 333M ops/sec |
| Map insert/lookup (100K) | 6.7M ops/sec |
| MPMC queue (1P/1C) | 5M ops/sec |

## Cross-Platform

- **Linux**: epoll backend (primary, fully tested)
- **macOS**: kqueue backend (implemented, conditional via `T_HAVE_KQUEUE`)
- **Windows**: IOCP (planned)

CI runs on all three platforms via GitHub Actions.

## Quick Start

```c
#include "transit.h"

int main(void) {
    t_broker *broker = t_broker_create("my-broker");
    t_broker_start(broker);
    t_broker_create_queue(broker, "default", "my.queue", T_QUEUE_FIFO, 0);

    t_broker_subscribe(broker, "my.queue", my_callback, NULL);
    t_broker_publish(broker, "my.queue", (const uint8_t *)"hello", 5, 0);

    t_broker_stop(broker);
    t_broker_destroy(broker);
    return 0;
}
```

## Project Stats

- 46 source files, 49 headers
- ~6,100 LOC (source)
- ~3,200 LOC (tests)
- Zero external dependencies
- Single `#include "transit.h"` for all APIs

## License

MIT
