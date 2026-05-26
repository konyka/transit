# Transit

High-performance distributed message queue in pure C (C99-C23 compatible).

Reimplements Bloomberg's [BlazingMQ](https://github.com/bloomberg/blazingmq) feature set with zero external dependencies.

## Architecture

```
Phase 0:  Build System & CI
Phase 1:  Core Primitives (compiler, atomics, time, mutex, spinlock, rwlock, error, log)
Phase 2:  Memory Management (tiered pool, slab, refcount buffer, arena)
Phase 3:  Collections (vec, map, list, SPSC ringbuf, Vyukov MPMC, priority queue)
Phase 4:  Event Loop (epoll + wakeup pipe) & Timer (min-heap)
Phase 5:  Thread Pool (work-stealing, per-worker MPMC queues)
Phase 6:  Coroutines (pure x86-64 assembly context switch)
Phase 7:  Networking (non-blocking socket, TCP server with evloop integration)
Phase 8:  Wire Protocol (binary header encode/decode, CRC32C Castagnoli)
Phase 9:  Storage (in-memory + file-backed hashmap, POSIX mmap)
Phase 10: Queue Engine (FIFO/priority/broadcast, consumer callbacks, ack/nack)
Phase 11: Session & Client Library (lifecycle, pub/sub, queue registry)
Phase 12: Broker (domain management, message routing, publish/subscribe)
Phase 13: Clustering (simplified Raft consensus, node membership, leader election)
```

## Build

```bash
cmake -B build -DBUILD_EXAMPLES=ON
cmake --build build
ctest --test-dir build --output-on-failure
```

Requires GCC or Clang with C99+ support. Builds with `-Wall -Wextra -Werror`.

## Test Results

17 test suites, 100% pass rate:

| Test | Description |
|------|-------------|
| test_broker | Broker domain/queue/pubsub management |
| test_cluster | Raft consensus + cluster membership |
| test_collections_lockfree | SPSC ringbuf + MPMC Vyukov queue |
| test_core_primitives | Compiler, atomics, time, mutex, spinlock, rwlock |
| test_coroutine | Assembly context switch + resume/yield |
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

## Examples

```bash
./build/examples/demo_broker    # Broker publish/subscribe demo
./build/examples/demo_cluster   # Raft consensus + cluster demo
```

## Cross-Platform

- Linux: epoll backend (primary)
- macOS: kqueue (planned)
- Windows: IOCP (planned)

CI runs on all three platforms via GitHub Actions.

## License

MIT
