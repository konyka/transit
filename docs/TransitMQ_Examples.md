Transit MQ - Examples & Documentation
====================================

This folder provides minimal C examples demonstrating how to use the Transit MQ APIs exposed by the project.

- examples/demo_broker.c: A full broker demo that creates a broker, starts it, creates a queue in the default domain, publishes messages, subscribes to the queue with a callback, prints basic statistics, and then stops and destroys the broker.
- examples/demo_cluster.c: A cluster demo showing how to construct a cluster, add nodes, designate a leader, create a Raft instance, perform a simple election flow, append a log entry, commit/apply, and report basic cluster state.

Build and run
- Build with examples enabled:
  cmake -B build -DBUILD_EXAMPLES=ON --fresh
  cmake --build build
- Run the demos:
  ./build/examples/demo_broker
  ./build/examples/demo_cluster

Assumptions and notes
- Prefer `#include "transit.h"` for the full public API (includes broker, cluster,
  queue, TTL, cgroup, flowcontrol, DLQ, ratelimit, shutdown, etc.).
- Also available: `examples/demo_full.c` and `examples/transit-server.c`.
  `transit-server` listens for client frames on the configured host/port
  (default `127.0.0.1:4222`) and for admin HTTP on `127.0.0.1:8222`
  (`/health`, `/ready`, `/stats`). `/ready` is 200 only on a standalone
  node or Raft leader; followers return 503.
- The examples intentionally use a straightforward style with basic error checking,
  minimal resource management, and printf-based status output.
- No external dependencies are introduced; the examples compile against the project source tree.
