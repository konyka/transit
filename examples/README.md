# Transit MQ - Examples

This directory contains C examples that illustrate how to use the Transit MQ APIs:

- `demo_broker.c` — Broker lifecycle: queue creation, publish, subscribe, statistics
- `demo_cluster.c` — Cluster/Raft: node addition, leadership, log entries
- `demo_full.c` — Cross-subsystem demo (broker, dispatch, routing, cluster, storage, protocol)
- `transit-server.c` — Integrated server (config, admin HTTP, protocol
  listener, broker, evloop, signal, shutdown). Default client bind is
  `127.0.0.1:4222`; pass `-h 0.0.0.0` only when remote clients are intended.
  Durable queues need `-d <dir>` or `[storage] datadir=` in the INI config.

## Build and run

```bash
cmake -B build -DBUILD_EXAMPLES=ON
cmake --build build

./build/examples/demo_broker
./build/examples/demo_cluster
./build/examples/demo_full
./build/examples/transit-server   # optional: pass a config path
```

## Notes

- Examples link only against the in-tree `transit` static library (zero external deps).
- Intended for manual testing and demos, not production deployment.
- Prefer `#include "transit.h"` for the full public API surface.
