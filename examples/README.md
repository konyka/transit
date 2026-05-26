# Transit MQ - Examples

This directory contains two small C examples that illustrate how to use the Transit MQ APIs:
- demo_broker.c: A complete broker lifecycle demo with queue creation, publishing, subscribing, and statistics.
- demo_cluster.c: A basic cluster/raft demo showing node addition, leadership, and log entry handling.

Build and run
- Ensure you build with examples enabled: cmake -B build -DBUILD_EXAMPLES=ON --fresh; cmake --build build
- Run the executables:
  ./build/examples/demo_broker
  ./build/examples/demo_cluster

Notes
- These examples rely on the project headers and do not pull in external libraries.
- They are intended for quick manual testing and demonstrations, not for production use.
