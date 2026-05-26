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
- The examples rely on the public API listed in the headers:
  - Broker API: t_broker_create, t_broker_destroy, t_broker_start, t_broker_stop, t_broker_create_queue, t_broker_publish, t_broker_subscribe, t_broker_total_queues, t_broker_total_messages
  - Cluster API: t_cluster_create, t_cluster_destroy, t_cluster_add_node, t_cluster_remove_node, t_cluster_set_leader, t_cluster_get_leader, t_cluster_node_count, t_cluster_alive_count
  - Raft API: t_raft_create, t_raft_destroy, t_raft_become_candidate, t_raft_become_leader, t_raft_become_follower, t_raft_append_entry, t_raft_advance_commit, t_raft_apply_entries
- The exact signatures are provided by the project headers. The examples intentionally use a straightforward style with basic error checking, minimal resource management, and printf-based status output.
- No external dependencies are introduced; the examples compile against the project source tree.
