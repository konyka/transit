#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "t_cluster.h"
#include "t_raft.h"
#include "t_node.h"

int main(void) {
    t_cluster *cluster = t_cluster_create(1);
    if (!cluster) {
        fprintf(stderr, "Failed to create cluster\n");
        return 1;
    }

    t_cluster_add_node(cluster, 2, "127.0.0.2", 11111);
    t_cluster_add_node(cluster, 3, "127.0.0.3", 22222);
    printf("Cluster nodes: %zu\n", t_cluster_node_count(cluster));

    t_cluster_set_leader(cluster, 2);
    t_node *leader = t_cluster_get_leader(cluster);
    printf("Leader: node %lu at %s:%u\n",
           (unsigned long)t_node_id(leader),
           t_node_host(leader),
           (unsigned)t_node_port(leader));

    t_raft_config cfg = {1, 150, 50};
    t_raft *raft = t_raft_create(&cfg);
    if (!raft) {
        fprintf(stderr, "Failed to create raft\n");
        t_cluster_destroy(cluster);
        return 1;
    }

    printf("Raft: term=%lu, state=%s\n",
           (unsigned long)t_raft_current_term(raft),
           t_raft_state(raft) == T_NODE_FOLLOWER ? "follower" : "other");

    t_raft_become_candidate(raft);
    t_raft_become_leader(raft);
    printf("Raft: promoted to leader, term=%lu\n",
           (unsigned long)t_raft_current_term(raft));

    uint8_t data[] = "cluster-log-entry";
    t_raft_append_entry(raft, 2, data, sizeof(data));
    t_raft_advance_commit(raft, 1);
    t_raft_apply_entries(raft);
    printf("Raft: log=%zu entries, applied=%zu\n",
           t_raft_log_count(raft),
           t_raft_applied_count(raft));

    printf("Cluster alive: %zu\n", t_cluster_alive_count(cluster));

    t_raft_destroy(raft);
    t_cluster_destroy(cluster);
    return 0;
}
