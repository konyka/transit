#include "t_test.h"
#include "t_cluster.h"
#include "t_node.h"
#include "t_raft.h"
#include <string.h>

T_TEST(node_create_destroy) {
    t_node *n = t_node_create(42, "127.0.0.1", 8080);
    T_ASSERT_NOT_NULL(n);
    T_ASSERT_EQ((int)t_node_id(n), 42);
    T_ASSERT(strcmp(t_node_host(n), "127.0.0.1") == 0);
    T_ASSERT_EQ((int)t_node_port(n), 8080);
    T_ASSERT_EQ((int)t_node_get_role(n), T_NODE_FOLLOWER);
    T_ASSERT(t_node_is_alive(n));
    T_ASSERT_EQ((int)t_node_term(n), 0);
    t_node_destroy(n);
}

T_TEST(node_role_lifecycle) {
    t_node *n = t_node_create(1, "localhost", 9090);
    T_ASSERT(!t_node_is_leader(n));
    t_node_set_role(n, T_NODE_LEADER);
    T_ASSERT(t_node_is_leader(n));
    T_ASSERT_EQ((int)t_node_get_role(n), T_NODE_LEADER);
    t_node_set_role(n, T_NODE_CANDIDATE);
    T_ASSERT_EQ((int)t_node_get_role(n), T_NODE_CANDIDATE);
    t_node_set_alive(n, 0);
    T_ASSERT(!t_node_is_alive(n));
    t_node_set_term(n, 5);
    T_ASSERT_EQ((int)t_node_term(n), 5);
    t_node_destroy(n);
}

T_TEST(cluster_add_remove_nodes) {
    t_cluster *c = t_cluster_create(1);
    T_ASSERT_NOT_NULL(c);
    T_ASSERT_EQ((int)t_cluster_self_id(c), 1);
    T_ASSERT_EQ((int)t_cluster_node_count(c), 0);
    T_ASSERT_EQ(t_cluster_add_node(c, 2, "127.0.0.2", 1111), 0);
    T_ASSERT_EQ(t_cluster_add_node(c, 3, "127.0.0.3", 2222), 0);
    T_ASSERT_EQ((int)t_cluster_node_count(c), 2);
    t_node *n2 = t_cluster_get_node(c, 2);
    T_ASSERT_NOT_NULL(n2);
    T_ASSERT_EQ((int)t_node_id(n2), 2);
    T_ASSERT_EQ(t_cluster_remove_node(c, 2), 0);
    T_ASSERT_EQ((int)t_cluster_node_count(c), 1);
    T_ASSERT_NULL(t_cluster_get_node(c, 2));
    t_cluster_destroy(c);
}

T_TEST(cluster_leader) {
    t_cluster *c = t_cluster_create(1);
    T_ASSERT_NULL(t_cluster_get_leader(c));
    T_ASSERT_EQ(t_cluster_add_node(c, 2, "127.0.0.2", 1111), 0);
    T_ASSERT_EQ(t_cluster_set_leader(c, 2), 0);
    t_node *leader = t_cluster_get_leader(c);
    T_ASSERT_NOT_NULL(leader);
    T_ASSERT_EQ((int)t_node_id(leader), 2);
    T_ASSERT(!t_cluster_is_leader(c));
    T_ASSERT_EQ(t_cluster_set_leader(c, 1), -1);
    t_cluster_destroy(c);
}

T_TEST(cluster_alive_count) {
    t_cluster *c = t_cluster_create(1);
    T_ASSERT_EQ(t_cluster_add_node(c, 2, "127.0.0.2", 1111), 0);
    T_ASSERT_EQ(t_cluster_add_node(c, 3, "127.0.0.3", 2222), 0);
    T_ASSERT_EQ((int)t_cluster_alive_count(c), 2);
    t_node *n2 = t_cluster_get_node(c, 2);
    t_node_set_alive(n2, 0);
    T_ASSERT_EQ((int)t_cluster_alive_count(c), 1);
    t_cluster_destroy(c);
}

T_TEST(raft_create_destroy) {
    t_raft_config cfg = {1, 150, 50};
    t_raft *r = t_raft_create(&cfg);
    T_ASSERT_NOT_NULL(r);
    T_ASSERT_EQ((int)t_raft_current_term(r), 0);
    T_ASSERT_EQ((int)t_raft_state(r), T_NODE_FOLLOWER);
    T_ASSERT_EQ((int)t_raft_commit_index(r), 0);
    T_ASSERT_EQ((int)t_raft_last_applied(r), 0);
    T_ASSERT_EQ((int)t_raft_voted_for(r), 0);
    t_raft_destroy(r);
}

T_TEST(raft_election) {
    t_raft_config cfg = {1, 150, 50};
    t_raft *r = t_raft_create(&cfg);
    T_ASSERT_EQ(t_raft_become_candidate(r), 0);
    T_ASSERT_EQ((int)t_raft_current_term(r), 1);
    T_ASSERT_EQ((int)t_raft_state(r), T_NODE_CANDIDATE);
    T_ASSERT_EQ((int)t_raft_voted_for(r), 1);
    T_ASSERT_EQ(t_raft_become_leader(r), 0);
    T_ASSERT_EQ((int)t_raft_state(r), T_NODE_LEADER);
    t_raft_destroy(r);
}

T_TEST(raft_follower_transition) {
    t_raft_config cfg = {1, 150, 50};
    t_raft *r = t_raft_create(&cfg);
    T_ASSERT_EQ(t_raft_become_candidate(r), 0);
    T_ASSERT_EQ(t_raft_become_leader(r), 0);
    T_ASSERT_EQ(t_raft_become_follower(r, 5), 0);
    T_ASSERT_EQ((int)t_raft_current_term(r), 5);
    T_ASSERT_EQ((int)t_raft_state(r), T_NODE_FOLLOWER);
    t_raft_destroy(r);
}

T_TEST(raft_log_append_commit) {
    t_raft_config cfg = {1, 150, 50};
    t_raft *r = t_raft_create(&cfg);
    T_ASSERT_EQ(t_raft_become_candidate(r), 0);
    T_ASSERT_EQ(t_raft_become_leader(r), 0);
    uint8_t data[] = {1, 2, 3};
    T_ASSERT_EQ(t_raft_append_entry(r, 2, data, sizeof(data)), 0);
    T_ASSERT_EQ((int)t_raft_log_count(r), 1);
    const t_raft_entry *e = t_raft_get_entry(r, 1);
    T_ASSERT_NOT_NULL(e);
    T_ASSERT_EQ((int)e->index, 1);
    T_ASSERT_EQ((int)e->term, 1);
    T_ASSERT_EQ((int)e->type, 2);
    T_ASSERT_EQ((int)e->data_len, 3);
    T_ASSERT_EQ(t_raft_advance_commit(r, 1), 0);
    T_ASSERT_EQ(t_raft_apply_entries(r), 0);
    T_ASSERT_EQ((int)t_raft_applied_count(r), 1);
    t_raft_destroy(r);
}

T_TEST(raft_voting) {
    t_raft_config cfg = {1, 150, 50};
    t_raft *r = t_raft_create(&cfg);
    T_ASSERT_EQ(t_raft_request_vote(r, 2, 1), 1);
    T_ASSERT_EQ((int)t_raft_current_term(r), 1);
    T_ASSERT_EQ((int)t_raft_voted_for(r), 2);
    T_ASSERT_EQ(t_raft_grant_vote(r, 3), 0);
    T_ASSERT_EQ((int)t_raft_voted_for(r), 3);
    t_raft_destroy(r);
}

int main(void) {
    return t_run_all_tests();
}
