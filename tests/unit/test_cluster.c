#include "t_test.h"
#include "t_cluster.h"
#include "t_node.h"
#include "t_raft.h"
#include "t_wire.h"
#include "t_broker.h"
#include <string.h>
#include <unistd.h>

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
    T_ASSERT_EQ((int)t_raft_id(r), 1);
    T_ASSERT_EQ((int)t_raft_election_timeout_ms(r), 150);
    T_ASSERT_EQ((int)t_raft_heartbeat_interval_ms(r), 50);
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
    T_ASSERT_EQ((int)t_raft_voted_for(r), 0);
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

static int g_apply_calls;
static size_t g_apply_bytes;

static void on_raft_apply(const t_raft_entry *entry, void *ud) {
    (void)ud;
    g_apply_calls++;
    if (entry) g_apply_bytes += entry->data_len;
}

T_TEST(raft_apply_invokes_callback) {
    t_raft_config cfg = {1, 150, 50};
    t_raft *r = t_raft_create(&cfg);
    g_apply_calls = 0;
    g_apply_bytes = 0;
    t_raft_set_apply_cb(r, on_raft_apply, NULL);
    T_ASSERT_EQ(t_raft_become_candidate(r), 0);
    T_ASSERT_EQ(t_raft_become_leader(r), 0);
    uint8_t data[] = {9, 8, 7, 6};
    T_ASSERT_EQ(t_raft_append_entry(r, 1, data, sizeof(data)), 0);
    T_ASSERT_EQ(t_raft_advance_commit(r, 1), 0);
    T_ASSERT_EQ(t_raft_apply_entries(r), 0);
    T_ASSERT_EQ(g_apply_calls, 1);
    T_ASSERT_EQ((int)g_apply_bytes, 4);
    t_raft_destroy(r);
}

T_TEST(raft_voting) {
    t_raft_config cfg = {1, 150, 50};
    t_raft *r = t_raft_create(&cfg);
    T_ASSERT_EQ(t_raft_request_vote(r, 2, 1), 1);
    T_ASSERT_EQ((int)t_raft_current_term(r), 1);
    T_ASSERT_EQ((int)t_raft_voted_for(r), 2);
    T_ASSERT_EQ((int)t_raft_state(r), T_NODE_FOLLOWER);
    T_ASSERT_EQ(t_raft_request_vote(r, 3, 1), 0); /* already voted this term */
    T_ASSERT_EQ((int)t_raft_voted_for(r), 2);
    T_ASSERT_EQ(t_raft_grant_vote(r, 3), -1); /* cannot override existing vote */
    T_ASSERT_EQ((int)t_raft_voted_for(r), 2);
    T_ASSERT_EQ(t_raft_grant_vote(r, 2), 0); /* same candidate ok */
    t_raft_destroy(r);
}

T_TEST(raft_rpc_vote_and_append) {
    t_raft_config c1 = {1, 150, 50};
    t_raft_config c2 = {2, 150, 50};
    t_raft *leader = t_raft_create(&c1);
    t_raft *fol = t_raft_create(&c2);
    T_ASSERT_EQ(t_raft_become_candidate(leader), 0);
    T_ASSERT_EQ(t_raft_become_leader(leader), 0);
    uint8_t data[] = {1, 2};
    T_ASSERT_EQ(t_raft_append_entry(leader, 1, data, sizeof(data)), 0);

    t_wire_vote_req vr;
    memset(&vr, 0, sizeof(vr));
    vr.term = t_raft_current_term(leader);
    vr.candidate_id = 1;
    vr.last_log_index = t_raft_last_log_index(leader);
    vr.last_log_term = t_raft_last_log_term(leader);
    uint8_t buf[256], resp[256];
    int n = t_wire_encode_vote_req(buf, sizeof(buf), &vr);
    T_ASSERT(n > 0);
    int m = t_raft_rpc(fol, buf, (size_t)n, resp, sizeof(resp));
    T_ASSERT(m > 0);
    t_wire_vote_resp vo;
    T_ASSERT_EQ(t_wire_decode_vote_resp(resp, (size_t)m, &vo), 0);
    T_ASSERT_EQ((int)vo.granted, 1);

    const t_raft_entry *e = t_raft_get_entry(leader, 1);
    t_wire_cluster_entry ent;
    memset(&ent, 0, sizeof(ent));
    ent.index = e->index;
    ent.term = e->term;
    ent.type = e->type;
    ent.data = e->data;
    ent.data_len = (uint32_t)e->data_len;
    t_wire_append_req ar;
    memset(&ar, 0, sizeof(ar));
    ar.term = t_raft_current_term(leader);
    ar.leader_id = 1;
    ar.leader_commit = 1;
    n = t_wire_encode_append_req(buf, sizeof(buf), &ar, &ent, 1);
    T_ASSERT(n > 0);
    m = t_raft_rpc(fol, buf, (size_t)n, resp, sizeof(resp));
    T_ASSERT(m > 0);
    t_wire_append_resp ao;
    T_ASSERT_EQ(t_wire_decode_append_resp(resp, (size_t)m, &ao), 0);
    T_ASSERT_EQ((int)ao.success, 1);
    T_ASSERT_EQ((int)t_raft_log_count(fol), 1);
    T_ASSERT_EQ((int)t_raft_commit_index(fol), 1);
    t_raft_destroy(leader);
    t_raft_destroy(fol);
}

T_TEST(raft_rpc_stale_candidate_denied) {
    t_raft_config cfg = {2, 150, 50};
    t_raft *fol = t_raft_create(&cfg);
    T_ASSERT_EQ(t_raft_become_candidate(fol), 0);
    T_ASSERT_EQ(t_raft_become_leader(fol), 0);
    uint8_t data[] = {9};
    T_ASSERT_EQ(t_raft_append_entry(fol, 1, data, 1), 0);
    T_ASSERT_EQ(t_raft_become_follower(fol, t_raft_current_term(fol)), 0);

    t_wire_vote_req vr;
    memset(&vr, 0, sizeof(vr));
    vr.term = t_raft_current_term(fol) + 1;
    vr.candidate_id = 9;
    vr.last_log_index = 0;
    vr.last_log_term = 0;
    uint8_t buf[128], resp[128];
    int n = t_wire_encode_vote_req(buf, sizeof(buf), &vr);
    int m = t_raft_rpc(fol, buf, (size_t)n, resp, sizeof(resp));
    t_wire_vote_resp vo;
    T_ASSERT_EQ(t_wire_decode_vote_resp(resp, (size_t)m, &vo), 0);
    T_ASSERT_EQ((int)vo.granted, 0);
    t_raft_destroy(fol);
}

T_TEST(raft_log_survives_reopen) {
    const char *path = "test_transit_raft.log";
    unlink(path);
    t_raft_config cfg = {1, 150, 50};
    t_raft *r = t_raft_create(&cfg);
    T_ASSERT_EQ(t_raft_open_log(r, path, 1), 0);
    T_ASSERT_EQ(t_raft_become_candidate(r), 0);
    T_ASSERT_EQ(t_raft_become_leader(r), 0);
    uint8_t data[] = {'x'};
    T_ASSERT_EQ(t_raft_append_entry(r, 3, data, 1), 0);
    t_raft_destroy(r);

    r = t_raft_create(&cfg);
    T_ASSERT_EQ(t_raft_open_log(r, path, 1), 0);
    T_ASSERT_EQ((int)t_raft_log_count(r), 1);
    T_ASSERT_EQ((int)t_raft_current_term(r), 1);
    const t_raft_entry *e = t_raft_get_entry(r, 1);
    T_ASSERT_NOT_NULL(e);
    T_ASSERT_MEM_EQ(e->data, data, 1);
    t_raft_destroy(r);
    unlink(path);
}

T_TEST(broker_leader_only_publish) {
    t_broker *b = t_broker_create("n0");
    t_cluster *c = t_cluster_create(1);
    T_ASSERT_EQ(t_cluster_add_node(c, 1, "127.0.0.1", 1), 0);
    T_ASSERT_EQ(t_cluster_add_node(c, 2, "127.0.0.1", 2), 0);
    T_ASSERT_EQ(t_cluster_set_leader(c, 2), 0);
    T_ASSERT_EQ(t_broker_set_cluster(b, c), 0);
    T_ASSERT_EQ(t_broker_start(b), 0);
    T_ASSERT_EQ(t_broker_create_queue(b, "default", "q", 0, 0), 0);
    T_ASSERT_EQ(t_broker_publish(b, "q", (const uint8_t *)"a", 1, 0), -1);
    T_ASSERT_EQ(t_cluster_set_leader(c, 1), 0);
    T_ASSERT_EQ(t_broker_publish(b, "q", (const uint8_t *)"a", 1, 0), 0);
    t_broker_destroy(b);
    t_cluster_destroy(c);
}

int main(void) {
    return t_run_all_tests();
}
