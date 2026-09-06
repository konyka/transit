#include "t_test.h"
#include "t_cluster.h"
#include "t_node.h"
#include "t_raft.h"
#include "t_raft_cmd.h"
#include "t_wire.h"
#include "t_broker.h"
#include "t_domain.h"
#include "t_queue.h"
#include "t_file.h"
#include <string.h>
#include <stdlib.h>

T_TEST(node_create_destroy) {
    t_node *n = t_node_create(42, "127.0.0.1", 8080);
    T_ASSERT_NOT_NULL(n);
    T_ASSERT_EQ((int)t_node_id(n), 42);
    T_ASSERT(strcmp(t_node_host(n), "127.0.0.1") == 0);
    T_ASSERT_EQ((int)t_node_port(n), 8080);
    T_ASSERT_EQ((int)t_node_client_port(n), 0);
    T_ASSERT_EQ(t_node_set_client_port(n, 4222), 0);
    T_ASSERT_EQ((int)t_node_client_port(n), 4222);
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
    t_file_unlink(path);
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
    t_file_unlink(path);
}

T_TEST(cluster_parse_peers) {
    t_cluster_peer_spec p[4];
    size_t n = 99;
    T_ASSERT_EQ(t_cluster_parse_peers(NULL, p, 4, &n), 0);
    T_ASSERT_EQ((int)n, 0);
    T_ASSERT_EQ(t_cluster_parse_peers("", p, 4, &n), 0);
    T_ASSERT_EQ((int)n, 0);
    T_ASSERT_EQ(t_cluster_parse_peers("1@127.0.0.1:4223,2@127.0.0.1:4224", p, 4, &n), 0);
    T_ASSERT_EQ((int)n, 2);
    T_ASSERT_EQ((int)p[0].id, 1);
    T_ASSERT_EQ((int)p[0].port, 4223);
    T_ASSERT_EQ((int)p[0].client_port, 0);
    T_ASSERT_STR_EQ(p[0].host, "127.0.0.1");
    T_ASSERT_EQ((int)p[1].id, 2);
    T_ASSERT_EQ(t_cluster_parse_peers("1@127.0.0.1:4223/4222,2@127.0.0.1:4224/4225", p, 4, &n), 0);
    T_ASSERT_EQ((int)n, 2);
    T_ASSERT_EQ((int)p[0].port, 4223);
    T_ASSERT_EQ((int)p[0].client_port, 4222);
    T_ASSERT_EQ((int)p[1].client_port, 4225);
    T_ASSERT_EQ(t_cluster_parse_peers("1@127.0.0.1:4223/", p, 4, &n), -1);
    T_ASSERT_EQ(t_cluster_parse_peers("1@127.0.0.1:4223/0", p, 4, &n), -1);
    T_ASSERT_EQ(t_cluster_parse_peers("1@127.0.0.1:4223,", p, 4, &n), -1);
    T_ASSERT_EQ(t_cluster_parse_peers("0@127.0.0.1:1", p, 4, &n), -1);
    T_ASSERT_EQ(t_cluster_parse_peers("1@127.0.0.1:0", p, 4, &n), -1);
    T_ASSERT_EQ(t_cluster_parse_peers("1@127.0.0.1:4223,1@10.0.0.1:9", p, 4, &n), -1);
    T_ASSERT_EQ(t_cluster_parse_peers("1@bad host:1", p, 4, &n), -1);
    T_ASSERT_EQ(t_cluster_parse_peers("1@127.0.0.1", p, 4, &n), -1);
    t_cluster *c = t_cluster_create(1);
    T_ASSERT_EQ(t_cluster_parse_peers("1@127.0.0.1:1,2@127.0.0.1:2", p, 4, &n), 0);
    T_ASSERT_EQ(t_cluster_add_peers(c, p, n), 0);
    T_ASSERT_EQ((int)t_cluster_node_count(c), 2);
    T_ASSERT_EQ(t_cluster_add_peers(c, p, n), -1);
    t_cluster_destroy(c);
    c = t_cluster_create(1);
    T_ASSERT_EQ(t_cluster_parse_peers("1@127.0.0.1:9/4222", p, 4, &n), 0);
    T_ASSERT_EQ(t_cluster_add_peers(c, p, n), 0);
    T_ASSERT_EQ((int)t_node_client_port(t_cluster_get_node(c, 1)), 4222);
    t_cluster_destroy(c);
}

T_TEST(raft_cmd_put_ack_roundtrip) {
    t_raft_cmd put;
    memset(&put, 0, sizeof(put));
    put.type = T_RAFT_CMD_PUT;
    put.qtype = 0;
    put.qflags = 1;
    put.priority = 7;
    put.msg_id = 42;
    put.name = "jobs";
    put.name_len = 4;
    put.data = (const uint8_t *)"hi";
    put.data_len = 2;
    uint8_t buf[64];
    int n = t_raft_cmd_encode_put(buf, sizeof(buf), &put);
    T_ASSERT(n > 0);
    t_raft_cmd out;
    T_ASSERT_EQ(t_raft_cmd_decode(buf, (size_t)n, &out), 0);
    T_ASSERT_EQ((int)out.type, T_RAFT_CMD_PUT);
    T_ASSERT_EQ((int)out.priority, 7);
    T_ASSERT_EQ((int)out.msg_id, 42);
    T_ASSERT_EQ((int)out.qflags, 1);
    T_ASSERT_EQ((int)out.name_len, 4);
    T_ASSERT(memcmp(out.name, "jobs", 4) == 0);
    T_ASSERT_EQ((int)out.data_len, 2);
    T_ASSERT_MEM_EQ(out.data, (const uint8_t *)"hi", 2);
    T_ASSERT_EQ(t_raft_cmd_decode(buf, (size_t)n - 1, &out), -1);

    t_raft_cmd ack;
    memset(&ack, 0, sizeof(ack));
    ack.type = T_RAFT_CMD_ACK;
    ack.msg_id = 42;
    ack.name = "jobs";
    ack.name_len = 4;
    n = t_raft_cmd_encode_ack(buf, sizeof(buf), &ack);
    T_ASSERT(n > 0);
    T_ASSERT_EQ(t_raft_cmd_decode(buf, (size_t)n, &out), 0);
    T_ASSERT_EQ((int)out.type, T_RAFT_CMD_ACK);
    T_ASSERT_EQ((int)out.msg_id, 42);
    T_ASSERT_EQ((int)out.name_len, 4);

    t_raft_cmd cr;
    memset(&cr, 0, sizeof(cr));
    cr.type = T_RAFT_CMD_CREATE;
    cr.qtype = 1;
    cr.qflags = 1;
    cr.name = "jobs";
    cr.name_len = 4;
    n = t_raft_cmd_encode_create(buf, sizeof(buf), &cr);
    T_ASSERT(n > 0);
    T_ASSERT_EQ(t_raft_cmd_decode(buf, (size_t)n, &out), 0);
    T_ASSERT_EQ((int)out.type, T_RAFT_CMD_CREATE);
    T_ASSERT_EQ((int)out.qtype, 1);
    T_ASSERT_EQ((int)out.qflags, 1);
    cr.type = T_RAFT_CMD_DELETE;
    n = t_raft_cmd_encode_delete(buf, sizeof(buf), &cr);
    T_ASSERT(n > 0);
    T_ASSERT_EQ(t_raft_cmd_decode(buf, (size_t)n, &out), 0);
    T_ASSERT_EQ((int)out.type, T_RAFT_CMD_DELETE);
    T_ASSERT_EQ(t_raft_cmd_encode_create(buf, sizeof(buf), &ack), -1);

    t_raft_cmd nk;
    memset(&nk, 0, sizeof(nk));
    nk.type = T_RAFT_CMD_NACK;
    nk.msg_id = 42;
    nk.name = "jobs";
    nk.name_len = 4;
    n = t_raft_cmd_encode_nack(buf, sizeof(buf), &nk);
    T_ASSERT(n > 0);
    T_ASSERT_EQ(t_raft_cmd_decode(buf, (size_t)n, &out), 0);
    T_ASSERT_EQ((int)out.type, T_RAFT_CMD_NACK);
    T_ASSERT_EQ((int)out.msg_id, 42);
    T_ASSERT_EQ(t_raft_cmd_encode_nack(buf, sizeof(buf), &ack), -1);
}

T_TEST(raft_majority_commit_single_node) {
    t_raft_config cfg = {1, 150, 50};
    t_raft *r = t_raft_create(&cfg);
    T_ASSERT_EQ(t_raft_become_candidate(r), 0);
    T_ASSERT_EQ(t_raft_become_leader(r), 0);
    uint8_t data[] = {1};
    T_ASSERT_EQ(t_raft_append_entry(r, 1, data, 1), 0);
    T_ASSERT_EQ(t_raft_majority_commit(r, NULL, 0, 1), 0);
    T_ASSERT_EQ((int)t_raft_commit_index(r), 1);
    T_ASSERT_EQ((int)t_raft_last_applied(r), 1);
    t_raft_destroy(r);
}

T_TEST(raft_majority_commit_needs_peer) {
    t_raft_config cfg = {1, 150, 50};
    t_raft *r = t_raft_create(&cfg);
    T_ASSERT_EQ(t_raft_become_candidate(r), 0);
    T_ASSERT_EQ(t_raft_become_leader(r), 0);
    uint8_t data[] = {1};
    T_ASSERT_EQ(t_raft_append_entry(r, 1, data, 1), 0);
    T_ASSERT_EQ(t_raft_majority_commit(r, NULL, 0, 2), 0);
    T_ASSERT_EQ((int)t_raft_commit_index(r), 0);
    uint64_t match = 1;
    T_ASSERT_EQ(t_raft_majority_commit(r, &match, 1, 2), 0);
    T_ASSERT_EQ((int)t_raft_commit_index(r), 1);
    T_ASSERT_EQ((int)t_raft_last_applied(r), 1);
    t_raft_destroy(r);
}

T_TEST(raft_majority_commit_old_term_not_committed) {
    t_raft_config cfg = {1, 150, 50};
    t_raft *r = t_raft_create(&cfg);
    T_ASSERT_EQ(t_raft_become_candidate(r), 0);
    T_ASSERT_EQ(t_raft_become_leader(r), 0);
    uint8_t data[] = {1};
    T_ASSERT_EQ(t_raft_append_entry(r, 1, data, 1), 0);
    T_ASSERT_EQ(t_raft_become_follower(r, t_raft_current_term(r)), 0);
    T_ASSERT_EQ(t_raft_become_candidate(r), 0);
    T_ASSERT_EQ(t_raft_become_leader(r), 0);
    T_ASSERT_EQ(t_raft_majority_commit(r, NULL, 0, 1), 0);
    T_ASSERT_EQ((int)t_raft_commit_index(r), 0);
    T_ASSERT_EQ(t_raft_append_entry(r, 1, data, 1), 0);
    T_ASSERT_EQ(t_raft_majority_commit(r, NULL, 0, 1), 0);
    T_ASSERT_EQ((int)t_raft_commit_index(r), 2);
    t_raft_destroy(r);
}

T_TEST(raft_commit_survives_reopen) {
    const char *path = "test_transit_raft_commit.log";
    t_file_unlink(path);
    t_raft_config cfg = {1, 150, 50};
    t_raft *r = t_raft_create(&cfg);
    T_ASSERT_EQ(t_raft_open_log(r, path, 1), 0);
    T_ASSERT_EQ(t_raft_become_candidate(r), 0);
    T_ASSERT_EQ(t_raft_become_leader(r), 0);
    uint8_t data[] = {'x'};
    T_ASSERT_EQ(t_raft_append_entry(r, 3, data, 1), 0);
    T_ASSERT_EQ(t_raft_majority_commit(r, NULL, 0, 1), 0);
    T_ASSERT_EQ((int)t_raft_commit_index(r), 1);
    t_raft_destroy(r);

    r = t_raft_create(&cfg);
    T_ASSERT_EQ(t_raft_open_log(r, path, 1), 0);
    T_ASSERT_EQ((int)t_raft_log_count(r), 1);
    T_ASSERT_EQ((int)t_raft_commit_index(r), 1);
    T_ASSERT_EQ((int)t_raft_last_applied(r), 0);
    T_ASSERT_EQ(t_raft_apply_entries(r), 0);
    T_ASSERT_EQ((int)t_raft_last_applied(r), 1);
    t_raft_destroy(r);
    t_file_unlink(path);
}

typedef struct {
    t_raft *fol;
} raft_repl_ctx;

static int test_inproc_repl(t_raft *r, uint64_t index, void *ud) {
    raft_repl_ctx *cx = (raft_repl_ctx *)ud;
    (void)index;
    t_wire_cluster_entry ents[8];
    uint32_t nent = 0;
    uint64_t last = t_raft_last_log_index(r);
    for (uint64_t i = 1; i <= last && nent < 8; i++) {
        const t_raft_entry *e = t_raft_get_entry(r, i);
        if (!e) break;
        ents[nent].index = e->index;
        ents[nent].term = e->term;
        ents[nent].type = e->type;
        ents[nent].data = e->data;
        ents[nent].data_len = (uint32_t)e->data_len;
        nent++;
    }
    t_wire_append_req ar;
    memset(&ar, 0, sizeof(ar));
    ar.term = t_raft_current_term(r);
    ar.leader_id = t_raft_id(r);
    ar.leader_commit = t_raft_commit_index(r);
    uint8_t buf[512], resp[256];
    int n = t_wire_encode_append_req(buf, sizeof(buf), &ar, ents, nent);
    if (n < 0) return -1;
    int m = t_raft_rpc(cx->fol, buf, (size_t)n, resp, sizeof(resp));
    if (m < 0) return -1;
    t_wire_append_resp ao;
    if (t_wire_decode_append_resp(resp, (size_t)m, &ao) != 0 || !ao.success)
        return -1;
    uint64_t match = ao.match_index;
    int rc = t_raft_majority_commit(r, &match, 1, 2);
    if (rc != 0) return rc;
    memset(&ar, 0, sizeof(ar));
    ar.term = t_raft_current_term(r);
    ar.leader_id = t_raft_id(r);
    ar.leader_commit = t_raft_commit_index(r);
    n = t_wire_encode_append_req(buf, sizeof(buf), &ar, NULL, 0);
    if (n < 0) return -1;
    return (t_raft_rpc(cx->fol, buf, (size_t)n, resp, sizeof(resp)) < 0) ? -1 : 0;
}

static t_queue *broker_q(t_broker *b, const char *name) {
    t_domain *d = t_broker_get_domain(b, "default");
    return d ? (t_queue *)t_domain_get_queue(d, name) : NULL;
}

T_TEST(broker_raft_publish_uncommitted_not_visible) {
    t_broker *b = t_broker_create("n1");
    t_cluster *c = t_cluster_create(1);
    t_raft_config cfg = {1, 150, 50};
    t_raft *r = t_raft_create(&cfg);
    T_ASSERT_EQ(t_cluster_add_node(c, 1, "127.0.0.1", 1), 0);
    T_ASSERT_EQ(t_cluster_add_node(c, 2, "127.0.0.1", 2), 0);
    T_ASSERT_EQ(t_cluster_set_leader(c, 1), 0);
    T_ASSERT_EQ(t_raft_become_candidate(r), 0);
    T_ASSERT_EQ(t_raft_become_leader(r), 0);
    T_ASSERT_EQ(t_broker_set_cluster(b, c), 0);
    T_ASSERT_EQ(t_broker_start(b), 0);
    T_ASSERT_EQ(t_broker_create_queue(b, "default", "jobs", 0, 0), 0);
    T_ASSERT_EQ(t_broker_set_raft(b, r), 0);
    T_ASSERT_EQ(t_broker_publish(b, "jobs", (const uint8_t *)"a", 1, 0), -1);
    T_ASSERT_EQ((int)t_queue_pending_count(broker_q(b, "jobs")), 0);
    T_ASSERT_EQ((int)t_raft_commit_index(r), 0);
    t_broker_destroy(b);
    t_raft_destroy(r);
    t_cluster_destroy(c);
}

T_TEST(broker_raft_publish_and_ack_two_nodes) {
    t_broker *b1 = t_broker_create("n1");
    t_broker *b2 = t_broker_create("n2");
    t_cluster *c = t_cluster_create(1);
    t_raft_config c1 = {1, 150, 50};
    t_raft_config c2 = {2, 150, 50};
    t_raft *r1 = t_raft_create(&c1);
    t_raft *r2 = t_raft_create(&c2);
    T_ASSERT_EQ(t_cluster_add_node(c, 1, "127.0.0.1", 1), 0);
    T_ASSERT_EQ(t_cluster_add_node(c, 2, "127.0.0.1", 2), 0);
    T_ASSERT_EQ(t_cluster_set_leader(c, 1), 0);
    T_ASSERT_EQ(t_raft_become_candidate(r1), 0);
    T_ASSERT_EQ(t_raft_become_leader(r1), 0);
    raft_repl_ctx cx = { r2 };
    t_raft_set_replicate_cb(r1, test_inproc_repl, &cx);
    T_ASSERT_EQ(t_broker_set_cluster(b1, c), 0);
    T_ASSERT_EQ(t_broker_set_raft(b1, r1), 0);
    T_ASSERT_EQ(t_broker_set_raft(b2, r2), 0);
    T_ASSERT_EQ(t_broker_start(b1), 0);
    T_ASSERT_EQ(t_broker_start(b2), 0);
    T_ASSERT_EQ(t_broker_create_queue(b1, "default", "jobs", 0, T_QUEUE_FLAG_DURABLE), 0);
    T_ASSERT_EQ(t_broker_publish(b1, "jobs", (const uint8_t *)"hi", 2, 3), 0);
    t_queue *q1 = broker_q(b1, "jobs");
    t_queue *q2 = broker_q(b2, "jobs");
    T_ASSERT_NOT_NULL(q1);
    T_ASSERT_NOT_NULL(q2);
    T_ASSERT_EQ((int)t_queue_pending_count(q1), 1);
    T_ASSERT_EQ((int)t_queue_pending_count(q2), 1);
    t_msg m1, m2;
    T_ASSERT_EQ(t_queue_consume(q1, &m1), 0);
    T_ASSERT_EQ(t_queue_consume(q2, &m2), 0);
    T_ASSERT_EQ((int)m1.msg_id, (int)m2.msg_id);
    T_ASSERT_MEM_EQ(m1.data, (const uint8_t *)"hi", 2);
    T_ASSERT_EQ(t_broker_ack(b1, "jobs", m1.msg_id), 0);
    T_ASSERT(!t_queue_has_inflight(q1));
    T_ASSERT(!t_queue_has_inflight(q2));
    T_ASSERT_EQ((int)t_queue_pending_count(q1), 0);
    T_ASSERT_EQ((int)t_queue_pending_count(q2), 0);
    t_broker_destroy(b1);
    t_broker_destroy(b2);
    t_raft_destroy(r1);
    t_raft_destroy(r2);
    t_cluster_destroy(c);
}

T_TEST(broker_raft_nack_two_nodes) {
    t_broker *b1 = t_broker_create("n1");
    t_broker *b2 = t_broker_create("n2");
    t_cluster *c = t_cluster_create(1);
    t_raft_config c1 = {1, 150, 50};
    t_raft_config c2 = {2, 150, 50};
    t_raft *r1 = t_raft_create(&c1);
    t_raft *r2 = t_raft_create(&c2);
    T_ASSERT_EQ(t_cluster_add_node(c, 1, "127.0.0.1", 1), 0);
    T_ASSERT_EQ(t_cluster_add_node(c, 2, "127.0.0.1", 2), 0);
    T_ASSERT_EQ(t_cluster_set_leader(c, 1), 0);
    T_ASSERT_EQ(t_raft_become_candidate(r1), 0);
    T_ASSERT_EQ(t_raft_become_leader(r1), 0);
    raft_repl_ctx cx = { r2 };
    t_raft_set_replicate_cb(r1, test_inproc_repl, &cx);
    T_ASSERT_EQ(t_broker_set_cluster(b1, c), 0);
    T_ASSERT_EQ(t_broker_set_raft(b1, r1), 0);
    T_ASSERT_EQ(t_broker_set_raft(b2, r2), 0);
    T_ASSERT_EQ(t_broker_start(b1), 0);
    T_ASSERT_EQ(t_broker_start(b2), 0);
    T_ASSERT_EQ(t_broker_create_queue(b1, "default", "jobs", 0, T_QUEUE_FLAG_DURABLE), 0);
    T_ASSERT_EQ(t_broker_publish(b1, "jobs", (const uint8_t *)"hi", 2, 3), 0);
    t_queue *q1 = broker_q(b1, "jobs");
    t_queue *q2 = broker_q(b2, "jobs");
    t_msg m1, m2;
    T_ASSERT_EQ(t_queue_consume(q1, &m1), 0);
    T_ASSERT_EQ(t_queue_consume(q2, &m2), 0);
    T_ASSERT_EQ(t_broker_nack(b1, "jobs", m1.msg_id), 0);
    T_ASSERT(!t_queue_has_inflight(q1));
    T_ASSERT(!t_queue_has_inflight(q2));
    T_ASSERT_EQ((int)t_queue_pending_count(q1), 1);
    T_ASSERT_EQ((int)t_queue_pending_count(q2), 1);
    T_ASSERT_EQ(t_queue_consume(q1, &m1), 0);
    T_ASSERT_MEM_EQ(m1.data, (const uint8_t *)"hi", 2);
    t_broker_destroy(b1);
    t_broker_destroy(b2);
    t_raft_destroy(r1);
    t_raft_destroy(r2);
    t_cluster_destroy(c);
}

T_TEST(broker_raft_nack_uncommitted) {
    t_broker *b = t_broker_create("n1");
    t_cluster *c = t_cluster_create(1);
    t_raft_config cfg = {1, 150, 50};
    t_raft *r = t_raft_create(&cfg);
    T_ASSERT_EQ(t_cluster_add_node(c, 1, "127.0.0.1", 1), 0);
    T_ASSERT_EQ(t_cluster_set_leader(c, 1), 0);
    T_ASSERT_EQ(t_raft_become_candidate(r), 0);
    T_ASSERT_EQ(t_raft_become_leader(r), 0);
    T_ASSERT_EQ(t_broker_set_cluster(b, c), 0);
    T_ASSERT_EQ(t_broker_set_raft(b, r), 0);
    T_ASSERT_EQ(t_broker_start(b), 0);
    T_ASSERT_EQ(t_broker_create_queue(b, "default", "jobs", 0, 0), 0);
    T_ASSERT_EQ(t_broker_publish(b, "jobs", (const uint8_t *)"a", 1, 0), 0);
    t_queue *q = broker_q(b, "jobs");
    t_msg m;
    T_ASSERT_EQ(t_queue_consume(q, &m), 0);
    T_ASSERT_EQ(t_cluster_add_node(c, 2, "127.0.0.1", 2), 0);
    T_ASSERT_EQ(t_broker_nack(b, "jobs", m.msg_id), -1);
    T_ASSERT(t_queue_has_inflight(q));
    T_ASSERT_EQ((int)t_queue_pending_count(q), 0);
    t_broker_destroy(b);
    t_raft_destroy(r);
    t_cluster_destroy(c);
}

T_TEST(broker_raft_create_delete_two_nodes) {
    t_broker *b1 = t_broker_create("n1");
    t_broker *b2 = t_broker_create("n2");
    t_cluster *c = t_cluster_create(1);
    t_raft_config a = {1, 150, 50};
    t_raft_config b = {2, 150, 50};
    t_raft *r1 = t_raft_create(&a);
    t_raft *r2 = t_raft_create(&b);
    T_ASSERT_EQ(t_cluster_add_node(c, 1, "127.0.0.1", 1), 0);
    T_ASSERT_EQ(t_cluster_add_node(c, 2, "127.0.0.1", 2), 0);
    T_ASSERT_EQ(t_cluster_set_leader(c, 1), 0);
    T_ASSERT_EQ(t_raft_become_candidate(r1), 0);
    T_ASSERT_EQ(t_raft_become_leader(r1), 0);
    raft_repl_ctx cx = { r2 };
    t_raft_set_replicate_cb(r1, test_inproc_repl, &cx);
    T_ASSERT_EQ(t_broker_set_cluster(b1, c), 0);
    T_ASSERT_EQ(t_broker_set_raft(b1, r1), 0);
    T_ASSERT_EQ(t_broker_set_raft(b2, r2), 0);
    T_ASSERT_EQ(t_broker_start(b1), 0);
    T_ASSERT_EQ(t_broker_start(b2), 0);
    T_ASSERT_EQ(t_broker_create_queue(b1, "default", "jobs", 0, 0), 0);
    T_ASSERT_NOT_NULL(broker_q(b1, "jobs"));
    T_ASSERT_NOT_NULL(broker_q(b2, "jobs"));
    T_ASSERT_EQ(t_broker_delete_queue(b1, "default", "jobs"), 0);
    T_ASSERT_NULL(broker_q(b1, "jobs"));
    T_ASSERT_NULL(broker_q(b2, "jobs"));
    t_broker_destroy(b1);
    t_broker_destroy(b2);
    t_raft_destroy(r1);
    t_raft_destroy(r2);
    t_cluster_destroy(c);
}

T_TEST(broker_raft_single_node_commits) {
    t_broker *b = t_broker_create("n1");
    t_cluster *c = t_cluster_create(1);
    t_raft_config cfg = {1, 150, 50};
    t_raft *r = t_raft_create(&cfg);
    T_ASSERT_EQ(t_cluster_add_node(c, 1, "127.0.0.1", 1), 0);
    T_ASSERT_EQ(t_cluster_set_leader(c, 1), 0);
    T_ASSERT_EQ(t_raft_become_candidate(r), 0);
    T_ASSERT_EQ(t_raft_become_leader(r), 0);
    T_ASSERT_EQ(t_broker_set_cluster(b, c), 0);
    T_ASSERT_EQ(t_broker_set_raft(b, r), 0);
    T_ASSERT_EQ(t_broker_start(b), 0);
    T_ASSERT_EQ(t_broker_create_queue(b, "default", "jobs", 0, 0), 0);
    T_ASSERT_EQ(t_broker_publish(b, "jobs", (const uint8_t *)"z", 1, 0), 0);
    T_ASSERT_EQ((int)t_queue_pending_count(broker_q(b, "jobs")), 1);
    t_broker_destroy(b);
    t_raft_destroy(r);
    t_cluster_destroy(c);
}

T_TEST(broker_snapshot_roundtrip) {
    t_broker *b = t_broker_create("n1");
    T_ASSERT_EQ(t_broker_start(b), 0);
    T_ASSERT_EQ(t_broker_create_queue(b, "default", "jobs", 0, 0), 0);
    T_ASSERT_EQ(t_broker_publish(b, "jobs", (const uint8_t *)"hi", 2, 7), 0);
    uint8_t *snap = NULL;
    size_t slen = 0;
    T_ASSERT_EQ(t_broker_snapshot_encode(b, &snap, &slen), 0);
    T_ASSERT(slen > 0);
    t_broker *b2 = t_broker_create("n2");
    T_ASSERT_EQ(t_broker_start(b2), 0);
    T_ASSERT_EQ(t_broker_snapshot_apply(b2, snap, slen), 0);
    T_ASSERT_EQ(t_broker_snapshot_apply(b2, snap, slen), 0);
    t_queue *q = broker_q(b2, "jobs");
    T_ASSERT_NOT_NULL(q);
    T_ASSERT_EQ((int)t_queue_pending_count(q), 1);
    t_msg m;
    T_ASSERT_EQ(t_queue_consume(q, &m), 0);
    T_ASSERT_MEM_EQ(m.data, (const uint8_t *)"hi", 2);
    T_ASSERT_EQ(m.priority, 7);
    free(snap);
    t_broker_destroy(b2);
    t_broker_destroy(b);
}

T_TEST(raft_compact_drops_prefix) {
    t_raft_config cfg = {1, 150, 50};
    t_raft *r = t_raft_create(&cfg);
    T_ASSERT_EQ(t_raft_become_candidate(r), 0);
    T_ASSERT_EQ(t_raft_become_leader(r), 0);
    uint8_t d[] = {1};
    T_ASSERT_EQ(t_raft_append_entry(r, 1, d, 1), 0);
    T_ASSERT_EQ(t_raft_append_entry(r, 1, d, 1), 0);
    T_ASSERT_EQ(t_raft_majority_commit(r, NULL, 0, 1), 0);
    T_ASSERT_EQ((int)t_raft_last_applied(r), 2);
    uint8_t empty[] = {0};
    T_ASSERT_EQ(t_raft_snapshot(r, empty, 0), 0);
    T_ASSERT_EQ((int)t_raft_log_count(r), 0);
    T_ASSERT_EQ((int)t_raft_last_log_index(r), 2);
    T_ASSERT_NULL(t_raft_get_entry(r, 1));
    T_ASSERT_EQ(t_raft_append_entry(r, 1, d, 1), 0);
    T_ASSERT_EQ((int)t_raft_last_log_index(r), 3);
    T_ASSERT_NOT_NULL(t_raft_get_entry(r, 3));
    t_raft_destroy(r);
}

T_TEST(raft_maybe_snapshot_needs_all_peers) {
    t_broker *b = t_broker_create("n1");
    t_raft_config cfg = {1, 150, 50};
    t_raft *r = t_raft_create(&cfg);
    T_ASSERT_EQ(t_raft_become_candidate(r), 0);
    T_ASSERT_EQ(t_raft_become_leader(r), 0);
    T_ASSERT_EQ(t_broker_set_raft(b, r), 0);
    T_ASSERT_EQ(t_broker_start(b), 0);
    T_ASSERT_EQ(t_broker_create_queue(b, "default", "jobs", 0, 0), 0);
    T_ASSERT((int)t_raft_log_count(r) >= 1);
    t_raft_set_compact_min(r, 1);
    uint64_t behind = 0;
    T_ASSERT_EQ(t_raft_maybe_snapshot(r, &behind, 1, 2), 0);
    T_ASSERT((int)t_raft_log_count(r) >= 1);
    uint64_t caught = t_raft_last_applied(r);
    T_ASSERT_EQ(t_raft_maybe_snapshot(r, &caught, 1, 2), 0);
    T_ASSERT_EQ((int)t_raft_log_count(r), 0);
    T_ASSERT_EQ((int)t_raft_snapshot_index(r), (int)caught);
    t_broker_destroy(b);
    t_raft_destroy(r);
}

T_TEST(raft_snapshot_survives_reopen) {
    const char *path = "test_transit_raft_snap.log";
    t_file_unlink(path);
    t_file_unlink("test_transit_raft_snap.log.snap");
    t_broker *b = t_broker_create("n1");
    t_raft_config cfg = {1, 150, 50};
    t_raft *r = t_raft_create(&cfg);
    T_ASSERT_EQ(t_raft_open_log(r, path, 1), 0);
    T_ASSERT_EQ(t_raft_become_candidate(r), 0);
    T_ASSERT_EQ(t_raft_become_leader(r), 0);
    T_ASSERT_EQ(t_broker_set_raft(b, r), 0);
    T_ASSERT_EQ(t_broker_start(b), 0);
    T_ASSERT_EQ(t_broker_create_queue(b, "default", "jobs", 0, 0), 0);
    T_ASSERT_EQ(t_broker_publish(b, "jobs", (const uint8_t *)"zz", 2, 0), 0);
    uint8_t *snap = NULL;
    size_t slen = 0;
    T_ASSERT_EQ(t_broker_snapshot_encode(b, &snap, &slen), 0);
    T_ASSERT_EQ(t_raft_snapshot(r, snap, slen), 0);
    free(snap);
    T_ASSERT_EQ((int)t_raft_log_count(r), 0);
    t_broker_destroy(b);
    t_raft_destroy(r);

    b = t_broker_create("n1");
    r = t_raft_create(&cfg);
    T_ASSERT_EQ(t_raft_open_log(r, path, 1), 0);
    T_ASSERT_EQ((int)t_raft_last_applied(r), (int)t_raft_snapshot_index(r));
    T_ASSERT((int)t_raft_snapshot_index(r) > 0);
    T_ASSERT_EQ(t_broker_set_raft(b, r), 0);
    T_ASSERT_EQ(t_broker_start(b), 0);
    T_ASSERT_EQ(t_raft_apply_entries(r), 0);
    t_queue *q = broker_q(b, "jobs");
    T_ASSERT_NOT_NULL(q);
    T_ASSERT_EQ((int)t_queue_pending_count(q), 1);
    t_msg m;
    T_ASSERT_EQ(t_queue_consume(q, &m), 0);
    T_ASSERT_MEM_EQ(m.data, (const uint8_t *)"zz", 2);
    t_broker_destroy(b);
    t_raft_destroy(r);
    t_file_unlink(path);
    t_file_unlink("test_transit_raft_snap.log.snap");
}

T_TEST(raft_install_snapshot_catchup) {
    t_broker *b1 = t_broker_create("n1");
    t_raft_config c1 = {1, 150, 50};
    t_raft *r1 = t_raft_create(&c1);
    T_ASSERT_EQ(t_raft_become_candidate(r1), 0);
    T_ASSERT_EQ(t_raft_become_leader(r1), 0);
    T_ASSERT_EQ(t_broker_set_raft(b1, r1), 0);
    T_ASSERT_EQ(t_broker_start(b1), 0);
    T_ASSERT_EQ(t_broker_create_queue(b1, "default", "jobs", 0, 0), 0);
    T_ASSERT_EQ(t_broker_publish(b1, "jobs", (const uint8_t *)"hi", 2, 0), 0);
    uint8_t *enc = NULL;
    size_t elen = 0;
    T_ASSERT_EQ(t_broker_snapshot_encode(b1, &enc, &elen), 0);
    T_ASSERT_EQ(t_raft_snapshot(r1, enc, elen), 0);
    free(enc);
    T_ASSERT_EQ((int)t_raft_log_count(r1), 0);
    const uint8_t *bytes = NULL;
    size_t blen = 0;
    T_ASSERT_EQ(t_raft_snapshot_bytes(r1, &bytes, &blen), 0);

    t_broker *b2 = t_broker_create("n2");
    t_raft_config c2 = {2, 150, 50};
    t_raft *r2 = t_raft_create(&c2);
    T_ASSERT_EQ(t_broker_set_raft(b2, r2), 0);
    T_ASSERT_EQ(t_broker_start(b2), 0);
    T_ASSERT_NULL(broker_q(b2, "jobs"));
    uint8_t req[4096], resp[64];
    t_wire_snap_req sr;
    memset(&sr, 0, sizeof(sr));
    sr.term = t_raft_current_term(r1);
    sr.leader_id = 1;
    sr.last_index = t_raft_snapshot_index(r1);
    sr.last_term = t_raft_snapshot_term(r1);
    sr.data = bytes;
    sr.data_len = (uint32_t)blen;
    int n = t_wire_encode_snap_req(req, sizeof(req), &sr);
    T_ASSERT(n > 0);
    int m = t_raft_rpc(r2, req, (size_t)n, resp, sizeof(resp));
    T_ASSERT(m > 0);
    t_wire_snap_resp so;
    T_ASSERT_EQ(t_wire_decode_snap_resp(resp, (size_t)m, &so), 0);
    T_ASSERT_EQ((int)so.success, 1);
    T_ASSERT_EQ((int)t_raft_snapshot_index(r2), (int)t_raft_snapshot_index(r1));
    t_queue *q = broker_q(b2, "jobs");
    T_ASSERT_NOT_NULL(q);
    T_ASSERT_EQ((int)t_queue_pending_count(q), 1);
    t_msg msg;
    T_ASSERT_EQ(t_queue_consume(q, &msg), 0);
    T_ASSERT_MEM_EQ(msg.data, (const uint8_t *)"hi", 2);
    t_broker_destroy(b2);
    t_broker_destroy(b1);
    t_raft_destroy(r2);
    t_raft_destroy(r1);
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
