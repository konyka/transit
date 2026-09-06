#include "t_test.h"
#include "t_peer.h"
#include "t_cluster.h"
#include "t_raft.h"
#include "t_node.h"
#include "t_evloop.h"
#include <string.h>
#include <stdint.h>

static void peer_timer_stop(void *ud) {
    t_evloop_stop((t_evloop *)ud);
}

static void peer_pump(t_evloop *loop, int ms) {
    (void)t_evloop_timer_add(loop, ms, 0, peer_timer_stop, loop);
    t_evloop_run(loop, 20);
}

typedef struct {
    t_evloop  *loop;
    t_cluster *c1;
    t_cluster *c2;
    t_raft    *r1;
    t_raft    *r2;
    t_peer    *p1;
    t_peer    *p2;
} peer_pair;

static void peer_pair_destroy(peer_pair *pp) {
    if (!pp) return;
    if (pp->p1) t_peer_destroy(pp->p1);
    if (pp->p2) t_peer_destroy(pp->p2);
    if (pp->r1) t_raft_destroy(pp->r1);
    if (pp->r2) t_raft_destroy(pp->r2);
    if (pp->c1) t_cluster_destroy(pp->c1);
    if (pp->c2) t_cluster_destroy(pp->c2);
    if (pp->loop) t_evloop_destroy(pp->loop);
    memset(pp, 0, sizeof(*pp));
}

static int peer_pair_start(peer_pair *pp, uint64_t e1, uint64_t h1,
                           uint64_t e2, uint64_t h2) {
    memset(pp, 0, sizeof(*pp));
    pp->loop = t_evloop_create();
    pp->c1 = t_cluster_create(1);
    pp->c2 = t_cluster_create(2);
    t_raft_config a = {1, e1, h1};
    t_raft_config b = {2, e2, h2};
    pp->r1 = t_raft_create(&a);
    pp->r2 = t_raft_create(&b);
    t_peer_config cfg;
    t_peer_config_init(&cfg);
    cfg.port = 0;
    pp->p1 = t_peer_create(pp->loop, pp->r1, pp->c1, &cfg);
    pp->p2 = t_peer_create(pp->loop, pp->r2, pp->c2, &cfg);
    if (!pp->loop || !pp->c1 || !pp->c2 || !pp->r1 || !pp->r2 ||
        !pp->p1 || !pp->p2)
        return -1;
    if (t_peer_start(pp->p1) != 0 || t_peer_start(pp->p2) != 0)
        return -1;
    uint16_t pa = t_peer_port(pp->p1);
    uint16_t pb = t_peer_port(pp->p2);
    if (pa == 0 || pb == 0) return -1;
    if (t_cluster_add_node(pp->c1, 1, "127.0.0.1", pa) != 0) return -1;
    if (t_cluster_add_node(pp->c1, 2, "127.0.0.1", pb) != 0) return -1;
    if (t_cluster_add_node(pp->c2, 1, "127.0.0.1", pa) != 0) return -1;
    if (t_cluster_add_node(pp->c2, 2, "127.0.0.1", pb) != 0) return -1;
    return 0;
}

T_TEST(peer_config_defaults) {
    t_peer_config cfg;
    t_peer_config_init(&cfg);
    T_ASSERT_STR_EQ(cfg.host, "127.0.0.1");
    T_ASSERT_EQ((int)cfg.port, 4223);
    T_ASSERT_NULL(t_peer_create(NULL, NULL, NULL, NULL));
}

T_TEST(peer_listen_loopback) {
    t_evloop *loop = t_evloop_create();
    t_cluster *c = t_cluster_create(1);
    t_raft_config rc = {1, 40, 15};
    t_raft *r = t_raft_create(&rc);
    t_peer_config cfg;
    t_peer_config_init(&cfg);
    cfg.port = 0;
    t_peer *p = t_peer_create(loop, r, c, &cfg);
    T_ASSERT_NOT_NULL(p);
    T_ASSERT_EQ(t_peer_start(p), 0);
    T_ASSERT(t_peer_is_running(p));
    T_ASSERT(t_peer_port(p) > 0);
    T_ASSERT_STR_EQ(t_peer_host(p), "127.0.0.1");
    T_ASSERT(!t_peer_is_leader(p));
    t_peer_destroy(p);
    t_raft_destroy(r);
    t_cluster_destroy(c);
    t_evloop_destroy(loop);
}

T_TEST(peer_campaign_elects) {
    peer_pair pp;
    T_ASSERT_EQ(peer_pair_start(&pp, 40, 15, 40, 15), 0);
    T_ASSERT_EQ(t_peer_campaign(pp.p1), 0);
    peer_pump(pp.loop, 80);
    T_ASSERT(t_peer_is_leader(pp.p1));
    T_ASSERT(!t_peer_is_leader(pp.p2));
    T_ASSERT(t_cluster_is_leader(pp.c1));
    T_ASSERT_EQ((int)t_node_id(t_cluster_get_leader(pp.c2)), 1);
    peer_pair_destroy(&pp);
}

T_TEST(peer_election_timeout) {
    peer_pair pp;
    T_ASSERT_EQ(peer_pair_start(&pp, 30, 10, 400, 10), 0);
    peer_pump(pp.loop, 200);
    T_ASSERT(t_peer_is_leader(pp.p1));
    T_ASSERT(!t_peer_is_leader(pp.p2));
    peer_pair_destroy(&pp);
}

T_TEST(peer_heartbeat_replicates) {
    peer_pair pp;
    T_ASSERT_EQ(peer_pair_start(&pp, 40, 15, 40, 15), 0);
    T_ASSERT_EQ(t_peer_campaign(pp.p1), 0);
    peer_pump(pp.loop, 80);
    T_ASSERT(t_peer_is_leader(pp.p1));
    uint8_t data[] = {'x'};
    T_ASSERT_EQ(t_raft_append_entry(pp.r1, 1, data, 1), 0);
    peer_pump(pp.loop, 100);
    T_ASSERT_EQ((int)t_raft_log_count(pp.r2), 1);
    const t_raft_entry *e = t_raft_get_entry(pp.r2, 1);
    T_ASSERT_NOT_NULL(e);
    T_ASSERT_MEM_EQ(e->data, data, 1);
    T_ASSERT_EQ((int)t_raft_commit_index(pp.r1), 1);
    T_ASSERT_EQ((int)t_raft_last_applied(pp.r1), 1);
    peer_pump(pp.loop, 80);
    T_ASSERT_EQ((int)t_raft_commit_index(pp.r2), 1);
    T_ASSERT_EQ((int)t_raft_last_applied(pp.r2), 1);
    peer_pair_destroy(&pp);
}

int main(void) {
    return t_run_all_tests();
}
