#include "t_test.h"
#include "t_server.h"
#include "t_broker.h"
#include "t_domain.h"
#include "t_queue.h"
#include "t_evloop.h"
#include "t_client.h"
#include "t_error.h"
#include "t_socket.h"
#include "t_cluster.h"
#include "t_raft.h"
#include "t_conn.h"
#include "t_wire.h"
#include "t_proto.h"
#include "t_thread.h"
#include "t_time.h"
#include <string.h>
#include <stdint.h>

static void timer_stop(void *ud) {
    t_evloop_stop((t_evloop *)ud);
}

/* Slice-sleep until predicate; fail closed at timeout (Windows poll can be coarse). */
static int wait_flag_ge(volatile int *flag, int want, int timeout_ms) {
    int64_t start = t_time_now_ms();
    while (*flag < want && t_time_now_ms() - start < timeout_ms)
        t_time_sleep_ms(5);
    return *flag >= want;
}

/* last_status starts at 0; only ack_seq proves an ACK arrived. */
static int wait_next_ack(t_client *c, unsigned prev, int timeout_ms) {
    int64_t start = t_time_now_ms();
    while (t_client_ack_seq(c) == prev && t_time_now_ms() - start < timeout_ms)
        t_time_sleep_ms(5);
    return t_client_ack_seq(c) != prev;
}

static int wait_ack_status(t_client *c, unsigned prev, int want, int timeout_ms) {
    if (!wait_next_ack(c, prev, timeout_ms)) return 0;
    return t_client_last_status(c) == want;
}

static void *loop_runner(void *arg) {
    t_evloop *loop = (t_evloop *)arg;
    t_evloop_timer_add(loop, 4000, 0, timer_stop, loop);
    t_evloop_run(loop, 50);
    return NULL;
}

T_TEST(server_config_defaults) {
    t_server_config cfg;
    t_server_config_init(&cfg);
    T_ASSERT_STR_EQ(cfg.host, "127.0.0.1");
    T_ASSERT_EQ((int)cfg.port, 4222);
    T_ASSERT(cfg.max_conns > 0);
    T_ASSERT(cfg.idle_timeout_ms > 0);
    T_ASSERT_EQ((int)cfg.push_credits, T_SERVER_PUSH_CREDITS_DEFAULT);
}

T_TEST(server_create_start_stop) {
    t_evloop *loop = t_evloop_create();
    t_broker *b = t_broker_create("n0");
    T_ASSERT_NOT_NULL(loop);
    T_ASSERT_NOT_NULL(b);
    T_ASSERT_EQ(t_broker_start(b), 0);
    t_server_config cfg;
    t_server_config_init(&cfg);
    cfg.port = 0;
    cfg.idle_timeout_ms = 0;
    t_server *srv = t_server_create(loop, b, &cfg);
    T_ASSERT_NOT_NULL(srv);
    T_ASSERT_STR_EQ(t_server_host(srv), "127.0.0.1");
    T_ASSERT_EQ(t_server_start(srv), 0);
    T_ASSERT(t_server_is_running(srv));
    T_ASSERT(t_server_port(srv) > 0);
    t_server_stop(srv);
    T_ASSERT(!t_server_is_running(srv));
    t_server_destroy(srv);
    t_broker_destroy(b);
    t_evloop_destroy(loop);
}

T_TEST(server_rejects_nulls) {
    T_ASSERT_NULL(t_server_create(NULL, NULL, NULL));
}

T_TEST(server_max_conns) {
    t_evloop *loop = t_evloop_create();
    t_broker *b = t_broker_create("n0");
    t_broker_start(b);
    t_server_config cfg;
    t_server_config_init(&cfg);
    cfg.port = 0;
    cfg.max_conns = 1;
    cfg.idle_timeout_ms = 0;
    t_server *srv = t_server_create(loop, b, &cfg);
    T_ASSERT_EQ(t_server_start(srv), 0);
    uint16_t port = t_server_port(srv);

    t_thread th;
    T_ASSERT_EQ(t_thread_spawn(&th, loop_runner, loop), 0);
    t_time_sleep_us(20000);

    t_client *c1 = t_client_create("a");
    t_client *c2 = t_client_create("b");
    T_ASSERT_EQ(t_client_dial(c1, loop, "127.0.0.1", port), 0);
    {
        int64_t start = t_time_now_ms();
        while ((int)t_server_conn_count(srv) < 1 && t_time_now_ms() - start < 500)
            t_time_sleep_ms(5);
    }
    T_ASSERT_EQ((int)t_server_conn_count(srv), 1);
    T_ASSERT_EQ(t_client_dial(c2, loop, "127.0.0.1", port), 0);
    {
        int64_t start = t_time_now_ms();
        while (t_server_dropped_conns(srv) < 1 && t_time_now_ms() - start < 500)
            t_time_sleep_ms(5);
    }
    T_ASSERT_EQ((int)t_server_conn_count(srv), 1);
    T_ASSERT(t_server_dropped_conns(srv) >= 1);

    t_evloop_stop(loop);
    T_ASSERT_EQ(t_thread_join(&th), 0);
    t_client_destroy(c1);
    t_client_destroy(c2);
    t_server_destroy(srv);
    t_broker_destroy(b);
    t_evloop_destroy(loop);
}

T_TEST(server_post_requires_open) {
    t_evloop *loop = t_evloop_create();
    t_broker *b = t_broker_create("n0");
    t_broker_start(b);
    t_server_config cfg;
    t_server_config_init(&cfg);
    cfg.port = 0;
    cfg.idle_timeout_ms = 0;
    t_server *srv = t_server_create(loop, b, &cfg);
    t_server_start(srv);
    uint16_t port = t_server_port(srv);

    t_thread th;
    T_ASSERT_EQ(t_thread_spawn(&th, loop_runner, loop), 0);
    t_time_sleep_us(20000);

    t_client *c = t_client_create("c");
    T_ASSERT_EQ(t_client_dial(c, loop, "127.0.0.1", port), 0);
    /* Consumer-only OPEN must not send POST or bump published. */
    unsigned seq = t_client_ack_seq(c);
    T_ASSERT_EQ(t_client_open_queue(c, "only.consume", T_CLIENT_OPEN_CONSUMER), 0);
    T_ASSERT(wait_ack_status(c, seq, 0, 500));
    T_ASSERT_EQ(t_client_post(c, "only.consume", (const uint8_t *)"x", 1, 0), -1);
    T_ASSERT_EQ((int)t_client_total_published(c), 0);

    t_evloop_stop(loop);
    T_ASSERT_EQ(t_thread_join(&th), 0);
    t_client_destroy(c);
    t_server_destroy(srv);
    t_broker_destroy(b);
    t_evloop_destroy(loop);
}

static volatile int g_got;
static char g_payload[64];

static void on_net_msg(const char *queue_name, const uint8_t *data, size_t len, void *ud) {
    (void)queue_name;
    (void)ud;
    g_got++;
    size_t n = len < sizeof(g_payload) - 1 ? len : sizeof(g_payload) - 1;
    memcpy(g_payload, data, n);
    g_payload[n] = '\0';
}

T_TEST(server_pubsub_tcp) {
    t_evloop *loop = t_evloop_create();
    t_broker *b = t_broker_create("n0");
    t_broker_start(b);
    t_server_config cfg;
    t_server_config_init(&cfg);
    cfg.port = 0;
    cfg.idle_timeout_ms = 0;
    t_server *srv = t_server_create(loop, b, &cfg);
    t_server_start(srv);
    uint16_t port = t_server_port(srv);

    t_thread th;
    T_ASSERT_EQ(t_thread_spawn(&th, loop_runner, loop), 0);
    t_time_sleep_us(20000);

    t_client *prod = t_client_create("p");
    t_client *cons = t_client_create("c");
    T_ASSERT_EQ(t_client_dial(prod, loop, "127.0.0.1", port), 0);
    T_ASSERT_EQ(t_client_dial(cons, loop, "127.0.0.1", port), 0);
    unsigned cseq = t_client_ack_seq(cons);
    T_ASSERT_EQ(t_client_open_queue(cons, "jobs", T_CLIENT_OPEN_CONSUMER), 0);
    T_ASSERT(wait_ack_status(cons, cseq, 0, 500));
    T_ASSERT_EQ(t_client_subscribe(cons, "jobs", on_net_msg, NULL), 0);
    unsigned pseq = t_client_ack_seq(prod);
    T_ASSERT_EQ(t_client_open_queue(prod, "jobs", T_CLIENT_OPEN_PRODUCER), 0);
    T_ASSERT(wait_ack_status(prod, pseq, 0, 500));
    g_got = 0;
    g_payload[0] = 0;
    pseq = t_client_ack_seq(prod);
    T_ASSERT_EQ(t_client_post(prod, "jobs", (const uint8_t *)"hello", 5, 0), 0);
    T_ASSERT(wait_flag_ge(&g_got, 1, 500));
    T_ASSERT_STR_EQ(g_payload, "hello");
    T_ASSERT(wait_ack_status(prod, pseq, 0, 500));

    t_evloop_stop(loop);
    T_ASSERT_EQ(t_thread_join(&th), 0);
    t_client_destroy(prod);
    t_client_destroy(cons);
    t_server_destroy(srv);
    t_broker_destroy(b);
    t_evloop_destroy(loop);
}

T_TEST(server_rate_limit_busy) {
    t_evloop *loop = t_evloop_create();
    t_broker *b = t_broker_create("n0");
    t_broker_start(b);
    t_server_config cfg;
    t_server_config_init(&cfg);
    cfg.port = 0;
    cfg.idle_timeout_ms = 0;
    cfg.rate_tokens = 1;
    cfg.rate_refill = 0.0;
    t_server *srv = t_server_create(loop, b, &cfg);
    t_server_start(srv);
    uint16_t port = t_server_port(srv);

    t_thread th;
    T_ASSERT_EQ(t_thread_spawn(&th, loop_runner, loop), 0);
    t_time_sleep_us(20000);

    t_client *c = t_client_create("rl");
    T_ASSERT_EQ(t_client_dial(c, loop, "127.0.0.1", port), 0);
    unsigned seq = t_client_ack_seq(c);
    T_ASSERT_EQ(t_client_open_queue(c, "rl.q", T_CLIENT_OPEN_PRODUCER), 0);
    T_ASSERT(wait_ack_status(c, seq, 0, 500));
    seq = t_client_ack_seq(c);
    T_ASSERT_EQ(t_client_post(c, "rl.q", (const uint8_t *)"a", 1, 0), 0);
    T_ASSERT(wait_ack_status(c, seq, (int)T_ERR_BUSY, 500));
    T_ASSERT(t_server_msgs_dropped(srv) >= 1);

    t_evloop_stop(loop);
    T_ASSERT_EQ(t_thread_join(&th), 0);
    t_client_destroy(c);
    t_server_destroy(srv);
    t_broker_destroy(b);
    t_evloop_destroy(loop);
}

T_TEST(server_durable_needs_datadir) {
    t_evloop *loop = t_evloop_create();
    t_broker *b = t_broker_create("n0");
    t_broker_start(b);
    t_server_config cfg;
    t_server_config_init(&cfg);
    cfg.port = 0;
    cfg.idle_timeout_ms = 0;
    t_server *srv = t_server_create(loop, b, &cfg);
    t_server_start(srv);
    uint16_t port = t_server_port(srv);

    t_thread th;
    T_ASSERT_EQ(t_thread_spawn(&th, loop_runner, loop), 0);
    t_time_sleep_us(20000);

    t_client *c = t_client_create("d");
    T_ASSERT_EQ(t_client_dial(c, loop, "127.0.0.1", port), 0);
    unsigned seq = t_client_ack_seq(c);
    T_ASSERT_EQ(t_client_open_queue(c, "dur.q",
                                    T_CLIENT_OPEN_PRODUCER | T_CLIENT_QFLAG_DURABLE), 0);
    T_ASSERT(wait_ack_status(c, seq, (int)T_ERR_IO, 500));

    t_evloop_stop(loop);
    T_ASSERT_EQ(t_thread_join(&th), 0);
    t_client_destroy(c);
    t_server_destroy(srv);
    t_broker_destroy(b);
    t_evloop_destroy(loop);
}

T_TEST(server_exclusive_second_consumer) {
    t_evloop *loop = t_evloop_create();
    t_broker *b = t_broker_create("n0");
    t_broker_start(b);
    t_server_config cfg;
    t_server_config_init(&cfg);
    cfg.port = 0;
    cfg.idle_timeout_ms = 0;
    t_server *srv = t_server_create(loop, b, &cfg);
    t_server_start(srv);
    uint16_t port = t_server_port(srv);

    t_thread th;
    T_ASSERT_EQ(t_thread_spawn(&th, loop_runner, loop), 0);
    t_time_sleep_us(20000);

    t_client *c1 = t_client_create("ex1");
    t_client *c2 = t_client_create("ex2");
    T_ASSERT_EQ(t_client_dial(c1, loop, "127.0.0.1", port), 0);
    T_ASSERT_EQ(t_client_dial(c2, loop, "127.0.0.1", port), 0);
    unsigned s1 = t_client_ack_seq(c1);
    T_ASSERT_EQ(t_client_open_queue(c1, "ex.q",
                                    T_CLIENT_OPEN_CONSUMER | T_CLIENT_QFLAG_EXCLUSIVE), 0);
    T_ASSERT(wait_ack_status(c1, s1, 0, 500));
    unsigned s2 = t_client_ack_seq(c2);
    T_ASSERT_EQ(t_client_open_queue(c2, "ex.q",
                                    T_CLIENT_OPEN_CONSUMER | T_CLIENT_QFLAG_EXCLUSIVE), 0);
    T_ASSERT(wait_ack_status(c2, s2, (int)T_ERR_BUSY, 500));

    t_evloop_stop(loop);
    T_ASSERT_EQ(t_thread_join(&th), 0);
    t_client_destroy(c1);
    t_client_destroy(c2);
    t_server_destroy(srv);
    t_broker_destroy(b);
    t_evloop_destroy(loop);
}

T_TEST(server_autodelete_on_last_close) {
    t_evloop *loop = t_evloop_create();
    t_broker *b = t_broker_create("n0");
    t_broker_start(b);
    t_server_config cfg;
    t_server_config_init(&cfg);
    cfg.port = 0;
    cfg.idle_timeout_ms = 0;
    t_server *srv = t_server_create(loop, b, &cfg);
    t_server_start(srv);
    uint16_t port = t_server_port(srv);

    t_thread th;
    T_ASSERT_EQ(t_thread_spawn(&th, loop_runner, loop), 0);
    t_time_sleep_us(20000);

    t_client *c = t_client_create("ad");
    T_ASSERT_EQ(t_client_dial(c, loop, "127.0.0.1", port), 0);
    unsigned seq = t_client_ack_seq(c);
    T_ASSERT_EQ(t_client_open_queue(c, "tmp.q",
                                    T_CLIENT_OPEN_PRODUCER | T_CLIENT_QFLAG_AUTODELETE), 0);
    T_ASSERT(wait_ack_status(c, seq, 0, 500));
    t_domain *d = t_broker_get_domain(b, "default");
    T_ASSERT_NOT_NULL(t_domain_get_queue(d, "tmp.q"));
    seq = t_client_ack_seq(c);
    T_ASSERT_EQ(t_client_close_queue(c, "tmp.q"), 0);
    T_ASSERT(wait_ack_status(c, seq, 0, 500));
    T_ASSERT_NULL(t_domain_get_queue(d, "tmp.q"));

    t_evloop_stop(loop);
    T_ASSERT_EQ(t_thread_join(&th), 0);
    t_client_destroy(c);
    t_server_destroy(srv);
    t_broker_destroy(b);
    t_evloop_destroy(loop);
}

T_TEST(client_parse_leader_hint) {
    char host[64];
    uint16_t port = 0;
    T_ASSERT_EQ(t_client_parse_leader_hint("127.0.0.1_9999", host, sizeof(host), &port), 0);
    T_ASSERT_STR_EQ(host, "127.0.0.1");
    T_ASSERT_EQ((int)port, 9999);
    T_ASSERT_EQ(t_client_parse_leader_hint("jobs", host, sizeof(host), &port), -1);
    T_ASSERT_EQ(t_client_parse_leader_hint("127.0.0.1_", host, sizeof(host), &port), -1);
    T_ASSERT_EQ(t_client_parse_leader_hint("_9999", host, sizeof(host), &port), -1);
    T_ASSERT_EQ(t_client_parse_leader_hint("127.0.0.1_0", host, sizeof(host), &port), -1);
    T_ASSERT_EQ(t_client_parse_leader_hint("127.0.0.1_65536", host, sizeof(host), &port), -1);
    T_ASSERT_EQ(t_client_parse_leader_hint(NULL, host, sizeof(host), &port), -1);
}

T_TEST(server_follower_open_redirect_hint) {
    t_evloop *loop = t_evloop_create();
    t_broker *b = t_broker_create("n0");
    t_cluster *cl = t_cluster_create(1);
    T_ASSERT_EQ(t_cluster_add_node(cl, 1, "127.0.0.1", 4222), 0);
    T_ASSERT_EQ(t_cluster_add_node(cl, 2, "127.0.0.1", 9999), 0);
    T_ASSERT_EQ(t_node_set_client_port(t_cluster_get_node(cl, 2), 5555), 0);
    T_ASSERT_EQ(t_cluster_set_leader(cl, 2), 0);
    T_ASSERT_EQ(t_broker_set_cluster(b, cl), 0);
    t_broker_start(b);
    t_server_config cfg;
    t_server_config_init(&cfg);
    cfg.port = 0;
    cfg.idle_timeout_ms = 0;
    t_server *srv = t_server_create(loop, b, &cfg);
    t_server_start(srv);
    uint16_t port = t_server_port(srv);

    t_thread th;
    T_ASSERT_EQ(t_thread_spawn(&th, loop_runner, loop), 0);
    t_time_sleep_us(20000);

    t_client *c = t_client_create("c");
    T_ASSERT_EQ(t_client_dial(c, loop, "127.0.0.1", port), 0);
    unsigned seq = t_client_ack_seq(c);
    T_ASSERT_EQ(t_client_open_queue(c, "jobs", T_CLIENT_OPEN_PRODUCER), 0);
    T_ASSERT(wait_ack_status(c, seq, (int)T_ERR_AGAIN, 500));
    T_ASSERT_STR_EQ(t_client_last_ack_name(c), "127.0.0.1_5555");
    char host[64];
    uint16_t hint = 0;
    T_ASSERT_EQ(t_client_leader_hint(c, host, sizeof(host), &hint), 0);
    T_ASSERT_STR_EQ(host, "127.0.0.1");
    T_ASSERT_EQ((int)hint, 5555);

    t_evloop_stop(loop);
    T_ASSERT_EQ(t_thread_join(&th), 0);
    t_client_destroy(c);
    t_server_destroy(srv);
    t_broker_destroy(b);
    t_cluster_destroy(cl);
    t_evloop_destroy(loop);
}

T_TEST(server_follower_no_client_port_no_hint) {
    t_evloop *loop = t_evloop_create();
    t_broker *b = t_broker_create("n0");
    t_cluster *cl = t_cluster_create(1);
    T_ASSERT_EQ(t_cluster_add_node(cl, 1, "127.0.0.1", 4222), 0);
    T_ASSERT_EQ(t_cluster_add_node(cl, 2, "127.0.0.1", 9999), 0);
    T_ASSERT_EQ(t_cluster_set_leader(cl, 2), 0);
    T_ASSERT_EQ(t_broker_set_cluster(b, cl), 0);
    t_broker_start(b);
    t_server_config cfg;
    t_server_config_init(&cfg);
    cfg.port = 0;
    cfg.idle_timeout_ms = 0;
    t_server *srv = t_server_create(loop, b, &cfg);
    t_server_start(srv);
    uint16_t port = t_server_port(srv);

    t_thread th;
    T_ASSERT_EQ(t_thread_spawn(&th, loop_runner, loop), 0);
    t_time_sleep_us(20000);

    t_client *c = t_client_create("c");
    T_ASSERT_EQ(t_client_dial(c, loop, "127.0.0.1", port), 0);
    unsigned seq = t_client_ack_seq(c);
    T_ASSERT_EQ(t_client_open_queue(c, "jobs", T_CLIENT_OPEN_PRODUCER), 0);
    T_ASSERT(wait_ack_status(c, seq, (int)T_ERR_AGAIN, 500));
    T_ASSERT_STR_EQ(t_client_last_ack_name(c), "");
    char host[64];
    uint16_t hint = 99;
    T_ASSERT_EQ(t_client_leader_hint(c, host, sizeof(host), &hint), -1);
    T_ASSERT_EQ(t_client_redial_leader(c), -1);
    T_ASSERT_EQ(t_client_is_connected(c), 1);

    t_evloop_stop(loop);
    T_ASSERT_EQ(t_thread_join(&th), 0);
    t_client_destroy(c);
    t_server_destroy(srv);
    t_broker_destroy(b);
    t_cluster_destroy(cl);
    t_evloop_destroy(loop);
}

T_TEST(client_redial_leader_then_post) {
    t_evloop *loop = t_evloop_create();

    t_broker *lb = t_broker_create("lead");
    t_cluster *lcl = t_cluster_create(1);
    T_ASSERT_EQ(t_cluster_add_node(lcl, 1, "127.0.0.1", 1), 0);
    T_ASSERT_EQ(t_cluster_add_node(lcl, 2, "127.0.0.1", 2), 0);
    T_ASSERT_EQ(t_cluster_set_leader(lcl, 1), 0);
    T_ASSERT_EQ(t_broker_set_cluster(lb, lcl), 0);
    t_broker_start(lb);
    t_server_config lcfg;
    t_server_config_init(&lcfg);
    lcfg.port = 0;
    lcfg.idle_timeout_ms = 0;
    t_server *ls = t_server_create(loop, lb, &lcfg);
    t_server_start(ls);
    uint16_t lport = t_server_port(ls);
    T_ASSERT_EQ(t_node_set_client_port(t_cluster_get_node(lcl, 1), lport), 0);

    t_broker *fb = t_broker_create("foll");
    t_cluster *fcl = t_cluster_create(2);
    T_ASSERT_EQ(t_cluster_add_node(fcl, 1, "127.0.0.1", 1), 0);
    T_ASSERT_EQ(t_cluster_add_node(fcl, 2, "127.0.0.1", 2), 0);
    T_ASSERT_EQ(t_node_set_client_port(t_cluster_get_node(fcl, 1), lport), 0);
    T_ASSERT_EQ(t_cluster_set_leader(fcl, 1), 0);
    T_ASSERT_EQ(t_broker_set_cluster(fb, fcl), 0);
    t_broker_start(fb);
    t_server_config fcfg;
    t_server_config_init(&fcfg);
    fcfg.port = 0;
    fcfg.idle_timeout_ms = 0;
    t_server *fs = t_server_create(loop, fb, &fcfg);
    t_server_start(fs);
    uint16_t fport = t_server_port(fs);

    t_thread th;
    T_ASSERT_EQ(t_thread_spawn(&th, loop_runner, loop), 0);
    t_time_sleep_us(20000);

    t_client *c = t_client_create("c");
    T_ASSERT_EQ(t_client_dial(c, loop, "127.0.0.1", fport), 0);
    unsigned seq = t_client_ack_seq(c);
    T_ASSERT_EQ(t_client_open_queue(c, "jobs", T_CLIENT_OPEN_PRODUCER), 0);
    T_ASSERT(wait_ack_status(c, seq, (int)T_ERR_AGAIN, 500));
    T_ASSERT_EQ(t_client_redial_leader(c), 0);
    seq = t_client_ack_seq(c);
    T_ASSERT_EQ(t_client_open_queue(c, "jobs", T_CLIENT_OPEN_PRODUCER), 0);
    T_ASSERT(wait_ack_status(c, seq, 0, 500));
    seq = t_client_ack_seq(c);
    T_ASSERT_EQ(t_client_post(c, "jobs", (const uint8_t *)"x", 1, 0), 0);
    T_ASSERT(wait_ack_status(c, seq, 0, 500));

    t_evloop_stop(loop);
    T_ASSERT_EQ(t_thread_join(&th), 0);
    t_client_destroy(c);
    t_server_destroy(fs);
    t_server_destroy(ls);
    t_broker_destroy(fb);
    t_broker_destroy(lb);
    t_cluster_destroy(fcl);
    t_cluster_destroy(lcl);
    t_evloop_destroy(loop);
}

T_TEST(client_wait_ack_timeout) {
    t_client *c = t_client_create("c");
    T_ASSERT_EQ(t_client_wait_ack(NULL, 0, 10), -1);
    T_ASSERT_EQ(t_client_wait_ack(c, 0, -1), -1);
    T_ASSERT_EQ(t_client_wait_ack(c, 0, 15), -1);
    t_client_destroy(c);
}

T_TEST(client_heartbeat_stub_fails) {
    t_client *c = t_client_create("stub");
    T_ASSERT_EQ(t_client_connect(c, "127.0.0.1", 1), 0);
    T_ASSERT_EQ(t_client_heartbeat(c), -1);
    T_ASSERT_EQ(t_client_set_heartbeat(c, -1), -1);
    T_ASSERT_EQ(t_client_set_heartbeat(c, 0), 0);
    t_client_destroy(c);
}

T_TEST(server_idle_timeout_disconnects) {
    t_evloop *loop = t_evloop_create();
    t_broker *b = t_broker_create("n0");
    t_broker_start(b);
    t_server_config cfg;
    t_server_config_init(&cfg);
    cfg.port = 0;
    cfg.idle_timeout_ms = 80;
    t_server *srv = t_server_create(loop, b, &cfg);
    t_server_start(srv);
    uint16_t port = t_server_port(srv);

    t_thread th;
    T_ASSERT_EQ(t_thread_spawn(&th, loop_runner, loop), 0);
    t_time_sleep_us(20000);

    t_client *c = t_client_create("c");
    T_ASSERT_EQ(t_client_set_heartbeat(c, 0), 0);
    T_ASSERT_EQ(t_client_dial(c, loop, "127.0.0.1", port), 0);
    {
        int64_t start = t_time_now_ms();
        while (t_client_is_connected(c) && t_time_now_ms() - start < 400)
            t_time_sleep_ms(5);
    }
    T_ASSERT_EQ(t_client_is_connected(c), 0);

    t_evloop_stop(loop);
    T_ASSERT_EQ(t_thread_join(&th), 0);
    t_client_destroy(c);
    t_server_destroy(srv);
    t_broker_destroy(b);
    t_evloop_destroy(loop);
}

T_TEST(client_reopen_after_idle_drop) {
    t_evloop *loop = t_evloop_create();
    t_broker *b = t_broker_create("n0");
    t_broker_start(b);
    t_server_config cfg;
    t_server_config_init(&cfg);
    cfg.port = 0;
    cfg.idle_timeout_ms = 80;
    t_server *srv = t_server_create(loop, b, &cfg);
    t_server_start(srv);
    uint16_t port = t_server_port(srv);

    t_thread th;
    T_ASSERT_EQ(t_thread_spawn(&th, loop_runner, loop), 0);
    t_time_sleep_us(20000);

    t_client *cons = t_client_create("c");
    t_client *prod = t_client_create("p");
    T_ASSERT_EQ(t_client_set_heartbeat(cons, 0), 0);
    T_ASSERT_EQ(t_client_set_heartbeat(prod, 25), 0);
    T_ASSERT_EQ(t_client_dial(cons, loop, "127.0.0.1", port), 0);
    T_ASSERT_EQ(t_client_dial(prod, loop, "127.0.0.1", port), 0);
    g_got = 0;
    T_ASSERT_EQ(t_client_subscribe_follow(cons, "jobs", on_net_msg, NULL, 0, 500), 0);
    {
        int64_t start = t_time_now_ms();
        while (t_client_is_connected(cons) && t_time_now_ms() - start < 400)
            t_time_sleep_ms(5);
    }
    T_ASSERT_EQ(t_client_is_connected(cons), 0);
    T_ASSERT_EQ(t_client_dial(cons, loop, "127.0.0.1", port), 0);
    T_ASSERT_EQ(t_client_open_follow(cons, "jobs", T_CLIENT_OPEN_CONSUMER, 500), 0);
    T_ASSERT_EQ(t_client_post_follow(prod, "jobs", (const uint8_t *)"hi", 2, 0, 500), 0);
    T_ASSERT(wait_flag_ge(&g_got, 1, 500));
    T_ASSERT_EQ(g_got, 1);

    t_evloop_stop(loop);
    T_ASSERT_EQ(t_thread_join(&th), 0);
    t_client_destroy(cons);
    t_client_destroy(prod);
    t_server_destroy(srv);
    t_broker_destroy(b);
    t_evloop_destroy(loop);
}

T_TEST(client_post_after_drop_needs_reopen) {
    t_evloop *loop = t_evloop_create();
    t_broker *b = t_broker_create("n0");
    t_broker_start(b);
    t_server_config cfg;
    t_server_config_init(&cfg);
    cfg.port = 0;
    cfg.idle_timeout_ms = 80;
    t_server *srv = t_server_create(loop, b, &cfg);
    t_server_start(srv);
    uint16_t port = t_server_port(srv);

    t_thread th;
    T_ASSERT_EQ(t_thread_spawn(&th, loop_runner, loop), 0);
    t_time_sleep_us(20000);

    t_client *prod = t_client_create("p");
    T_ASSERT_EQ(t_client_set_heartbeat(prod, 0), 0);
    T_ASSERT_EQ(t_client_dial(prod, loop, "127.0.0.1", port), 0);
    T_ASSERT_EQ(t_client_open_follow(prod, "jobs", T_CLIENT_OPEN_PRODUCER, 500), 0);
    {
        int64_t start = t_time_now_ms();
        while (t_client_is_connected(prod) && t_time_now_ms() - start < 400)
            t_time_sleep_ms(5);
    }
    T_ASSERT_EQ(t_client_is_connected(prod), 0);
    T_ASSERT_EQ(t_client_dial(prod, loop, "127.0.0.1", port), 0);
    T_ASSERT_EQ(t_client_post(prod, "jobs", (const uint8_t *)"x", 1, 0), -1);
    T_ASSERT_EQ((int)t_client_total_published(prod), 0);
    T_ASSERT_EQ(t_client_open_follow(prod, "jobs", T_CLIENT_OPEN_PRODUCER, 500), 0);
    unsigned seq = t_client_ack_seq(prod);
    T_ASSERT_EQ(t_client_post(prod, "jobs", (const uint8_t *)"x", 1, 0), 0);
    T_ASSERT(wait_ack_status(prod, seq, 0, 500));
    T_ASSERT_EQ((int)t_client_total_published(prod), 1);

    t_evloop_stop(loop);
    T_ASSERT_EQ(t_thread_join(&th), 0);
    t_client_destroy(prod);
    t_server_destroy(srv);
    t_broker_destroy(b);
    t_evloop_destroy(loop);
}

T_TEST(client_close_after_drop_needs_reopen) {
    t_evloop *loop = t_evloop_create();
    t_broker *b = t_broker_create("n0");
    t_broker_start(b);
    t_server_config cfg;
    t_server_config_init(&cfg);
    cfg.port = 0;
    cfg.idle_timeout_ms = 80;
    t_server *srv = t_server_create(loop, b, &cfg);
    t_server_start(srv);
    uint16_t port = t_server_port(srv);

    t_thread th;
    T_ASSERT_EQ(t_thread_spawn(&th, loop_runner, loop), 0);
    t_time_sleep_us(20000);

    t_client *c = t_client_create("c");
    T_ASSERT_EQ(t_client_set_heartbeat(c, 0), 0);
    T_ASSERT_EQ(t_client_dial(c, loop, "127.0.0.1", port), 0);
    T_ASSERT_EQ(t_client_open_follow(c, "jobs", T_CLIENT_OPEN_PRODUCER, 500), 0);
    {
        int64_t start = t_time_now_ms();
        while (t_client_is_connected(c) && t_time_now_ms() - start < 400)
            t_time_sleep_ms(5);
    }
    T_ASSERT_EQ(t_client_is_connected(c), 0);
    T_ASSERT_EQ(t_client_dial(c, loop, "127.0.0.1", port), 0);
    T_ASSERT_EQ(t_client_close_queue(c, "jobs"), -1);
    T_ASSERT_EQ((int)t_client_queue_count(c), 1);
    T_ASSERT_EQ(t_client_close_follow(c, "jobs", 500), 0);
    T_ASSERT_EQ((int)t_client_queue_count(c), 0);

    t_evloop_stop(loop);
    T_ASSERT_EQ(t_thread_join(&th), 0);
    t_client_destroy(c);
    t_server_destroy(srv);
    t_broker_destroy(b);
    t_evloop_destroy(loop);
}

T_TEST(client_heartbeat_keeps_idle) {
    t_evloop *loop = t_evloop_create();
    t_broker *b = t_broker_create("n0");
    t_broker_start(b);
    t_server_config cfg;
    t_server_config_init(&cfg);
    cfg.port = 0;
    cfg.idle_timeout_ms = 80;
    t_server *srv = t_server_create(loop, b, &cfg);
    t_server_start(srv);
    uint16_t port = t_server_port(srv);

    t_thread th;
    T_ASSERT_EQ(t_thread_spawn(&th, loop_runner, loop), 0);
    t_time_sleep_us(20000);

    t_client *c = t_client_create("c");
    T_ASSERT_EQ(t_client_set_heartbeat(c, 25), 0);
    T_ASSERT_EQ(t_client_dial(c, loop, "127.0.0.1", port), 0);
    t_time_sleep_ms(220);
    T_ASSERT_EQ(t_client_is_connected(c), 1);

    t_evloop_stop(loop);
    T_ASSERT_EQ(t_thread_join(&th), 0);
    t_client_destroy(c);
    t_server_destroy(srv);
    t_broker_destroy(b);
    t_evloop_destroy(loop);
}

T_TEST(client_heartbeat_does_not_advance_ack_seq) {
    t_evloop *loop = t_evloop_create();
    t_broker *b = t_broker_create("n0");
    t_broker_start(b);
    t_server_config cfg;
    t_server_config_init(&cfg);
    cfg.port = 0;
    cfg.idle_timeout_ms = 0;
    t_server *srv = t_server_create(loop, b, &cfg);
    t_server_start(srv);
    uint16_t port = t_server_port(srv);

    t_thread th;
    T_ASSERT_EQ(t_thread_spawn(&th, loop_runner, loop), 0);
    t_time_sleep_us(20000);

    t_client *c = t_client_create("c");
    T_ASSERT_EQ(t_client_set_heartbeat(c, 0), 0);
    T_ASSERT_EQ(t_client_dial(c, loop, "127.0.0.1", port), 0);
    T_ASSERT_EQ((int)t_client_ack_seq(c), 0);
    T_ASSERT_EQ(t_client_heartbeat(c), 0);
    t_time_sleep_ms(50);
    T_ASSERT_EQ((int)t_client_ack_seq(c), 0);
    T_ASSERT_EQ(t_client_open_follow(c, "jobs", T_CLIENT_OPEN_PRODUCER, 500), 0);
    T_ASSERT((int)t_client_ack_seq(c) > 0);

    t_evloop_stop(loop);
    T_ASSERT_EQ(t_thread_join(&th), 0);
    t_client_destroy(c);
    t_server_destroy(srv);
    t_broker_destroy(b);
    t_evloop_destroy(loop);
}

T_TEST(client_open_follow_to_leader) {
    t_evloop *loop = t_evloop_create();

    t_broker *lb = t_broker_create("lead");
    t_cluster *lcl = t_cluster_create(1);
    T_ASSERT_EQ(t_cluster_add_node(lcl, 1, "127.0.0.1", 1), 0);
    T_ASSERT_EQ(t_cluster_add_node(lcl, 2, "127.0.0.1", 2), 0);
    T_ASSERT_EQ(t_cluster_set_leader(lcl, 1), 0);
    T_ASSERT_EQ(t_broker_set_cluster(lb, lcl), 0);
    t_broker_start(lb);
    t_server_config lcfg;
    t_server_config_init(&lcfg);
    lcfg.port = 0;
    lcfg.idle_timeout_ms = 0;
    t_server *ls = t_server_create(loop, lb, &lcfg);
    t_server_start(ls);
    uint16_t lport = t_server_port(ls);
    T_ASSERT_EQ(t_node_set_client_port(t_cluster_get_node(lcl, 1), lport), 0);

    t_broker *fb = t_broker_create("foll");
    t_cluster *fcl = t_cluster_create(2);
    T_ASSERT_EQ(t_cluster_add_node(fcl, 1, "127.0.0.1", 1), 0);
    T_ASSERT_EQ(t_cluster_add_node(fcl, 2, "127.0.0.1", 2), 0);
    T_ASSERT_EQ(t_node_set_client_port(t_cluster_get_node(fcl, 1), lport), 0);
    T_ASSERT_EQ(t_cluster_set_leader(fcl, 1), 0);
    T_ASSERT_EQ(t_broker_set_cluster(fb, fcl), 0);
    t_broker_start(fb);
    t_server_config fcfg;
    t_server_config_init(&fcfg);
    fcfg.port = 0;
    fcfg.idle_timeout_ms = 0;
    t_server *fs = t_server_create(loop, fb, &fcfg);
    t_server_start(fs);
    uint16_t fport = t_server_port(fs);

    t_thread th;
    T_ASSERT_EQ(t_thread_spawn(&th, loop_runner, loop), 0);
    t_time_sleep_us(20000);

    t_client *c = t_client_create("c");
    T_ASSERT_EQ(t_client_dial(c, loop, "127.0.0.1", fport), 0);
    T_ASSERT_EQ(t_client_open_follow(c, "jobs", T_CLIENT_OPEN_PRODUCER, 500), 0);
    unsigned seq = t_client_ack_seq(c);
    T_ASSERT_EQ(t_client_post(c, "jobs", (const uint8_t *)"x", 1, 0), 0);
    T_ASSERT(wait_ack_status(c, seq, 0, 500));

    t_evloop_stop(loop);
    T_ASSERT_EQ(t_thread_join(&th), 0);
    t_client_destroy(c);
    t_server_destroy(fs);
    t_server_destroy(ls);
    t_broker_destroy(fb);
    t_broker_destroy(lb);
    t_cluster_destroy(fcl);
    t_cluster_destroy(lcl);
    t_evloop_destroy(loop);
}

T_TEST(client_open_follow_no_hint_stays) {
    t_evloop *loop = t_evloop_create();
    t_broker *b = t_broker_create("n0");
    t_cluster *cl = t_cluster_create(1);
    T_ASSERT_EQ(t_cluster_add_node(cl, 1, "127.0.0.1", 4222), 0);
    T_ASSERT_EQ(t_cluster_add_node(cl, 2, "127.0.0.1", 9999), 0);
    T_ASSERT_EQ(t_cluster_set_leader(cl, 2), 0);
    T_ASSERT_EQ(t_broker_set_cluster(b, cl), 0);
    t_broker_start(b);
    t_server_config cfg;
    t_server_config_init(&cfg);
    cfg.port = 0;
    cfg.idle_timeout_ms = 0;
    t_server *srv = t_server_create(loop, b, &cfg);
    t_server_start(srv);
    uint16_t port = t_server_port(srv);

    t_thread th;
    T_ASSERT_EQ(t_thread_spawn(&th, loop_runner, loop), 0);
    t_time_sleep_us(20000);

    t_client *c = t_client_create("c");
    T_ASSERT_EQ(t_client_dial(c, loop, "127.0.0.1", port), 0);
    T_ASSERT_EQ(t_client_open_follow(c, "jobs", T_CLIENT_OPEN_PRODUCER, 500), -1);
    T_ASSERT_EQ(t_client_is_connected(c), 1);
    T_ASSERT_EQ(t_client_redial_leader(c), -1);

    t_evloop_stop(loop);
    T_ASSERT_EQ(t_thread_join(&th), 0);
    t_client_destroy(c);
    t_server_destroy(srv);
    t_broker_destroy(b);
    t_cluster_destroy(cl);
    t_evloop_destroy(loop);
}

T_TEST(client_open_follow_same_peer_no_bounce) {
    t_evloop *loop = t_evloop_create();
    t_broker *b = t_broker_create("n0");
    t_cluster *cl = t_cluster_create(1);
    T_ASSERT_EQ(t_cluster_add_node(cl, 1, "127.0.0.1", 4222), 0);
    T_ASSERT_EQ(t_cluster_add_node(cl, 2, "127.0.0.1", 9999), 0);
    T_ASSERT_EQ(t_cluster_set_leader(cl, 2), 0);
    T_ASSERT_EQ(t_broker_set_cluster(b, cl), 0);
    t_broker_start(b);
    t_server_config cfg;
    t_server_config_init(&cfg);
    cfg.port = 0;
    cfg.idle_timeout_ms = 0;
    t_server *srv = t_server_create(loop, b, &cfg);
    t_server_start(srv);
    uint16_t port = t_server_port(srv);
    T_ASSERT_EQ(t_node_set_client_port(t_cluster_get_node(cl, 2), port), 0);

    t_thread th;
    T_ASSERT_EQ(t_thread_spawn(&th, loop_runner, loop), 0);
    t_time_sleep_us(20000);

    t_client *c = t_client_create("c");
    T_ASSERT_EQ(t_client_dial(c, loop, "127.0.0.1", port), 0);
    T_ASSERT_EQ(t_client_open_follow(c, "jobs", T_CLIENT_OPEN_PRODUCER, 500), -1);
    T_ASSERT_EQ(t_client_is_connected(c), 1);
    T_ASSERT_EQ(t_client_redial_leader(c), -1);

    t_evloop_stop(loop);
    T_ASSERT_EQ(t_thread_join(&th), 0);
    t_client_destroy(c);
    t_server_destroy(srv);
    t_broker_destroy(b);
    t_cluster_destroy(cl);
    t_evloop_destroy(loop);
}

T_TEST(client_open_follow_idempotent) {
    t_evloop *loop = t_evloop_create();
    t_broker *b = t_broker_create("n0");
    t_broker_start(b);
    t_server_config cfg;
    t_server_config_init(&cfg);
    cfg.port = 0;
    cfg.idle_timeout_ms = 0;
    t_server *srv = t_server_create(loop, b, &cfg);
    t_server_start(srv);
    uint16_t port = t_server_port(srv);

    t_thread th;
    T_ASSERT_EQ(t_thread_spawn(&th, loop_runner, loop), 0);
    t_time_sleep_us(20000);

    t_client *c = t_client_create("c");
    T_ASSERT_EQ(t_client_dial(c, loop, "127.0.0.1", port), 0);
    T_ASSERT_EQ(t_client_open_follow(c, "jobs", T_CLIENT_OPEN_PRODUCER, 500), 0);
    int64_t t0 = t_time_now_ms();
    T_ASSERT_EQ(t_client_open_follow(c, "jobs", T_CLIENT_OPEN_PRODUCER, 500), 0);
    T_ASSERT(t_time_now_ms() - t0 < 80);

    t_evloop_stop(loop);
    T_ASSERT_EQ(t_thread_join(&th), 0);
    t_client_destroy(c);
    t_server_destroy(srv);
    t_broker_destroy(b);
    t_evloop_destroy(loop);
}

T_TEST(client_join_follow_to_leader) {
    t_evloop *loop = t_evloop_create();

    t_broker *lb = t_broker_create("lead");
    t_cluster *lcl = t_cluster_create(1);
    T_ASSERT_EQ(t_cluster_add_node(lcl, 1, "127.0.0.1", 1), 0);
    T_ASSERT_EQ(t_cluster_add_node(lcl, 2, "127.0.0.1", 2), 0);
    T_ASSERT_EQ(t_cluster_set_leader(lcl, 1), 0);
    T_ASSERT_EQ(t_broker_set_cluster(lb, lcl), 0);
    t_broker_start(lb);
    t_server_config lcfg;
    t_server_config_init(&lcfg);
    lcfg.port = 0;
    lcfg.idle_timeout_ms = 0;
    t_server *ls = t_server_create(loop, lb, &lcfg);
    t_server_start(ls);
    uint16_t lport = t_server_port(ls);
    T_ASSERT_EQ(t_node_set_client_port(t_cluster_get_node(lcl, 1), lport), 0);

    t_broker *fb = t_broker_create("foll");
    t_cluster *fcl = t_cluster_create(2);
    T_ASSERT_EQ(t_cluster_add_node(fcl, 1, "127.0.0.1", 1), 0);
    T_ASSERT_EQ(t_cluster_add_node(fcl, 2, "127.0.0.1", 2), 0);
    T_ASSERT_EQ(t_node_set_client_port(t_cluster_get_node(fcl, 1), lport), 0);
    T_ASSERT_EQ(t_cluster_set_leader(fcl, 1), 0);
    T_ASSERT_EQ(t_broker_set_cluster(fb, fcl), 0);
    t_broker_start(fb);
    t_server_config fcfg;
    t_server_config_init(&fcfg);
    fcfg.port = 0;
    fcfg.idle_timeout_ms = 0;
    t_server *fs = t_server_create(loop, fb, &fcfg);
    t_server_start(fs);
    uint16_t fport = t_server_port(fs);

    t_thread th;
    T_ASSERT_EQ(t_thread_spawn(&th, loop_runner, loop), 0);
    t_time_sleep_us(20000);

    t_client *cons = t_client_create("cons");
    T_ASSERT_EQ(t_client_dial(cons, loop, "127.0.0.1", fport), 0);
    T_ASSERT_EQ(t_client_join_follow(cons, "workers", "c1", "jobs", 500), 0);
    T_ASSERT_EQ(t_client_subscribe(cons, "jobs", on_net_msg, NULL), 0);

    t_client *prod = t_client_create("prod");
    T_ASSERT_EQ(t_client_dial(prod, loop, "127.0.0.1", lport), 0);
    T_ASSERT_EQ(t_client_open_follow(prod, "jobs", T_CLIENT_OPEN_PRODUCER, 500), 0);
    g_got = 0;
    T_ASSERT_EQ(t_client_post(prod, "jobs", (const uint8_t *)"x", 1, 0), 0);
    T_ASSERT(wait_flag_ge(&g_got, 1, 500));

    t_evloop_stop(loop);
    T_ASSERT_EQ(t_thread_join(&th), 0);
    t_client_destroy(cons);
    t_client_destroy(prod);
    t_server_destroy(fs);
    t_server_destroy(ls);
    t_broker_destroy(fb);
    t_broker_destroy(lb);
    t_cluster_destroy(fcl);
    t_cluster_destroy(lcl);
    t_evloop_destroy(loop);
}

T_TEST(client_join_follow_no_hint_stays) {
    t_evloop *loop = t_evloop_create();
    t_broker *b = t_broker_create("n0");
    t_cluster *cl = t_cluster_create(1);
    T_ASSERT_EQ(t_cluster_add_node(cl, 1, "127.0.0.1", 4222), 0);
    T_ASSERT_EQ(t_cluster_add_node(cl, 2, "127.0.0.1", 9999), 0);
    T_ASSERT_EQ(t_cluster_set_leader(cl, 2), 0);
    T_ASSERT_EQ(t_broker_set_cluster(b, cl), 0);
    t_broker_start(b);
    t_server_config cfg;
    t_server_config_init(&cfg);
    cfg.port = 0;
    cfg.idle_timeout_ms = 0;
    t_server *srv = t_server_create(loop, b, &cfg);
    t_server_start(srv);
    uint16_t port = t_server_port(srv);

    t_thread th;
    T_ASSERT_EQ(t_thread_spawn(&th, loop_runner, loop), 0);
    t_time_sleep_us(20000);

    t_client *c = t_client_create("c");
    T_ASSERT_EQ(t_client_dial(c, loop, "127.0.0.1", port), 0);
    T_ASSERT_EQ(t_client_join_follow(c, "workers", "c1", "jobs", 500), -1);
    T_ASSERT_EQ(t_client_is_connected(c), 1);
    T_ASSERT_EQ(t_client_redial_leader(c), -1);

    t_evloop_stop(loop);
    T_ASSERT_EQ(t_thread_join(&th), 0);
    t_client_destroy(c);
    t_server_destroy(srv);
    t_broker_destroy(b);
    t_cluster_destroy(cl);
    t_evloop_destroy(loop);
}

T_TEST(client_post_follow_to_leader) {
    t_evloop *loop = t_evloop_create();

    t_broker *lb = t_broker_create("lead");
    t_cluster *lcl = t_cluster_create(1);
    T_ASSERT_EQ(t_cluster_add_node(lcl, 1, "127.0.0.1", 1), 0);
    T_ASSERT_EQ(t_cluster_add_node(lcl, 2, "127.0.0.1", 2), 0);
    T_ASSERT_EQ(t_cluster_set_leader(lcl, 1), 0);
    T_ASSERT_EQ(t_broker_set_cluster(lb, lcl), 0);
    t_broker_start(lb);
    t_server_config lcfg;
    t_server_config_init(&lcfg);
    lcfg.port = 0;
    lcfg.idle_timeout_ms = 0;
    t_server *ls = t_server_create(loop, lb, &lcfg);
    t_server_start(ls);
    uint16_t lport = t_server_port(ls);
    T_ASSERT_EQ(t_node_set_client_port(t_cluster_get_node(lcl, 1), lport), 0);

    t_broker *fb = t_broker_create("foll");
    t_cluster *fcl = t_cluster_create(2);
    T_ASSERT_EQ(t_cluster_add_node(fcl, 1, "127.0.0.1", 1), 0);
    T_ASSERT_EQ(t_cluster_add_node(fcl, 2, "127.0.0.1", 2), 0);
    T_ASSERT_EQ(t_node_set_client_port(t_cluster_get_node(fcl, 1), lport), 0);
    T_ASSERT_EQ(t_cluster_set_leader(fcl, 1), 0);
    T_ASSERT_EQ(t_broker_set_cluster(fb, fcl), 0);
    t_broker_start(fb);
    t_server_config fcfg;
    t_server_config_init(&fcfg);
    fcfg.port = 0;
    fcfg.idle_timeout_ms = 0;
    t_server *fs = t_server_create(loop, fb, &fcfg);
    t_server_start(fs);
    uint16_t fport = t_server_port(fs);

    t_thread th;
    T_ASSERT_EQ(t_thread_spawn(&th, loop_runner, loop), 0);
    t_time_sleep_us(20000);

    t_client *c = t_client_create("c");
    T_ASSERT_EQ(t_client_dial(c, loop, "127.0.0.1", fport), 0);
    T_ASSERT_EQ(t_client_post_follow(c, "jobs", (const uint8_t *)"x", 1, 0, 500), 0);
    t_domain *d = t_broker_get_domain(lb, "default");
    t_queue *q = (t_queue *)t_domain_get_queue(d, "jobs");
    T_ASSERT_NOT_NULL(q);
    T_ASSERT_EQ((int)t_queue_pending_count(q), 1);

    t_evloop_stop(loop);
    T_ASSERT_EQ(t_thread_join(&th), 0);
    t_client_destroy(c);
    t_server_destroy(fs);
    t_server_destroy(ls);
    t_broker_destroy(fb);
    t_broker_destroy(lb);
    t_cluster_destroy(fcl);
    t_cluster_destroy(lcl);
    t_evloop_destroy(loop);
}

T_TEST(client_post_follow_same_peer_no_bounce) {
    t_evloop *loop = t_evloop_create();
    t_broker *b = t_broker_create("n0");
    t_cluster *cl = t_cluster_create(1);
    T_ASSERT_EQ(t_cluster_add_node(cl, 1, "127.0.0.1", 4222), 0);
    T_ASSERT_EQ(t_cluster_add_node(cl, 2, "127.0.0.1", 9999), 0);
    T_ASSERT_EQ(t_cluster_set_leader(cl, 2), 0);
    T_ASSERT_EQ(t_broker_set_cluster(b, cl), 0);
    t_broker_start(b);
    t_server_config cfg;
    t_server_config_init(&cfg);
    cfg.port = 0;
    cfg.idle_timeout_ms = 0;
    t_server *srv = t_server_create(loop, b, &cfg);
    t_server_start(srv);
    uint16_t port = t_server_port(srv);
    T_ASSERT_EQ(t_node_set_client_port(t_cluster_get_node(cl, 2), port), 0);

    t_thread th;
    T_ASSERT_EQ(t_thread_spawn(&th, loop_runner, loop), 0);
    t_time_sleep_us(20000);

    t_client *c = t_client_create("c");
    T_ASSERT_EQ(t_client_dial(c, loop, "127.0.0.1", port), 0);
    T_ASSERT_EQ(t_client_post_follow(c, "jobs", (const uint8_t *)"x", 1, 0, 500), -1);
    T_ASSERT_EQ(t_client_is_connected(c), 1);
    T_ASSERT_EQ(t_client_redial_leader(c), -1);

    t_evloop_stop(loop);
    T_ASSERT_EQ(t_thread_join(&th), 0);
    t_client_destroy(c);
    t_server_destroy(srv);
    t_broker_destroy(b);
    t_cluster_destroy(cl);
    t_evloop_destroy(loop);
}

static int raft_noop_repl(t_raft *r, uint64_t index, void *ud) {
    (void)r;
    (void)index;
    (void)ud;
    return 0;
}

typedef struct {
    t_evloop *loop;
    t_raft   *r;
    size_t    n;
    int       armed;
} raft_late_ctx;

static void raft_late_commit(void *ud) {
    raft_late_ctx *cx = (raft_late_ctx *)ud;
    uint64_t match = t_raft_last_log_index(cx->r);
    uint64_t matches[1];
    matches[0] = match;
    (void)t_raft_majority_commit(cx->r, matches, 1, cx->n);
}

static int raft_late_repl(t_raft *r, uint64_t index, void *ud) {
    raft_late_ctx *cx = (raft_late_ctx *)ud;
    (void)r;
    (void)index;
    if (cx->armed) return 0;
    cx->armed = 1;
    if (t_evloop_timer_add(cx->loop, 15, 0, raft_late_commit, cx) < 0)
        return -1;
    return 0;
}

T_TEST(client_raft_post_wait_expires) {
    t_evloop *loop = t_evloop_create();
    t_broker *b = t_broker_create("n0");
    t_cluster *cl = t_cluster_create(1);
    T_ASSERT_EQ(t_cluster_add_node(cl, 1, "127.0.0.1", 1), 0);
    T_ASSERT_EQ(t_cluster_add_node(cl, 2, "127.0.0.1", 2), 0);
    T_ASSERT_EQ(t_cluster_set_leader(cl, 1), 0);
    t_raft_config rcfg = {1, 40, 15};
    t_raft *r = t_raft_create(&rcfg);
    T_ASSERT_EQ(t_raft_become_candidate(r), 0);
    T_ASSERT_EQ(t_raft_become_leader(r), 0);
    t_raft_set_replicate_cb(r, raft_noop_repl, NULL);
    T_ASSERT_EQ(t_broker_set_cluster(b, cl), 0);
    T_ASSERT_EQ(t_broker_start(b), 0);
    T_ASSERT_EQ(t_broker_create_queue(b, "default", "jobs", 0, 0), 0);
    T_ASSERT_EQ(t_broker_set_raft(b, r), 0);

    t_server_config cfg;
    t_server_config_init(&cfg);
    cfg.port = 0;
    cfg.idle_timeout_ms = 0;
    t_server *srv = t_server_create(loop, b, &cfg);
    t_server_start(srv);
    uint16_t port = t_server_port(srv);

    t_thread th;
    T_ASSERT_EQ(t_thread_spawn(&th, loop_runner, loop), 0);
    t_time_sleep_us(20000);

    t_client *c = t_client_create("c");
    T_ASSERT_EQ(t_client_dial(c, loop, "127.0.0.1", port), 0);
    T_ASSERT_EQ(t_client_open_follow(c, "jobs", T_CLIENT_OPEN_PRODUCER, 500), 0);
    unsigned seq = t_client_ack_seq(c);
    T_ASSERT_EQ(t_client_post(c, "jobs", (const uint8_t *)"x", 1, 0), 0);
    T_ASSERT(!wait_next_ack(c, seq, 20));
    T_ASSERT(wait_ack_status(c, seq, (int)T_ERR_AGAIN, 400));
    t_queue *held = (t_queue *)t_domain_get_queue(t_broker_get_domain(b, "default"),
                                                 "jobs");
    T_ASSERT_NOT_NULL(held);
    T_ASSERT_EQ((int)t_queue_pending_count(held), 0);

    t_evloop_stop(loop);
    T_ASSERT_EQ(t_thread_join(&th), 0);
    t_client_destroy(c);
    t_server_destroy(srv);
    t_broker_destroy(b);
    t_raft_destroy(r);
    t_cluster_destroy(cl);
    t_evloop_destroy(loop);
}

T_TEST(client_raft_post_wait_acks_on_apply) {
    t_evloop *loop = t_evloop_create();
    t_broker *b = t_broker_create("n0");
    t_cluster *cl = t_cluster_create(1);
    T_ASSERT_EQ(t_cluster_add_node(cl, 1, "127.0.0.1", 1), 0);
    T_ASSERT_EQ(t_cluster_add_node(cl, 2, "127.0.0.1", 2), 0);
    T_ASSERT_EQ(t_cluster_set_leader(cl, 1), 0);
    t_raft_config rcfg = {1, 200, 50};
    t_raft *r = t_raft_create(&rcfg);
    T_ASSERT_EQ(t_raft_become_candidate(r), 0);
    T_ASSERT_EQ(t_raft_become_leader(r), 0);
    raft_late_ctx cx = {loop, r, 2, 0};
    t_raft_set_replicate_cb(r, raft_late_repl, &cx);
    T_ASSERT_EQ(t_broker_set_cluster(b, cl), 0);
    T_ASSERT_EQ(t_broker_start(b), 0);
    T_ASSERT_EQ(t_broker_create_queue(b, "default", "jobs", 0, 0), 0);
    T_ASSERT_EQ(t_broker_set_raft(b, r), 0);

    t_server_config cfg;
    t_server_config_init(&cfg);
    cfg.port = 0;
    cfg.idle_timeout_ms = 0;
    t_server *srv = t_server_create(loop, b, &cfg);
    t_server_start(srv);
    uint16_t port = t_server_port(srv);

    t_thread th;
    T_ASSERT_EQ(t_thread_spawn(&th, loop_runner, loop), 0);
    t_time_sleep_us(20000);

    t_client *c = t_client_create("c");
    T_ASSERT_EQ(t_client_dial(c, loop, "127.0.0.1", port), 0);
    T_ASSERT_EQ(t_client_open_follow(c, "jobs", T_CLIENT_OPEN_PRODUCER, 500), 0);
    unsigned seq = t_client_ack_seq(c);
    T_ASSERT_EQ(t_client_post(c, "jobs", (const uint8_t *)"x", 1, 0), 0);
    T_ASSERT(wait_ack_status(c, seq, 0, 150));
    t_queue *q = (t_queue *)t_domain_get_queue(t_broker_get_domain(b, "default"),
                                              "jobs");
    T_ASSERT_NOT_NULL(q);
    T_ASSERT_EQ((int)t_queue_pending_count(q), 1);

    t_evloop_stop(loop);
    T_ASSERT_EQ(t_thread_join(&th), 0);
    t_client_destroy(c);
    t_server_destroy(srv);
    t_broker_destroy(b);
    t_raft_destroy(r);
    t_cluster_destroy(cl);
    t_evloop_destroy(loop);
}

T_TEST(client_raft_autodelete_wait_expires) {
    t_evloop *loop = t_evloop_create();
    t_broker *b = t_broker_create("n0");
    t_cluster *cl = t_cluster_create(1);
    T_ASSERT_EQ(t_cluster_add_node(cl, 1, "127.0.0.1", 1), 0);
    T_ASSERT_EQ(t_cluster_add_node(cl, 2, "127.0.0.1", 2), 0);
    T_ASSERT_EQ(t_cluster_set_leader(cl, 1), 0);
    t_raft_config rcfg = {1, 40, 15};
    t_raft *r = t_raft_create(&rcfg);
    T_ASSERT_EQ(t_raft_become_candidate(r), 0);
    T_ASSERT_EQ(t_raft_become_leader(r), 0);
    t_raft_set_replicate_cb(r, raft_noop_repl, NULL);
    T_ASSERT_EQ(t_broker_set_cluster(b, cl), 0);
    T_ASSERT_EQ(t_broker_start(b), 0);
    T_ASSERT_EQ(t_broker_create_queue(b, "default", "tmp.q", 0,
                                      T_QUEUE_FLAG_AUTODELETE), 0);
    T_ASSERT_EQ(t_broker_set_raft(b, r), 0);

    t_server_config cfg;
    t_server_config_init(&cfg);
    cfg.port = 0;
    cfg.idle_timeout_ms = 0;
    t_server *srv = t_server_create(loop, b, &cfg);
    t_server_start(srv);
    uint16_t port = t_server_port(srv);

    t_thread th;
    T_ASSERT_EQ(t_thread_spawn(&th, loop_runner, loop), 0);
    t_time_sleep_us(20000);

    t_client *c = t_client_create("c");
    T_ASSERT_EQ(t_client_dial(c, loop, "127.0.0.1", port), 0);
    T_ASSERT_EQ(t_client_open_follow(c, "tmp.q", T_CLIENT_OPEN_PRODUCER, 500), 0);
    unsigned seq = t_client_ack_seq(c);
    T_ASSERT_EQ(t_client_close_queue(c, "tmp.q"), 0);
    T_ASSERT(!wait_next_ack(c, seq, 20));
    T_ASSERT(wait_ack_status(c, seq, (int)T_ERR_AGAIN, 400));
    T_ASSERT_NOT_NULL(t_domain_get_queue(t_broker_get_domain(b, "default"),
                                         "tmp.q"));

    t_evloop_stop(loop);
    T_ASSERT_EQ(t_thread_join(&th), 0);
    t_client_destroy(c);
    t_server_destroy(srv);
    t_broker_destroy(b);
    t_raft_destroy(r);
    t_cluster_destroy(cl);
    t_evloop_destroy(loop);
}

T_TEST(client_raft_autodelete_acks_on_apply) {
    t_evloop *loop = t_evloop_create();
    t_broker *b = t_broker_create("n0");
    t_cluster *cl = t_cluster_create(1);
    T_ASSERT_EQ(t_cluster_add_node(cl, 1, "127.0.0.1", 1), 0);
    T_ASSERT_EQ(t_cluster_add_node(cl, 2, "127.0.0.1", 2), 0);
    T_ASSERT_EQ(t_cluster_set_leader(cl, 1), 0);
    t_raft_config rcfg = {1, 200, 50};
    t_raft *r = t_raft_create(&rcfg);
    T_ASSERT_EQ(t_raft_become_candidate(r), 0);
    T_ASSERT_EQ(t_raft_become_leader(r), 0);
    raft_late_ctx cx = {loop, r, 2, 0};
    t_raft_set_replicate_cb(r, raft_late_repl, &cx);
    T_ASSERT_EQ(t_broker_set_cluster(b, cl), 0);
    T_ASSERT_EQ(t_broker_start(b), 0);
    T_ASSERT_EQ(t_broker_create_queue(b, "default", "tmp.q", 0,
                                      T_QUEUE_FLAG_AUTODELETE), 0);
    T_ASSERT_EQ(t_broker_set_raft(b, r), 0);

    t_server_config cfg;
    t_server_config_init(&cfg);
    cfg.port = 0;
    cfg.idle_timeout_ms = 0;
    t_server *srv = t_server_create(loop, b, &cfg);
    t_server_start(srv);
    uint16_t port = t_server_port(srv);

    t_thread th;
    T_ASSERT_EQ(t_thread_spawn(&th, loop_runner, loop), 0);
    t_time_sleep_us(20000);

    t_client *c = t_client_create("c");
    T_ASSERT_EQ(t_client_dial(c, loop, "127.0.0.1", port), 0);
    T_ASSERT_EQ(t_client_open_follow(c, "tmp.q", T_CLIENT_OPEN_PRODUCER, 500), 0);
    unsigned seq = t_client_ack_seq(c);
    T_ASSERT_EQ(t_client_close_queue(c, "tmp.q"), 0);
    T_ASSERT(wait_ack_status(c, seq, 0, 150));
    T_ASSERT_NULL(t_domain_get_queue(t_broker_get_domain(b, "default"),
                                     "tmp.q"));

    t_evloop_stop(loop);
    T_ASSERT_EQ(t_thread_join(&th), 0);
    t_client_destroy(c);
    t_server_destroy(srv);
    t_broker_destroy(b);
    t_raft_destroy(r);
    t_cluster_destroy(cl);
    t_evloop_destroy(loop);
}

T_TEST(client_raft_autodelete_close_follow_expires) {
    t_evloop *loop = t_evloop_create();
    t_broker *b = t_broker_create("n0");
    t_cluster *cl = t_cluster_create(1);
    T_ASSERT_EQ(t_cluster_add_node(cl, 1, "127.0.0.1", 1), 0);
    T_ASSERT_EQ(t_cluster_add_node(cl, 2, "127.0.0.1", 2), 0);
    T_ASSERT_EQ(t_cluster_set_leader(cl, 1), 0);
    t_raft_config rcfg = {1, 40, 15};
    t_raft *r = t_raft_create(&rcfg);
    T_ASSERT_EQ(t_raft_become_candidate(r), 0);
    T_ASSERT_EQ(t_raft_become_leader(r), 0);
    t_raft_set_replicate_cb(r, raft_noop_repl, NULL);
    T_ASSERT_EQ(t_broker_set_cluster(b, cl), 0);
    T_ASSERT_EQ(t_broker_start(b), 0);
    T_ASSERT_EQ(t_broker_create_queue(b, "default", "tmp.q", 0,
                                      T_QUEUE_FLAG_AUTODELETE), 0);
    T_ASSERT_EQ(t_broker_set_raft(b, r), 0);

    t_server_config cfg;
    t_server_config_init(&cfg);
    cfg.port = 0;
    cfg.idle_timeout_ms = 0;
    t_server *srv = t_server_create(loop, b, &cfg);
    t_server_start(srv);
    uint16_t port = t_server_port(srv);

    t_thread th;
    T_ASSERT_EQ(t_thread_spawn(&th, loop_runner, loop), 0);
    t_time_sleep_us(20000);

    t_client *c = t_client_create("c");
    T_ASSERT_EQ(t_client_dial(c, loop, "127.0.0.1", port), 0);
    T_ASSERT_EQ(t_client_open_follow(c, "tmp.q", T_CLIENT_OPEN_PRODUCER, 500), 0);
    T_ASSERT_EQ(t_client_close_follow(c, "tmp.q", 400), -1);
    T_ASSERT_NOT_NULL(t_domain_get_queue(t_broker_get_domain(b, "default"),
                                         "tmp.q"));

    t_evloop_stop(loop);
    T_ASSERT_EQ(t_thread_join(&th), 0);
    t_client_destroy(c);
    t_server_destroy(srv);
    t_broker_destroy(b);
    t_raft_destroy(r);
    t_cluster_destroy(cl);
    t_evloop_destroy(loop);
}

T_TEST(client_raft_autodelete_close_follow_acks) {
    t_evloop *loop = t_evloop_create();
    t_broker *b = t_broker_create("n0");
    t_cluster *cl = t_cluster_create(1);
    T_ASSERT_EQ(t_cluster_add_node(cl, 1, "127.0.0.1", 1), 0);
    T_ASSERT_EQ(t_cluster_add_node(cl, 2, "127.0.0.1", 2), 0);
    T_ASSERT_EQ(t_cluster_set_leader(cl, 1), 0);
    t_raft_config rcfg = {1, 200, 50};
    t_raft *r = t_raft_create(&rcfg);
    T_ASSERT_EQ(t_raft_become_candidate(r), 0);
    T_ASSERT_EQ(t_raft_become_leader(r), 0);
    raft_late_ctx cx = {loop, r, 2, 0};
    t_raft_set_replicate_cb(r, raft_late_repl, &cx);
    T_ASSERT_EQ(t_broker_set_cluster(b, cl), 0);
    T_ASSERT_EQ(t_broker_start(b), 0);
    T_ASSERT_EQ(t_broker_create_queue(b, "default", "tmp.q", 0,
                                      T_QUEUE_FLAG_AUTODELETE), 0);
    T_ASSERT_EQ(t_broker_set_raft(b, r), 0);

    t_server_config cfg;
    t_server_config_init(&cfg);
    cfg.port = 0;
    cfg.idle_timeout_ms = 0;
    t_server *srv = t_server_create(loop, b, &cfg);
    t_server_start(srv);
    uint16_t port = t_server_port(srv);

    t_thread th;
    T_ASSERT_EQ(t_thread_spawn(&th, loop_runner, loop), 0);
    t_time_sleep_us(20000);

    t_client *c = t_client_create("c");
    T_ASSERT_EQ(t_client_dial(c, loop, "127.0.0.1", port), 0);
    T_ASSERT_EQ(t_client_open_follow(c, "tmp.q",
                                     T_CLIENT_OPEN_PRODUCER | T_CLIENT_QFLAG_AUTODELETE,
                                     500), 0);
    T_ASSERT_EQ(t_client_close_follow(c, "tmp.q", 150), 0);
    T_ASSERT_NULL(t_domain_get_queue(t_broker_get_domain(b, "default"),
                                     "tmp.q"));

    t_evloop_stop(loop);
    T_ASSERT_EQ(t_thread_join(&th), 0);
    t_client_destroy(c);
    t_server_destroy(srv);
    t_broker_destroy(b);
    t_raft_destroy(r);
    t_cluster_destroy(cl);
    t_evloop_destroy(loop);
}

static volatile int g_reject_once;
static volatile int g_reject_rc;

static void on_net_reject_once(const char *queue_name, const uint8_t *data,
                               size_t len, void *ud) {
    on_net_msg(queue_name, data, len, NULL);
    if (g_reject_once) {
        g_reject_once = 0;
        g_reject_rc = t_client_reject((t_client *)ud, queue_name);
    }
}

T_TEST(client_reject_requeues) {
    t_evloop *loop = t_evloop_create();
    t_broker *b = t_broker_create("n0");
    t_broker_start(b);
    t_server_config cfg;
    t_server_config_init(&cfg);
    cfg.port = 0;
    cfg.idle_timeout_ms = 0;
    cfg.push_credits = 1;
    t_server *srv = t_server_create(loop, b, &cfg);
    t_server_start(srv);
    uint16_t port = t_server_port(srv);

    t_thread th;
    T_ASSERT_EQ(t_thread_spawn(&th, loop_runner, loop), 0);
    t_time_sleep_us(20000);

    t_client *prod = t_client_create("p");
    t_client *cons = t_client_create("c");
    T_ASSERT_EQ(t_client_dial(prod, loop, "127.0.0.1", port), 0);
    T_ASSERT_EQ(t_client_dial(cons, loop, "127.0.0.1", port), 0);
    T_ASSERT_EQ(t_client_open_follow(cons, "jobs", T_CLIENT_OPEN_CONSUMER, 500), 0);
    g_got = 0;
    g_reject_once = 1;
    g_reject_rc = -2;
    T_ASSERT_EQ(t_client_subscribe(cons, "jobs", on_net_reject_once, cons), 0);
    T_ASSERT_EQ(t_client_open_follow(prod, "jobs", T_CLIENT_OPEN_PRODUCER, 500), 0);
    T_ASSERT_EQ(t_client_post_follow(prod, "jobs", (const uint8_t *)"r", 1, 0, 500), 0);
    T_ASSERT(wait_flag_ge(&g_got, 2, 500));
    T_ASSERT_EQ(g_got, 2);
    T_ASSERT_EQ(g_reject_rc, 0);
    t_queue *q = (t_queue *)t_domain_get_queue(t_broker_get_domain(b, "default"),
                                              "jobs");
    T_ASSERT_NOT_NULL(q);
    T_ASSERT_EQ((int)t_queue_pending_count(q), 0);
    T_ASSERT_EQ(t_queue_has_inflight(q), 0);

    t_evloop_stop(loop);
    T_ASSERT_EQ(t_thread_join(&th), 0);
    t_client_destroy(prod);
    t_client_destroy(cons);
    t_server_destroy(srv);
    t_broker_destroy(b);
    t_evloop_destroy(loop);
}

T_TEST(client_confirm_follow_acks) {
    t_evloop *loop = t_evloop_create();
    t_broker *b = t_broker_create("n0");
    t_broker_start(b);
    t_server_config cfg;
    t_server_config_init(&cfg);
    cfg.port = 0;
    cfg.idle_timeout_ms = 0;
    t_server *srv = t_server_create(loop, b, &cfg);
    t_server_start(srv);
    uint16_t port = t_server_port(srv);

    t_thread th;
    T_ASSERT_EQ(t_thread_spawn(&th, loop_runner, loop), 0);
    t_time_sleep_us(20000);

    t_client *prod = t_client_create("p");
    t_client *cons = t_client_create("c");
    T_ASSERT_EQ(t_client_set_auto_confirm(cons, 0), 0);
    T_ASSERT_EQ(t_client_dial(prod, loop, "127.0.0.1", port), 0);
    T_ASSERT_EQ(t_client_dial(cons, loop, "127.0.0.1", port), 0);
    T_ASSERT_EQ(t_client_open_follow(cons, "jobs", T_CLIENT_OPEN_CONSUMER, 500), 0);
    g_got = 0;
    T_ASSERT_EQ(t_client_subscribe(cons, "jobs", on_net_msg, NULL), 0);
    T_ASSERT_EQ(t_client_open_follow(prod, "jobs", T_CLIENT_OPEN_PRODUCER, 500), 0);
    T_ASSERT_EQ(t_client_post_follow(prod, "jobs", (const uint8_t *)"ack", 3, 0, 500), 0);
    T_ASSERT(wait_flag_ge(&g_got, 1, 500));
    T_ASSERT(t_client_last_push_id(cons) != 0);
    T_ASSERT_EQ(t_client_confirm_follow(cons, "jobs", 500), 0);
    T_ASSERT_EQ(t_client_confirm_follow(cons, "jobs", 50), -1);
    t_queue *q = (t_queue *)t_domain_get_queue(t_broker_get_domain(b, "default"),
                                              "jobs");
    T_ASSERT_NOT_NULL(q);
    T_ASSERT_EQ((int)t_queue_pending_count(q), 0);
    T_ASSERT_EQ(t_queue_has_inflight(q), 0);

    t_evloop_stop(loop);
    T_ASSERT_EQ(t_thread_join(&th), 0);
    t_client_destroy(prod);
    t_client_destroy(cons);
    t_server_destroy(srv);
    t_broker_destroy(b);
    t_evloop_destroy(loop);
}

T_TEST(client_raft_reject_follow_expires) {
    t_evloop *loop = t_evloop_create();
    t_broker *b = t_broker_create("n0");
    t_cluster *cl = t_cluster_create(1);
    T_ASSERT_EQ(t_cluster_add_node(cl, 1, "127.0.0.1", 1), 0);
    T_ASSERT_EQ(t_cluster_add_node(cl, 2, "127.0.0.1", 2), 0);
    T_ASSERT_EQ(t_cluster_set_leader(cl, 1), 0);
    t_raft_config rcfg = {1, 40, 15};
    t_raft *r = t_raft_create(&rcfg);
    T_ASSERT_EQ(t_raft_become_candidate(r), 0);
    T_ASSERT_EQ(t_raft_become_leader(r), 0);
    t_raft_set_replicate_cb(r, raft_noop_repl, NULL);
    T_ASSERT_EQ(t_broker_set_cluster(b, cl), 0);
    T_ASSERT_EQ(t_broker_start(b), 0);
    T_ASSERT_EQ(t_broker_create_queue(b, "default", "jobs", 0, 0), 0);
    T_ASSERT_EQ(t_broker_publish(b, "jobs", (const uint8_t *)"r", 1, 0), 0);
    T_ASSERT_EQ(t_broker_set_raft(b, r), 0);

    t_server_config cfg;
    t_server_config_init(&cfg);
    cfg.port = 0;
    cfg.idle_timeout_ms = 0;
    t_server *srv = t_server_create(loop, b, &cfg);
    t_server_start(srv);
    uint16_t port = t_server_port(srv);

    t_thread th;
    T_ASSERT_EQ(t_thread_spawn(&th, loop_runner, loop), 0);
    t_time_sleep_us(20000);

    t_client *c = t_client_create("c");
    T_ASSERT_EQ(t_client_set_auto_confirm(c, 0), 0);
    T_ASSERT_EQ(t_client_dial(c, loop, "127.0.0.1", port), 0);
    g_got = 0;
    T_ASSERT_EQ(t_client_subscribe(c, "jobs", on_net_msg, NULL), 0);
    T_ASSERT_EQ(t_client_open_follow(c, "jobs", T_CLIENT_OPEN_CONSUMER, 500), 0);
    T_ASSERT(wait_flag_ge(&g_got, 1, 500));
    T_ASSERT_EQ(t_client_reject_follow(c, "jobs", 400), -1);
    t_queue *q = (t_queue *)t_domain_get_queue(t_broker_get_domain(b, "default"),
                                              "jobs");
    T_ASSERT_NOT_NULL(q);
    T_ASSERT_EQ(t_queue_has_inflight(q), 1);

    t_evloop_stop(loop);
    T_ASSERT_EQ(t_thread_join(&th), 0);
    t_client_destroy(c);
    t_server_destroy(srv);
    t_broker_destroy(b);
    t_raft_destroy(r);
    t_cluster_destroy(cl);
    t_evloop_destroy(loop);
}

T_TEST(client_raft_reject_follow_acks) {
    t_evloop *loop = t_evloop_create();
    t_broker *b = t_broker_create("n0");
    t_cluster *cl = t_cluster_create(1);
    T_ASSERT_EQ(t_cluster_add_node(cl, 1, "127.0.0.1", 1), 0);
    T_ASSERT_EQ(t_cluster_add_node(cl, 2, "127.0.0.1", 2), 0);
    T_ASSERT_EQ(t_cluster_set_leader(cl, 1), 0);
    t_raft_config rcfg = {1, 200, 50};
    t_raft *r = t_raft_create(&rcfg);
    T_ASSERT_EQ(t_raft_become_candidate(r), 0);
    T_ASSERT_EQ(t_raft_become_leader(r), 0);
    raft_late_ctx cx = {loop, r, 2, 0};
    t_raft_set_replicate_cb(r, raft_late_repl, &cx);
    T_ASSERT_EQ(t_broker_set_cluster(b, cl), 0);
    T_ASSERT_EQ(t_broker_start(b), 0);
    T_ASSERT_EQ(t_broker_create_queue(b, "default", "jobs", 0, 0), 0);
    T_ASSERT_EQ(t_broker_publish(b, "jobs", (const uint8_t *)"r", 1, 0), 0);
    T_ASSERT_EQ(t_broker_set_raft(b, r), 0);

    t_server_config cfg;
    t_server_config_init(&cfg);
    cfg.port = 0;
    cfg.idle_timeout_ms = 0;
    t_server *srv = t_server_create(loop, b, &cfg);
    t_server_start(srv);
    uint16_t port = t_server_port(srv);

    t_thread th;
    T_ASSERT_EQ(t_thread_spawn(&th, loop_runner, loop), 0);
    t_time_sleep_us(20000);

    t_client *c = t_client_create("c");
    T_ASSERT_EQ(t_client_set_auto_confirm(c, 0), 0);
    T_ASSERT_EQ(t_client_dial(c, loop, "127.0.0.1", port), 0);
    g_got = 0;
    T_ASSERT_EQ(t_client_subscribe(c, "jobs", on_net_msg, NULL), 0);
    T_ASSERT_EQ(t_client_open_follow(c, "jobs", T_CLIENT_OPEN_CONSUMER, 500), 0);
    T_ASSERT(wait_flag_ge(&g_got, 1, 500));
    T_ASSERT_EQ(t_client_reject_follow(c, "jobs", 150), 0);
    T_ASSERT(wait_flag_ge(&g_got, 2, 500));
    T_ASSERT_EQ(g_got, 2);

    t_evloop_stop(loop);
    T_ASSERT_EQ(t_thread_join(&th), 0);
    t_client_destroy(c);
    t_server_destroy(srv);
    t_broker_destroy(b);
    t_raft_destroy(r);
    t_cluster_destroy(cl);
    t_evloop_destroy(loop);
}

T_TEST(client_unsubscribed_push_not_confirmed) {
    t_evloop *loop = t_evloop_create();
    t_broker *b = t_broker_create("n0");
    t_broker_start(b);
    t_server_config cfg;
    t_server_config_init(&cfg);
    cfg.port = 0;
    cfg.idle_timeout_ms = 0;
    t_server *srv = t_server_create(loop, b, &cfg);
    t_server_start(srv);
    uint16_t port = t_server_port(srv);

    t_thread th;
    T_ASSERT_EQ(t_thread_spawn(&th, loop_runner, loop), 0);
    t_time_sleep_us(20000);

    t_client *prod = t_client_create("p");
    t_client *cons = t_client_create("c");
    T_ASSERT_EQ(t_client_dial(prod, loop, "127.0.0.1", port), 0);
    T_ASSERT_EQ(t_client_dial(cons, loop, "127.0.0.1", port), 0);
    g_got = 0;
    unsigned cseq = t_client_ack_seq(cons);
    T_ASSERT_EQ(t_client_subscribe(cons, "jobs", on_net_msg, NULL), 0);
    T_ASSERT(wait_ack_status(cons, cseq, 0, 500));
    unsigned cseq2 = t_client_ack_seq(cons);
    T_ASSERT_EQ(t_client_unsubscribe(cons, "jobs"), 0);
    T_ASSERT(wait_ack_status(cons, cseq2, 0, 500));
    T_ASSERT_EQ((int)t_client_queue_count(cons), 0);
    T_ASSERT_EQ(t_client_open_follow(prod, "jobs", T_CLIENT_OPEN_PRODUCER, 500), 0);
    T_ASSERT_EQ(t_client_post_follow(prod, "jobs", (const uint8_t *)"lost", 4, 0, 500), 0);
    t_queue *q = (t_queue *)t_domain_get_queue(t_broker_get_domain(b, "default"),
                                              "jobs");
    T_ASSERT_NOT_NULL(q);
    int64_t start = t_time_now_ms();
    while (t_queue_pending_count(q) == 0 && t_time_now_ms() - start < 500)
        t_time_sleep_ms(5);
    T_ASSERT_EQ(g_got, 0);
    T_ASSERT_EQ(t_queue_has_inflight(q), 0);
    T_ASSERT_EQ((int)t_queue_pending_count(q), 1);
    t_client *take = t_client_create("t");
    T_ASSERT_EQ(t_client_dial(take, loop, "127.0.0.1", port), 0);
    T_ASSERT_EQ(t_client_subscribe_follow(take, "jobs", on_net_msg, NULL, 0, 500), 0);
    T_ASSERT(wait_flag_ge(&g_got, 1, 500));
    T_ASSERT_EQ(g_got, 1);

    t_evloop_stop(loop);
    T_ASSERT_EQ(t_thread_join(&th), 0);
    t_client_destroy(prod);
    t_client_destroy(cons);
    t_client_destroy(take);
    t_server_destroy(srv);
    t_broker_destroy(b);
    t_evloop_destroy(loop);
}

T_TEST(client_unsubscribe_releases_exclusive) {
    t_evloop *loop = t_evloop_create();
    t_broker *b = t_broker_create("n0");
    t_broker_start(b);
    t_server_config cfg;
    t_server_config_init(&cfg);
    cfg.port = 0;
    cfg.idle_timeout_ms = 0;
    t_server *srv = t_server_create(loop, b, &cfg);
    t_server_start(srv);
    uint16_t port = t_server_port(srv);

    t_thread th;
    T_ASSERT_EQ(t_thread_spawn(&th, loop_runner, loop), 0);
    t_time_sleep_us(20000);

    t_client *c1 = t_client_create("ex1");
    t_client *c2 = t_client_create("ex2");
    T_ASSERT_EQ(t_client_dial(c1, loop, "127.0.0.1", port), 0);
    T_ASSERT_EQ(t_client_dial(c2, loop, "127.0.0.1", port), 0);
    T_ASSERT_EQ(t_client_subscribe_follow(c1, "ex.q", on_net_msg, NULL,
                                          T_CLIENT_QFLAG_EXCLUSIVE, 500), 0);
    unsigned seq = t_client_ack_seq(c1);
    T_ASSERT_EQ(t_client_unsubscribe(c1, "ex.q"), 0);
    T_ASSERT(wait_ack_status(c1, seq, 0, 500));
    T_ASSERT_EQ(t_client_subscribe_follow(c2, "ex.q", on_net_msg, NULL,
                                          T_CLIENT_QFLAG_EXCLUSIVE, 500), 0);

    t_evloop_stop(loop);
    T_ASSERT_EQ(t_thread_join(&th), 0);
    t_client_destroy(c1);
    t_client_destroy(c2);
    t_server_destroy(srv);
    t_broker_destroy(b);
    t_evloop_destroy(loop);
}

T_TEST(client_unsubscribe_keeps_producer) {
    t_evloop *loop = t_evloop_create();
    t_broker *b = t_broker_create("n0");
    t_broker_start(b);
    t_server_config cfg;
    t_server_config_init(&cfg);
    cfg.port = 0;
    cfg.idle_timeout_ms = 0;
    t_server *srv = t_server_create(loop, b, &cfg);
    t_server_start(srv);
    uint16_t port = t_server_port(srv);

    t_thread th;
    T_ASSERT_EQ(t_thread_spawn(&th, loop_runner, loop), 0);
    t_time_sleep_us(20000);

    t_client *c = t_client_create("both");
    T_ASSERT_EQ(t_client_dial(c, loop, "127.0.0.1", port), 0);
    T_ASSERT_EQ(t_client_open_follow(c, "jobs", T_CLIENT_OPEN_PRODUCER, 500), 0);
    g_got = 0;
    T_ASSERT_EQ(t_client_subscribe_follow(c, "jobs", on_net_msg, NULL, 0, 500), 0);
    T_ASSERT_EQ((int)t_client_queue_count(c), 1);
    T_ASSERT_EQ(t_client_post_follow(c, "jobs", (const uint8_t *)"in", 2, 0, 500), 0);
    T_ASSERT(wait_flag_ge(&g_got, 1, 500));
    T_ASSERT_EQ(g_got, 1);
    T_ASSERT_EQ(t_client_unsubscribe(c, "jobs"), 0);
    T_ASSERT_EQ((int)t_client_queue_count(c), 1);
    T_ASSERT_EQ(t_client_post_follow(c, "jobs", (const uint8_t *)"x", 1, 0, 500), 0);
    t_queue *q = (t_queue *)t_domain_get_queue(t_broker_get_domain(b, "default"),
                                              "jobs");
    T_ASSERT_NOT_NULL(q);
    T_ASSERT_EQ((int)t_queue_pending_count(q), 1);

    t_evloop_stop(loop);
    T_ASSERT_EQ(t_thread_join(&th), 0);
    t_client_destroy(c);
    t_server_destroy(srv);
    t_broker_destroy(b);
    t_evloop_destroy(loop);
}

T_TEST(client_subscribe_close_follow_autodelete) {
    t_evloop *loop = t_evloop_create();
    t_broker *b = t_broker_create("n0");
    t_broker_start(b);
    t_server_config cfg;
    t_server_config_init(&cfg);
    cfg.port = 0;
    cfg.idle_timeout_ms = 0;
    t_server *srv = t_server_create(loop, b, &cfg);
    t_server_start(srv);
    uint16_t port = t_server_port(srv);

    t_thread th;
    T_ASSERT_EQ(t_thread_spawn(&th, loop_runner, loop), 0);
    t_time_sleep_us(20000);

    t_client *c = t_client_create("c");
    T_ASSERT_EQ(t_client_dial(c, loop, "127.0.0.1", port), 0);
    g_got = 0;
    T_ASSERT_EQ(t_client_subscribe_follow(c, "tmp.q", on_net_msg, NULL,
                                          T_CLIENT_QFLAG_AUTODELETE, 500), 0);
    T_ASSERT_NOT_NULL(t_domain_get_queue(t_broker_get_domain(b, "default"),
                                         "tmp.q"));
    T_ASSERT_EQ(t_client_close_follow(c, "tmp.q", 500), 0);
    T_ASSERT_NULL(t_domain_get_queue(t_broker_get_domain(b, "default"),
                                     "tmp.q"));

    t_evloop_stop(loop);
    T_ASSERT_EQ(t_thread_join(&th), 0);
    t_client_destroy(c);
    t_server_destroy(srv);
    t_broker_destroy(b);
    t_evloop_destroy(loop);
}

T_TEST(client_subscribe_follow_exclusive) {
    t_evloop *loop = t_evloop_create();
    t_broker *b = t_broker_create("n0");
    t_broker_start(b);
    t_server_config cfg;
    t_server_config_init(&cfg);
    cfg.port = 0;
    cfg.idle_timeout_ms = 0;
    t_server *srv = t_server_create(loop, b, &cfg);
    t_server_start(srv);
    uint16_t port = t_server_port(srv);

    t_thread th;
    T_ASSERT_EQ(t_thread_spawn(&th, loop_runner, loop), 0);
    t_time_sleep_us(20000);

    t_client *c1 = t_client_create("ex1");
    t_client *c2 = t_client_create("ex2");
    T_ASSERT_EQ(t_client_dial(c1, loop, "127.0.0.1", port), 0);
    T_ASSERT_EQ(t_client_dial(c2, loop, "127.0.0.1", port), 0);
    T_ASSERT_EQ(t_client_subscribe_follow(c1, "ex.q", on_net_msg, NULL,
                                          T_CLIENT_QFLAG_EXCLUSIVE, 500), 0);
    T_ASSERT_EQ(t_client_subscribe_follow(c2, "ex.q", on_net_msg, NULL,
                                          T_CLIENT_QFLAG_EXCLUSIVE, 500), -1);
    T_ASSERT_EQ(t_client_last_status(c2), (int)T_ERR_BUSY);
    T_ASSERT_EQ(t_client_close_follow(c1, "ex.q", 500), 0);
    T_ASSERT_EQ(t_client_subscribe_follow(c2, "ex.q", on_net_msg, NULL,
                                          T_CLIENT_QFLAG_EXCLUSIVE, 500), 0);

    t_evloop_stop(loop);
    T_ASSERT_EQ(t_thread_join(&th), 0);
    t_client_destroy(c1);
    t_client_destroy(c2);
    t_server_destroy(srv);
    t_broker_destroy(b);
    t_evloop_destroy(loop);
}

T_TEST(client_subscribe_then_post_follow) {
    t_evloop *loop = t_evloop_create();
    t_broker *b = t_broker_create("n0");
    t_broker_start(b);
    t_server_config cfg;
    t_server_config_init(&cfg);
    cfg.port = 0;
    cfg.idle_timeout_ms = 0;
    t_server *srv = t_server_create(loop, b, &cfg);
    t_server_start(srv);
    uint16_t port = t_server_port(srv);

    t_thread th;
    T_ASSERT_EQ(t_thread_spawn(&th, loop_runner, loop), 0);
    t_time_sleep_us(20000);

    g_got = 0;
    t_client *c = t_client_create("both");
    T_ASSERT_EQ(t_client_dial(c, loop, "127.0.0.1", port), 0);
    T_ASSERT_EQ(t_client_subscribe_follow(c, "jobs", on_net_msg, NULL, 0, 500), 0);
    T_ASSERT_EQ(t_client_post_follow(c, "jobs", (const uint8_t *)"hi", 2, 0, 500), 0);
    T_ASSERT(wait_flag_ge(&g_got, 1, 500));
    T_ASSERT_EQ(g_got, 1);

    t_evloop_stop(loop);
    T_ASSERT_EQ(t_thread_join(&th), 0);
    t_client_destroy(c);
    t_server_destroy(srv);
    t_broker_destroy(b);
    t_evloop_destroy(loop);
}

static volatile int g_out_got;
static void on_out_msg(const char *queue_name, const uint8_t *data, size_t len,
                       void *ud) {
    (void)queue_name;
    (void)data;
    (void)len;
    (void)ud;
    g_out_got++;
}

T_TEST(client_join_replays_after_subscribe) {
    t_evloop *loop = t_evloop_create();
    t_broker *b = t_broker_create("n0");
    t_broker_start(b);
    t_server_config cfg;
    t_server_config_init(&cfg);
    cfg.port = 0;
    cfg.idle_timeout_ms = 0;
    t_server *srv = t_server_create(loop, b, &cfg);
    t_server_start(srv);
    uint16_t port = t_server_port(srv);

    t_thread th;
    T_ASSERT_EQ(t_thread_spawn(&th, loop_runner, loop), 0);
    t_time_sleep_us(20000);

    t_client *prod = t_client_create("p");
    t_client *cons = t_client_create("c");
    t_client *out = t_client_create("o");
    T_ASSERT_EQ(t_client_dial(prod, loop, "127.0.0.1", port), 0);
    T_ASSERT_EQ(t_client_dial(cons, loop, "127.0.0.1", port), 0);
    T_ASSERT_EQ(t_client_dial(out, loop, "127.0.0.1", port), 0);
    unsigned seq = t_client_ack_seq(cons);
    T_ASSERT_EQ(t_client_join(cons, "workers", "c1", "jobs"), 0);
    T_ASSERT(wait_ack_status(cons, seq, (int)T_ERR_PERMISSION, 500));
    g_got = 0;
    g_out_got = 0;
    T_ASSERT_EQ(t_client_subscribe_follow(cons, "jobs", on_net_msg, NULL, 0, 500), 0);
    T_ASSERT_EQ(t_client_subscribe_follow(out, "jobs", on_out_msg, NULL, 0, 500), 0);
    T_ASSERT_EQ(t_client_post_follow(prod, "jobs", (const uint8_t *)"hi", 2, 0, 500), 0);
    T_ASSERT(wait_flag_ge(&g_got, 1, 500));
    T_ASSERT_EQ(g_got, 1);
    t_time_sleep_ms(50);
    T_ASSERT_EQ(g_out_got, 0);

    t_evloop_stop(loop);
    T_ASSERT_EQ(t_thread_join(&th), 0);
    t_client_destroy(prod);
    t_client_destroy(cons);
    t_client_destroy(out);
    t_server_destroy(srv);
    t_broker_destroy(b);
    t_evloop_destroy(loop);
}

T_TEST(client_join_replays_after_leader_redial) {
    t_evloop *loop = t_evloop_create();

    t_broker *lb = t_broker_create("lead");
    t_cluster *lcl = t_cluster_create(1);
    T_ASSERT_EQ(t_cluster_add_node(lcl, 1, "127.0.0.1", 1), 0);
    T_ASSERT_EQ(t_cluster_add_node(lcl, 2, "127.0.0.1", 2), 0);
    T_ASSERT_EQ(t_cluster_set_leader(lcl, 1), 0);
    T_ASSERT_EQ(t_broker_set_cluster(lb, lcl), 0);
    t_broker_start(lb);
    t_server_config lcfg;
    t_server_config_init(&lcfg);
    lcfg.port = 0;
    lcfg.idle_timeout_ms = 0;
    t_server *ls = t_server_create(loop, lb, &lcfg);
    t_server_start(ls);
    uint16_t lport = t_server_port(ls);
    T_ASSERT_EQ(t_node_set_client_port(t_cluster_get_node(lcl, 1), lport), 0);

    t_broker *fb = t_broker_create("foll");
    t_cluster *fcl = t_cluster_create(2);
    T_ASSERT_EQ(t_cluster_add_node(fcl, 1, "127.0.0.1", 1), 0);
    T_ASSERT_EQ(t_cluster_add_node(fcl, 2, "127.0.0.1", 2), 0);
    T_ASSERT_EQ(t_node_set_client_port(t_cluster_get_node(fcl, 1), lport), 0);
    T_ASSERT_EQ(t_cluster_set_leader(fcl, 1), 0);
    T_ASSERT_EQ(t_broker_set_cluster(fb, fcl), 0);
    t_broker_start(fb);
    t_server_config fcfg;
    t_server_config_init(&fcfg);
    fcfg.port = 0;
    fcfg.idle_timeout_ms = 0;
    t_server *fs = t_server_create(loop, fb, &fcfg);
    t_server_start(fs);
    uint16_t fport = t_server_port(fs);

    t_thread th;
    T_ASSERT_EQ(t_thread_spawn(&th, loop_runner, loop), 0);
    t_time_sleep_us(20000);

    t_client *prod = t_client_create("p");
    t_client *cons = t_client_create("c");
    t_client *out = t_client_create("o");
    T_ASSERT_EQ(t_client_dial(prod, loop, "127.0.0.1", lport), 0);
    T_ASSERT_EQ(t_client_dial(cons, loop, "127.0.0.1", fport), 0);
    T_ASSERT_EQ(t_client_dial(out, loop, "127.0.0.1", lport), 0);
    unsigned seq = t_client_ack_seq(cons);
    T_ASSERT_EQ(t_client_join(cons, "workers", "c1", "jobs"), 0);
    T_ASSERT(wait_ack_status(cons, seq, (int)T_ERR_PERMISSION, 500));
    g_got = 0;
    g_out_got = 0;
    T_ASSERT_EQ(t_client_subscribe_follow(cons, "jobs", on_net_msg, NULL, 0, 500), 0);
    T_ASSERT_EQ(t_client_subscribe_follow(out, "jobs", on_out_msg, NULL, 0, 500), 0);
    T_ASSERT_EQ(t_client_post_follow(prod, "jobs", (const uint8_t *)"hi", 2, 0, 500), 0);
    T_ASSERT(wait_flag_ge(&g_got, 1, 500));
    T_ASSERT_EQ(g_got, 1);
    t_time_sleep_ms(50);
    T_ASSERT_EQ(g_out_got, 0);

    t_evloop_stop(loop);
    T_ASSERT_EQ(t_thread_join(&th), 0);
    t_client_destroy(prod);
    t_client_destroy(cons);
    t_client_destroy(out);
    t_server_destroy(fs);
    t_server_destroy(ls);
    t_broker_destroy(fb);
    t_broker_destroy(lb);
    t_cluster_destroy(fcl);
    t_cluster_destroy(lcl);
    t_evloop_destroy(loop);
}

T_TEST(client_redial_restores_all_opens) {
    t_evloop *loop = t_evloop_create();

    t_broker *lb = t_broker_create("lead");
    t_cluster *lcl = t_cluster_create(1);
    T_ASSERT_EQ(t_cluster_add_node(lcl, 1, "127.0.0.1", 1), 0);
    T_ASSERT_EQ(t_cluster_add_node(lcl, 2, "127.0.0.1", 2), 0);
    T_ASSERT_EQ(t_cluster_set_leader(lcl, 1), 0);
    T_ASSERT_EQ(t_broker_set_cluster(lb, lcl), 0);
    t_broker_start(lb);
    t_server_config lcfg;
    t_server_config_init(&lcfg);
    lcfg.port = 0;
    lcfg.idle_timeout_ms = 0;
    t_server *ls = t_server_create(loop, lb, &lcfg);
    t_server_start(ls);
    uint16_t lport = t_server_port(ls);
    T_ASSERT_EQ(t_node_set_client_port(t_cluster_get_node(lcl, 1), lport), 0);

    t_broker *fb = t_broker_create("foll");
    t_cluster *fcl = t_cluster_create(2);
    T_ASSERT_EQ(t_cluster_add_node(fcl, 1, "127.0.0.1", 1), 0);
    T_ASSERT_EQ(t_cluster_add_node(fcl, 2, "127.0.0.1", 2), 0);
    T_ASSERT_EQ(t_node_set_client_port(t_cluster_get_node(fcl, 1), lport), 0);
    T_ASSERT_EQ(t_cluster_set_leader(fcl, 1), 0);
    T_ASSERT_EQ(t_broker_set_cluster(fb, fcl), 0);
    t_broker_start(fb);
    t_server_config fcfg;
    t_server_config_init(&fcfg);
    fcfg.port = 0;
    fcfg.idle_timeout_ms = 0;
    t_server *fs = t_server_create(loop, fb, &fcfg);
    t_server_start(fs);
    uint16_t fport = t_server_port(fs);

    t_thread th;
    T_ASSERT_EQ(t_thread_spawn(&th, loop_runner, loop), 0);
    t_time_sleep_us(20000);

    t_client *cons = t_client_create("c");
    t_client *prod = t_client_create("p");
    T_ASSERT_EQ(t_client_dial(cons, loop, "127.0.0.1", fport), 0);
    T_ASSERT_EQ(t_client_dial(prod, loop, "127.0.0.1", lport), 0);
    unsigned seq = t_client_ack_seq(cons);
    T_ASSERT_EQ(t_client_subscribe(cons, "a.q", on_net_msg, NULL), 0);
    T_ASSERT(wait_ack_status(cons, seq, (int)T_ERR_AGAIN, 500));
    seq = t_client_ack_seq(cons);
    T_ASSERT_EQ(t_client_subscribe(cons, "b.q", on_out_msg, NULL), 0);
    T_ASSERT(wait_ack_status(cons, seq, (int)T_ERR_AGAIN, 500));
    T_ASSERT_EQ(t_client_open_follow(cons, "a.q", T_CLIENT_OPEN_CONSUMER, 500), 0);
    g_got = 0;
    g_out_got = 0;
    T_ASSERT_EQ(t_client_post_follow(prod, "b.q", (const uint8_t *)"hi", 2, 0, 500), 0);
    T_ASSERT(wait_flag_ge(&g_out_got, 1, 500));
    T_ASSERT_EQ(g_out_got, 1);
    T_ASSERT_EQ(g_got, 0);

    t_evloop_stop(loop);
    T_ASSERT_EQ(t_thread_join(&th), 0);
    t_client_destroy(cons);
    t_client_destroy(prod);
    t_server_destroy(fs);
    t_server_destroy(ls);
    t_broker_destroy(fb);
    t_broker_destroy(lb);
    t_cluster_destroy(fcl);
    t_cluster_destroy(lcl);
    t_evloop_destroy(loop);
}

T_TEST(client_subscribe_follow_to_leader) {
    t_evloop *loop = t_evloop_create();

    t_broker *lb = t_broker_create("lead");
    t_cluster *lcl = t_cluster_create(1);
    T_ASSERT_EQ(t_cluster_add_node(lcl, 1, "127.0.0.1", 1), 0);
    T_ASSERT_EQ(t_cluster_add_node(lcl, 2, "127.0.0.1", 2), 0);
    T_ASSERT_EQ(t_cluster_set_leader(lcl, 1), 0);
    T_ASSERT_EQ(t_broker_set_cluster(lb, lcl), 0);
    t_broker_start(lb);
    t_server_config lcfg;
    t_server_config_init(&lcfg);
    lcfg.port = 0;
    lcfg.idle_timeout_ms = 0;
    t_server *ls = t_server_create(loop, lb, &lcfg);
    t_server_start(ls);
    uint16_t lport = t_server_port(ls);
    T_ASSERT_EQ(t_node_set_client_port(t_cluster_get_node(lcl, 1), lport), 0);

    t_broker *fb = t_broker_create("foll");
    t_cluster *fcl = t_cluster_create(2);
    T_ASSERT_EQ(t_cluster_add_node(fcl, 1, "127.0.0.1", 1), 0);
    T_ASSERT_EQ(t_cluster_add_node(fcl, 2, "127.0.0.1", 2), 0);
    T_ASSERT_EQ(t_node_set_client_port(t_cluster_get_node(fcl, 1), lport), 0);
    T_ASSERT_EQ(t_cluster_set_leader(fcl, 1), 0);
    T_ASSERT_EQ(t_broker_set_cluster(fb, fcl), 0);
    t_broker_start(fb);
    t_server_config fcfg;
    t_server_config_init(&fcfg);
    fcfg.port = 0;
    fcfg.idle_timeout_ms = 0;
    t_server *fs = t_server_create(loop, fb, &fcfg);
    t_server_start(fs);
    uint16_t fport = t_server_port(fs);

    t_thread th;
    T_ASSERT_EQ(t_thread_spawn(&th, loop_runner, loop), 0);
    t_time_sleep_us(20000);

    g_got = 0;
    t_client *cons = t_client_create("c");
    t_client *prod = t_client_create("p");
    T_ASSERT_EQ(t_client_dial(cons, loop, "127.0.0.1", fport), 0);
    T_ASSERT_EQ(t_client_subscribe_follow(cons, "jobs", on_net_msg, NULL, 0, 500), 0);
    T_ASSERT_EQ(t_client_dial(prod, loop, "127.0.0.1", lport), 0);
    T_ASSERT_EQ(t_client_post_follow(prod, "jobs", (const uint8_t *)"hi", 2, 0, 500), 0);
    T_ASSERT(wait_flag_ge(&g_got, 1, 500));
    T_ASSERT_EQ(g_got, 1);

    t_evloop_stop(loop);
    T_ASSERT_EQ(t_thread_join(&th), 0);
    t_client_destroy(cons);
    t_client_destroy(prod);
    t_server_destroy(fs);
    t_server_destroy(ls);
    t_broker_destroy(fb);
    t_broker_destroy(lb);
    t_cluster_destroy(fcl);
    t_cluster_destroy(lcl);
    t_evloop_destroy(loop);
}

static char g_pri_got[2][16];
static int g_pri_pri[2];
static volatile int g_pri_n;

static void on_pri_msg(const char *queue_name, const uint8_t *data, size_t len,
                       void *ud) {
    (void)queue_name;
    int i = g_pri_n;
    if (i >= 0 && i < 2) {
        size_t n = len < sizeof(g_pri_got[0]) - 1 ? len : sizeof(g_pri_got[0]) - 1;
        memcpy(g_pri_got[i], data, n);
        g_pri_got[i][n] = '\0';
        g_pri_pri[i] = t_client_last_push_priority((t_client *)ud);
    }
    g_pri_n++;
}

T_TEST(client_priority_orders_backlog) {
    t_evloop *loop = t_evloop_create();
    t_broker *b = t_broker_create("n0");
    t_broker_start(b);
    t_server_config cfg;
    t_server_config_init(&cfg);
    cfg.port = 0;
    cfg.idle_timeout_ms = 0;
    t_server *srv = t_server_create(loop, b, &cfg);
    t_server_start(srv);
    uint16_t port = t_server_port(srv);

    t_thread th;
    T_ASSERT_EQ(t_thread_spawn(&th, loop_runner, loop), 0);
    t_time_sleep_us(20000);

    t_client *prod = t_client_create("p");
    T_ASSERT_EQ(t_client_dial(prod, loop, "127.0.0.1", port), 0);
    T_ASSERT_EQ(t_client_open_follow(prod, "pri.q",
                                     T_CLIENT_OPEN_PRODUCER | T_CLIENT_QTYPE_PRIORITY,
                                     500), 0);
    T_ASSERT_EQ(t_client_post_follow(prod, "pri.q", (const uint8_t *)"low", 3, 10, 500), 0);
    T_ASSERT_EQ(t_client_post_follow(prod, "pri.q", (const uint8_t *)"high", 4, 1, 500), 0);
    t_queue *q = (t_queue *)t_domain_get_queue(t_broker_get_domain(b, "default"),
                                              "pri.q");
    T_ASSERT_NOT_NULL(q);
    T_ASSERT_EQ((int)t_queue_get_type(q), (int)T_QUEUE_PRIORITY);

    t_client *cons = t_client_create("c");
    T_ASSERT_EQ(t_client_dial(cons, loop, "127.0.0.1", port), 0);
    g_pri_n = 0;
    g_pri_got[0][0] = 0;
    g_pri_got[1][0] = 0;
    T_ASSERT_EQ(t_client_subscribe_follow(cons, "pri.q", on_pri_msg, cons, 0, 500), 0);
    T_ASSERT(wait_flag_ge(&g_pri_n, 2, 500));
    T_ASSERT_STR_EQ(g_pri_got[0], "high");
    T_ASSERT_STR_EQ(g_pri_got[1], "low");
    T_ASSERT_EQ(g_pri_pri[0], 1);
    T_ASSERT_EQ(g_pri_pri[1], 10);

    t_evloop_stop(loop);
    T_ASSERT_EQ(t_thread_join(&th), 0);
    t_client_destroy(prod);
    t_client_destroy(cons);
    t_server_destroy(srv);
    t_broker_destroy(b);
    t_evloop_destroy(loop);
}

static volatile int g_bc_a;
static volatile int g_bc_b;

static void on_bc_a(const char *queue_name, const uint8_t *data, size_t len, void *ud) {
    (void)queue_name;
    (void)data;
    (void)len;
    (void)ud;
    g_bc_a++;
}

static void on_bc_b(const char *queue_name, const uint8_t *data, size_t len, void *ud) {
    (void)queue_name;
    (void)data;
    (void)len;
    (void)ud;
    g_bc_b++;
}

T_TEST(client_broadcast_fans_out) {
    t_evloop *loop = t_evloop_create();
    t_broker *b = t_broker_create("n0");
    t_broker_start(b);
    t_server_config cfg;
    t_server_config_init(&cfg);
    cfg.port = 0;
    cfg.idle_timeout_ms = 0;
    t_server *srv = t_server_create(loop, b, &cfg);
    t_server_start(srv);
    uint16_t port = t_server_port(srv);

    t_thread th;
    T_ASSERT_EQ(t_thread_spawn(&th, loop_runner, loop), 0);
    t_time_sleep_us(20000);

    t_client *a = t_client_create("a");
    t_client *bcli = t_client_create("b");
    t_client *prod = t_client_create("p");
    T_ASSERT_EQ(t_client_dial(a, loop, "127.0.0.1", port), 0);
    T_ASSERT_EQ(t_client_dial(bcli, loop, "127.0.0.1", port), 0);
    T_ASSERT_EQ(t_client_dial(prod, loop, "127.0.0.1", port), 0);
    T_ASSERT_EQ(t_client_subscribe_follow(a, "bc.q", on_bc_a, NULL,
                                          T_CLIENT_QTYPE_BROADCAST, 500), 0);
    T_ASSERT_EQ(t_client_subscribe_follow(bcli, "bc.q", on_bc_b, NULL, 0, 500), 0);
    T_ASSERT_EQ((int)t_queue_get_type(
                    t_domain_get_queue(t_broker_get_domain(b, "default"), "bc.q")),
                (int)T_QUEUE_BROADCAST);
    g_bc_a = 0;
    g_bc_b = 0;
    T_ASSERT_EQ(t_client_open_follow(prod, "bc.q", T_CLIENT_OPEN_PRODUCER, 500), 0);
    T_ASSERT_EQ(t_client_post_follow(prod, "bc.q", (const uint8_t *)"fan", 3, 0, 500), 0);
    T_ASSERT(wait_flag_ge(&g_bc_a, 1, 500));
    T_ASSERT(wait_flag_ge(&g_bc_b, 1, 500));

    t_evloop_stop(loop);
    T_ASSERT_EQ(t_thread_join(&th), 0);
    t_client_destroy(a);
    t_client_destroy(bcli);
    t_client_destroy(prod);
    t_server_destroy(srv);
    t_broker_destroy(b);
    t_evloop_destroy(loop);
}

T_TEST(client_broadcast_durable_invalid) {
    t_evloop *loop = t_evloop_create();
    t_broker *b = t_broker_create("n0");
    t_broker_start(b);
    t_server_config cfg;
    t_server_config_init(&cfg);
    cfg.port = 0;
    cfg.idle_timeout_ms = 0;
    t_server *srv = t_server_create(loop, b, &cfg);
    t_server_start(srv);
    uint16_t port = t_server_port(srv);

    t_thread th;
    T_ASSERT_EQ(t_thread_spawn(&th, loop_runner, loop), 0);
    t_time_sleep_us(20000);

    t_client *c = t_client_create("c");
    T_ASSERT_EQ(t_client_dial(c, loop, "127.0.0.1", port), 0);
    T_ASSERT_EQ(t_client_open_follow(c, "bc.q",
                                     T_CLIENT_OPEN_PRODUCER | T_CLIENT_QTYPE_BROADCAST |
                                         T_CLIENT_QFLAG_DURABLE,
                                     500), -1);
    T_ASSERT_EQ(t_client_last_status(c), (int)T_ERR_INVALID);

    t_evloop_stop(loop);
    T_ASSERT_EQ(t_thread_join(&th), 0);
    t_client_destroy(c);
    t_server_destroy(srv);
    t_broker_destroy(b);
    t_evloop_destroy(loop);
}

T_TEST(server_non_loopback_requires_psk) {
    t_evloop *loop = t_evloop_create();
    t_broker *b = t_broker_create("n0");
    t_broker_start(b);
    t_server_config cfg;
    t_server_config_init(&cfg);
    cfg.host = "0.0.0.0";
    cfg.port = 0;
    cfg.idle_timeout_ms = 0;
    t_server *srv = t_server_create(loop, b, &cfg);
    T_ASSERT_NOT_NULL(srv);
    T_ASSERT_EQ(t_server_start(srv), -1);
    t_server_destroy(srv);
    t_broker_destroy(b);
    t_evloop_destroy(loop);
}

T_TEST(server_psk_rejects_unauthenticated) {
    t_evloop *loop = t_evloop_create();
    t_broker *b = t_broker_create("n0");
    t_broker_start(b);
    t_server_config cfg;
    t_server_config_init(&cfg);
    cfg.port = 0;
    cfg.idle_timeout_ms = 0;
    const uint8_t psk[] = "unit-psk";
    cfg.psk = psk;
    cfg.psk_len = sizeof(psk) - 1;
    t_server *srv = t_server_create(loop, b, &cfg);
    T_ASSERT_EQ(t_server_start(srv), 0);
    uint16_t port = t_server_port(srv);

    t_thread th;
    T_ASSERT_EQ(t_thread_spawn(&th, loop_runner, loop), 0);
    t_time_sleep_us(20000);

    t_client *c = t_client_create("c");
    T_ASSERT_EQ(t_client_dial(c, loop, "127.0.0.1", port), 0);
    unsigned seq = t_client_ack_seq(c);
    T_ASSERT_EQ(t_client_open_queue(c, "jobs", T_CLIENT_OPEN_PRODUCER), 0);
    T_ASSERT(wait_ack_status(c, seq, (int)T_ERR_PERMISSION, 500));

    t_evloop_stop(loop);
    T_ASSERT_EQ(t_thread_join(&th), 0);
    t_client_destroy(c);
    t_server_destroy(srv);
    t_broker_destroy(b);
    t_evloop_destroy(loop);
}

T_TEST(server_psk_accepts_matching_client) {
    t_evloop *loop = t_evloop_create();
    t_broker *b = t_broker_create("n0");
    t_broker_start(b);
    t_server_config cfg;
    t_server_config_init(&cfg);
    cfg.port = 0;
    cfg.idle_timeout_ms = 0;
    const uint8_t psk[] = "unit-psk";
    cfg.psk = psk;
    cfg.psk_len = sizeof(psk) - 1;
    t_server *srv = t_server_create(loop, b, &cfg);
    T_ASSERT_EQ(t_server_start(srv), 0);
    uint16_t port = t_server_port(srv);

    t_thread th;
    T_ASSERT_EQ(t_thread_spawn(&th, loop_runner, loop), 0);
    t_time_sleep_us(20000);

    t_client *c = t_client_create("c");
    T_ASSERT_EQ(t_client_set_psk(c, psk, sizeof(psk) - 1), 0);
    T_ASSERT_EQ(t_client_dial(c, loop, "127.0.0.1", port), 0);
    T_ASSERT(t_client_is_connected(c));
    T_ASSERT_EQ(t_client_last_status(c), 0);
    T_ASSERT(t_client_ack_seq(c) > 0u);
    unsigned seq = t_client_ack_seq(c);
    T_ASSERT_EQ(t_client_open_queue(c, "jobs", T_CLIENT_OPEN_PRODUCER), 0);
    T_ASSERT(wait_ack_status(c, seq, 0, 500));
    seq = t_client_ack_seq(c);
    T_ASSERT_EQ(t_client_post(c, "jobs", (const uint8_t *)"x", 1, 0), 0);
    T_ASSERT(wait_ack_status(c, seq, 0, 500));

    t_evloop_stop(loop);
    T_ASSERT_EQ(t_thread_join(&th), 0);
    t_client_destroy(c);
    t_server_destroy(srv);
    t_broker_destroy(b);
    t_evloop_destroy(loop);
}

T_TEST(server_psk_rejects_wrong_key) {
    t_evloop *loop = t_evloop_create();
    t_broker *b = t_broker_create("n0");
    t_broker_start(b);
    t_server_config cfg;
    t_server_config_init(&cfg);
    cfg.port = 0;
    cfg.idle_timeout_ms = 0;
    const uint8_t psk[] = "unit-psk";
    const uint8_t bad[] = "other-psk";
    cfg.psk = psk;
    cfg.psk_len = sizeof(psk) - 1;
    t_server *srv = t_server_create(loop, b, &cfg);
    T_ASSERT_EQ(t_server_start(srv), 0);
    uint16_t port = t_server_port(srv);

    t_thread th;
    T_ASSERT_EQ(t_thread_spawn(&th, loop_runner, loop), 0);
    t_time_sleep_us(20000);

    t_client *c = t_client_create("c");
    T_ASSERT_EQ(t_client_set_psk(c, bad, sizeof(bad) - 1), 0);
    T_ASSERT_EQ(t_client_dial(c, loop, "127.0.0.1", port), -1);
    T_ASSERT(!t_client_is_connected(c));
    T_ASSERT_EQ(t_client_last_status(c), (int)T_ERR_PERMISSION);

    t_evloop_stop(loop);
    T_ASSERT_EQ(t_thread_join(&th), 0);
    t_client_destroy(c);
    t_server_destroy(srv);
    t_broker_destroy(b);
    t_evloop_destroy(loop);
}

static volatile int g_raw_push_n;
static volatile int g_raw_ack_n;
static volatile int g_raw_ack_status;
static uint64_t g_raw_push_ids[8];

static void raw_on_push(t_conn *conn, const t_proto_msg *msg, void *ud) {
    (void)conn;
    (void)ud;
    if (!msg) return;
    if (msg->header.type == T_MSG_ACK) {
        t_wire_ack a;
        if (t_wire_decode_ack(msg->payload, msg->payload_len, &a) == 0) {
            g_raw_ack_status = a.status;
            g_raw_ack_n++;
        }
        return;
    }
    if (msg->header.type != T_MSG_PUSH) return;
    t_wire_push p;
    if (t_wire_decode_push(msg->payload, msg->payload_len, &p) != 0) return;
    if (g_raw_push_n < (int)(sizeof(g_raw_push_ids) / sizeof(g_raw_push_ids[0])))
        g_raw_push_ids[g_raw_push_n] = p.msg_id;
    g_raw_push_n++;
}

static int raw_send_open_consumer_type(t_conn *conn, const char *name, uint8_t qtype) {
    uint8_t buf[3 + 2 + T_WIRE_MAX_NAME];
    int n = t_wire_encode_open(buf, sizeof(buf), qtype, T_QUEUE_FLAG_NONE,
                               T_WIRE_MODE_CONSUMER, name);
    if (n < 0) return -1;
    t_proto_msg m;
    t_proto_header_init(&m.header, T_MSG_OPEN_QUEUE, (uint32_t)n);
    m.payload = buf;
    m.payload_len = (size_t)n;
    return t_conn_send(conn, &m);
}

static int raw_send_open_consumer(t_conn *conn, const char *name) {
    return raw_send_open_consumer_type(conn, name, (uint8_t)T_QUEUE_FIFO);
}

static int raw_send_confirm(t_conn *conn, uint64_t msg_id, const char *name) {
    uint8_t buf[8 + 2 + T_WIRE_MAX_NAME];
    int n = t_wire_encode_confirm(buf, sizeof(buf), msg_id, name);
    if (n < 0) return -1;
    t_proto_msg m;
    t_proto_header_init(&m.header, T_MSG_CONFIRM, (uint32_t)n);
    m.payload = buf;
    m.payload_len = (size_t)n;
    return t_conn_send(conn, &m);
}

T_TEST(server_push_credits_cap) {
    t_evloop *loop = t_evloop_create();
    t_broker *b = t_broker_create("n0");
    t_broker_start(b);
    t_server_config cfg;
    t_server_config_init(&cfg);
    cfg.port = 0;
    cfg.idle_timeout_ms = 0;
    cfg.push_credits = 2;
    t_server *srv = t_server_create(loop, b, &cfg);
    t_server_start(srv);
    uint16_t port = t_server_port(srv);

    t_thread th;
    T_ASSERT_EQ(t_thread_spawn(&th, loop_runner, loop), 0);
    t_time_sleep_us(20000);

    int cfd = t_socket_dial_ipv4("127.0.0.1", port);
    T_ASSERT(cfd >= 0);
    t_conn *cons = t_conn_create(cfd, loop);
    T_ASSERT_NOT_NULL(cons);
    g_raw_push_n = 0;
    t_conn_set_on_msg(cons, raw_on_push, NULL);
    T_ASSERT_EQ(raw_send_open_consumer(cons, "jobs"), 0);

    t_client *prod = t_client_create("p");
    T_ASSERT_EQ(t_client_dial(prod, loop, "127.0.0.1", port), 0);
    unsigned pseq = t_client_ack_seq(prod);
    T_ASSERT_EQ(t_client_open_queue(prod, "jobs", T_CLIENT_OPEN_PRODUCER), 0);
    T_ASSERT(wait_ack_status(prod, pseq, 0, 500));

    const uint8_t x[] = "x";
    int i;
    for (i = 0; i < 5; i++)
        T_ASSERT_EQ(t_client_post(prod, "jobs", x, 1, 0), 0);
    T_ASSERT(wait_flag_ge(&g_raw_push_n, 2, 500));
    T_ASSERT_EQ(g_raw_push_n, 2);

    T_ASSERT_EQ(raw_send_confirm(cons, g_raw_push_ids[0], "jobs"), 0);
    T_ASSERT_EQ(raw_send_confirm(cons, g_raw_push_ids[1], "jobs"), 0);
    T_ASSERT(wait_flag_ge(&g_raw_push_n, 4, 500));
    T_ASSERT_EQ(g_raw_push_n, 4);

    T_ASSERT_EQ(raw_send_confirm(cons, g_raw_push_ids[2], "jobs"), 0);
    T_ASSERT_EQ(raw_send_confirm(cons, g_raw_push_ids[3], "jobs"), 0);
    T_ASSERT(wait_flag_ge(&g_raw_push_n, 5, 500));
    T_ASSERT_EQ(g_raw_push_n, 5);

    t_evloop_stop(loop);
    T_ASSERT_EQ(t_thread_join(&th), 0);
    t_conn_destroy(cons);
    t_client_destroy(prod);
    t_server_destroy(srv);
    t_broker_destroy(b);
    t_evloop_destroy(loop);
}

T_TEST(server_unknown_confirm_no_credit) {
    t_evloop *loop = t_evloop_create();
    t_broker *b = t_broker_create("n0");
    t_broker_start(b);
    t_server_config cfg;
    t_server_config_init(&cfg);
    cfg.port = 0;
    cfg.idle_timeout_ms = 0;
    cfg.push_credits = 1;
    t_server *srv = t_server_create(loop, b, &cfg);
    t_server_start(srv);
    uint16_t port = t_server_port(srv);

    t_thread th;
    T_ASSERT_EQ(t_thread_spawn(&th, loop_runner, loop), 0);
    t_time_sleep_us(20000);

    int cfd = t_socket_dial_ipv4("127.0.0.1", port);
    T_ASSERT(cfd >= 0);
    t_conn *cons = t_conn_create(cfd, loop);
    T_ASSERT_NOT_NULL(cons);
    g_raw_push_n = 0;
    g_raw_ack_n = 0;
    t_conn_set_on_msg(cons, raw_on_push, NULL);
    T_ASSERT_EQ(raw_send_open_consumer(cons, "jobs"), 0);
    T_ASSERT(wait_flag_ge(&g_raw_ack_n, 1, 500));
    T_ASSERT_EQ(g_raw_ack_status, 0);

    t_client *prod = t_client_create("p");
    T_ASSERT_EQ(t_client_dial(prod, loop, "127.0.0.1", port), 0);
    unsigned pseq = t_client_ack_seq(prod);
    T_ASSERT_EQ(t_client_open_queue(prod, "jobs", T_CLIENT_OPEN_PRODUCER), 0);
    T_ASSERT(wait_ack_status(prod, pseq, 0, 500));
    T_ASSERT_EQ(t_client_post(prod, "jobs", (const uint8_t *)"a", 1, 0), 0);
    T_ASSERT_EQ(t_client_post(prod, "jobs", (const uint8_t *)"b", 1, 0), 0);
    T_ASSERT(wait_flag_ge(&g_raw_push_n, 1, 500));
    T_ASSERT_EQ(g_raw_push_n, 1);

    int acks = g_raw_ack_n;
    T_ASSERT_EQ(raw_send_confirm(cons, 0xdeadbeefULL, "jobs"), 0);
    T_ASSERT(wait_flag_ge(&g_raw_ack_n, acks + 1, 500));
    T_ASSERT_EQ(g_raw_ack_status, (int)T_ERR_NOTFOUND);
    T_ASSERT_EQ(g_raw_push_n, 1);

    T_ASSERT_EQ(raw_send_confirm(cons, g_raw_push_ids[0], "jobs"), 0);
    T_ASSERT(wait_flag_ge(&g_raw_push_n, 2, 500));
    T_ASSERT_EQ(g_raw_push_n, 2);

    t_evloop_stop(loop);
    T_ASSERT_EQ(t_thread_join(&th), 0);
    t_conn_destroy(cons);
    t_client_destroy(prod);
    t_server_destroy(srv);
    t_broker_destroy(b);
    t_evloop_destroy(loop);
}

T_TEST(server_push_credits_auto_confirm) {
    t_evloop *loop = t_evloop_create();
    t_broker *b = t_broker_create("n0");
    t_broker_start(b);
    t_server_config cfg;
    t_server_config_init(&cfg);
    cfg.port = 0;
    cfg.idle_timeout_ms = 0;
    cfg.push_credits = 2;
    t_server *srv = t_server_create(loop, b, &cfg);
    t_server_start(srv);
    uint16_t port = t_server_port(srv);

    t_thread th;
    T_ASSERT_EQ(t_thread_spawn(&th, loop_runner, loop), 0);
    t_time_sleep_us(20000);

    t_client *prod = t_client_create("p");
    t_client *cons = t_client_create("c");
    T_ASSERT_EQ(t_client_dial(prod, loop, "127.0.0.1", port), 0);
    T_ASSERT_EQ(t_client_dial(cons, loop, "127.0.0.1", port), 0);
    unsigned cseq = t_client_ack_seq(cons);
    T_ASSERT_EQ(t_client_open_queue(cons, "jobs", T_CLIENT_OPEN_CONSUMER), 0);
    T_ASSERT(wait_ack_status(cons, cseq, 0, 500));
    T_ASSERT_EQ(t_client_subscribe(cons, "jobs", on_net_msg, NULL), 0);
    unsigned pseq = t_client_ack_seq(prod);
    T_ASSERT_EQ(t_client_open_queue(prod, "jobs", T_CLIENT_OPEN_PRODUCER), 0);
    T_ASSERT(wait_ack_status(prod, pseq, 0, 500));
    g_got = 0;
    const uint8_t x[] = "x";
    int i;
    for (i = 0; i < 5; i++)
        T_ASSERT_EQ(t_client_post(prod, "jobs", x, 1, 0), 0);
    T_ASSERT(wait_flag_ge(&g_got, 5, 500));
    T_ASSERT_EQ(g_got, 5);

    t_evloop_stop(loop);
    T_ASSERT_EQ(t_thread_join(&th), 0);
    t_client_destroy(prod);
    t_client_destroy(cons);
    t_server_destroy(srv);
    t_broker_destroy(b);
    t_evloop_destroy(loop);
}

static int raw_send_reject(t_conn *conn, uint64_t msg_id, const char *name) {
    uint8_t buf[8 + 2 + T_WIRE_MAX_NAME];
    int n = t_wire_encode_confirm(buf, sizeof(buf), msg_id, name);
    if (n < 0) return -1;
    t_proto_msg m;
    t_proto_header_init(&m.header, T_MSG_REJECT, (uint32_t)n);
    m.payload = buf;
    m.payload_len = (size_t)n;
    return t_conn_send(conn, &m);
}

T_TEST(server_confirm_acks_pull) {
    t_evloop *loop = t_evloop_create();
    t_broker *b = t_broker_create("n0");
    t_broker_start(b);
    t_server_config cfg;
    t_server_config_init(&cfg);
    cfg.port = 0;
    cfg.idle_timeout_ms = 0;
    t_server *srv = t_server_create(loop, b, &cfg);
    t_server_start(srv);
    uint16_t port = t_server_port(srv);

    t_thread th;
    T_ASSERT_EQ(t_thread_spawn(&th, loop_runner, loop), 0);
    t_time_sleep_us(20000);

    t_client *prod = t_client_create("p");
    t_client *cons = t_client_create("c");
    T_ASSERT_EQ(t_client_dial(prod, loop, "127.0.0.1", port), 0);
    T_ASSERT_EQ(t_client_dial(cons, loop, "127.0.0.1", port), 0);
    unsigned cseq = t_client_ack_seq(cons);
    T_ASSERT_EQ(t_client_open_queue(cons, "jobs", T_CLIENT_OPEN_CONSUMER), 0);
    T_ASSERT(wait_ack_status(cons, cseq, 0, 500));
    T_ASSERT_EQ(t_client_subscribe(cons, "jobs", on_net_msg, NULL), 0);
    unsigned pseq = t_client_ack_seq(prod);
    T_ASSERT_EQ(t_client_open_queue(prod, "jobs", T_CLIENT_OPEN_PRODUCER), 0);
    T_ASSERT(wait_ack_status(prod, pseq, 0, 500));
    g_got = 0;
    T_ASSERT_EQ(t_client_post(prod, "jobs", (const uint8_t *)"ack", 3, 0), 0);
    T_ASSERT(wait_flag_ge(&g_got, 1, 500));
    T_ASSERT_EQ(g_got, 1);
    t_domain *d = t_broker_get_domain(b, "default");
    t_queue *q = (t_queue *)t_domain_get_queue(d, "jobs");
    T_ASSERT_NOT_NULL(q);
    T_ASSERT_EQ((int)t_queue_pending_count(q), 0);
    T_ASSERT_EQ(t_queue_has_inflight(q), 0);

    t_evloop_stop(loop);
    T_ASSERT_EQ(t_thread_join(&th), 0);
    t_client_destroy(prod);
    t_client_destroy(cons);
    t_server_destroy(srv);
    t_broker_destroy(b);
    t_evloop_destroy(loop);
}

T_TEST(server_reject_requeues) {
    t_evloop *loop = t_evloop_create();
    t_broker *b = t_broker_create("n0");
    t_broker_start(b);
    t_server_config cfg;
    t_server_config_init(&cfg);
    cfg.port = 0;
    cfg.idle_timeout_ms = 0;
    cfg.push_credits = 1;
    t_server *srv = t_server_create(loop, b, &cfg);
    t_server_start(srv);
    uint16_t port = t_server_port(srv);

    t_thread th;
    T_ASSERT_EQ(t_thread_spawn(&th, loop_runner, loop), 0);
    t_time_sleep_us(20000);

    int cfd = t_socket_dial_ipv4("127.0.0.1", port);
    T_ASSERT(cfd >= 0);
    t_conn *cons = t_conn_create(cfd, loop);
    T_ASSERT_NOT_NULL(cons);
    g_raw_push_n = 0;
    t_conn_set_on_msg(cons, raw_on_push, NULL);
    T_ASSERT_EQ(raw_send_open_consumer(cons, "jobs"), 0);

    t_client *prod = t_client_create("p");
    T_ASSERT_EQ(t_client_dial(prod, loop, "127.0.0.1", port), 0);
    unsigned pseq = t_client_ack_seq(prod);
    T_ASSERT_EQ(t_client_open_queue(prod, "jobs", T_CLIENT_OPEN_PRODUCER), 0);
    T_ASSERT(wait_ack_status(prod, pseq, 0, 500));
    T_ASSERT_EQ(t_client_post(prod, "jobs", (const uint8_t *)"r", 1, 0), 0);
    T_ASSERT(wait_flag_ge(&g_raw_push_n, 1, 500));
    T_ASSERT_EQ(g_raw_push_n, 1);
    uint64_t first = g_raw_push_ids[0];
    T_ASSERT_EQ(raw_send_reject(cons, first, "jobs"), 0);
    T_ASSERT(wait_flag_ge(&g_raw_push_n, 2, 500));
    T_ASSERT_EQ(g_raw_push_n, 2);
    T_ASSERT_EQ((long long)g_raw_push_ids[1], (long long)first);

    t_evloop_stop(loop);
    T_ASSERT_EQ(t_thread_join(&th), 0);
    t_conn_destroy(cons);
    t_client_destroy(prod);
    t_server_destroy(srv);
    t_broker_destroy(b);
    t_evloop_destroy(loop);
}

T_TEST(server_unknown_reject_no_credit) {
    t_evloop *loop = t_evloop_create();
    t_broker *b = t_broker_create("n0");
    t_broker_start(b);
    t_server_config cfg;
    t_server_config_init(&cfg);
    cfg.port = 0;
    cfg.idle_timeout_ms = 0;
    cfg.push_credits = 1;
    t_server *srv = t_server_create(loop, b, &cfg);
    t_server_start(srv);
    uint16_t port = t_server_port(srv);

    t_thread th;
    T_ASSERT_EQ(t_thread_spawn(&th, loop_runner, loop), 0);
    t_time_sleep_us(20000);

    int cfd = t_socket_dial_ipv4("127.0.0.1", port);
    T_ASSERT(cfd >= 0);
    t_conn *cons = t_conn_create(cfd, loop);
    T_ASSERT_NOT_NULL(cons);
    g_raw_push_n = 0;
    g_raw_ack_n = 0;
    t_conn_set_on_msg(cons, raw_on_push, NULL);
    T_ASSERT_EQ(raw_send_open_consumer(cons, "jobs"), 0);
    T_ASSERT(wait_flag_ge(&g_raw_ack_n, 1, 500));
    T_ASSERT_EQ(g_raw_ack_status, 0);

    t_client *prod = t_client_create("p");
    T_ASSERT_EQ(t_client_dial(prod, loop, "127.0.0.1", port), 0);
    unsigned pseq = t_client_ack_seq(prod);
    T_ASSERT_EQ(t_client_open_queue(prod, "jobs", T_CLIENT_OPEN_PRODUCER), 0);
    T_ASSERT(wait_ack_status(prod, pseq, 0, 500));
    T_ASSERT_EQ(t_client_post(prod, "jobs", (const uint8_t *)"a", 1, 0), 0);
    T_ASSERT_EQ(t_client_post(prod, "jobs", (const uint8_t *)"b", 1, 0), 0);
    T_ASSERT(wait_flag_ge(&g_raw_push_n, 1, 500));
    T_ASSERT_EQ(g_raw_push_n, 1);

    int acks = g_raw_ack_n;
    T_ASSERT_EQ(raw_send_reject(cons, 0xdeadbeefULL, "jobs"), 0);
    T_ASSERT(wait_flag_ge(&g_raw_ack_n, acks + 1, 500));
    T_ASSERT_EQ(g_raw_ack_status, (int)T_ERR_NOTFOUND);
    T_ASSERT_EQ(g_raw_push_n, 1);

    T_ASSERT_EQ(raw_send_reject(cons, g_raw_push_ids[0], "jobs"), 0);
    T_ASSERT(wait_flag_ge(&g_raw_push_n, 2, 500));
    T_ASSERT_EQ(g_raw_push_n, 2);
    T_ASSERT_EQ((long long)g_raw_push_ids[1], (long long)g_raw_push_ids[0]);

    t_evloop_stop(loop);
    T_ASSERT_EQ(t_thread_join(&th), 0);
    t_conn_destroy(cons);
    t_client_destroy(prod);
    t_server_destroy(srv);
    t_broker_destroy(b);
    t_evloop_destroy(loop);
}

T_TEST(server_unknown_confirm_broadcast_credits) {
    t_evloop *loop = t_evloop_create();
    t_broker *b = t_broker_create("n0");
    t_broker_start(b);
    t_server_config cfg;
    t_server_config_init(&cfg);
    cfg.port = 0;
    cfg.idle_timeout_ms = 0;
    cfg.push_credits = 1;
    t_server *srv = t_server_create(loop, b, &cfg);
    t_server_start(srv);
    uint16_t port = t_server_port(srv);

    t_thread th;
    T_ASSERT_EQ(t_thread_spawn(&th, loop_runner, loop), 0);
    t_time_sleep_us(20000);

    int cfd = t_socket_dial_ipv4("127.0.0.1", port);
    T_ASSERT(cfd >= 0);
    t_conn *cons = t_conn_create(cfd, loop);
    T_ASSERT_NOT_NULL(cons);
    g_raw_push_n = 0;
    g_raw_ack_n = 0;
    t_conn_set_on_msg(cons, raw_on_push, NULL);
    T_ASSERT_EQ(raw_send_open_consumer_type(cons, "bc.q", (uint8_t)T_QUEUE_BROADCAST), 0);
    T_ASSERT(wait_flag_ge(&g_raw_ack_n, 1, 500));
    T_ASSERT_EQ(g_raw_ack_status, 0);

    t_client *prod = t_client_create("p");
    T_ASSERT_EQ(t_client_dial(prod, loop, "127.0.0.1", port), 0);
    T_ASSERT_EQ(t_client_open_follow(prod, "bc.q", T_CLIENT_OPEN_PRODUCER, 500), 0);
    T_ASSERT_EQ(t_client_post(prod, "bc.q", (const uint8_t *)"a", 1, 0), 0);
    T_ASSERT(wait_flag_ge(&g_raw_push_n, 1, 500));
    T_ASSERT_EQ(g_raw_push_n, 1);

    int acks = g_raw_ack_n;
    T_ASSERT_EQ(raw_send_confirm(cons, 0xdeadbeefULL, "bc.q"), 0);
    T_ASSERT(wait_flag_ge(&g_raw_ack_n, acks + 1, 500));
    T_ASSERT_EQ(g_raw_ack_status, 0);

    T_ASSERT_EQ(t_client_post(prod, "bc.q", (const uint8_t *)"b", 1, 0), 0);
    T_ASSERT(wait_flag_ge(&g_raw_push_n, 2, 500));
    T_ASSERT_EQ(g_raw_push_n, 2);

    t_evloop_stop(loop);
    T_ASSERT_EQ(t_thread_join(&th), 0);
    t_conn_destroy(cons);
    t_client_destroy(prod);
    t_server_destroy(srv);
    t_broker_destroy(b);
    t_evloop_destroy(loop);
}

T_TEST(server_disconnect_requeues) {
    t_evloop *loop = t_evloop_create();
    t_broker *b = t_broker_create("n0");
    t_broker_start(b);
    t_server_config cfg;
    t_server_config_init(&cfg);
    cfg.port = 0;
    cfg.idle_timeout_ms = 0;
    t_server *srv = t_server_create(loop, b, &cfg);
    t_server_start(srv);
    uint16_t port = t_server_port(srv);

    t_thread th;
    T_ASSERT_EQ(t_thread_spawn(&th, loop_runner, loop), 0);
    t_time_sleep_us(20000);

    int cfd = t_socket_dial_ipv4("127.0.0.1", port);
    T_ASSERT(cfd >= 0);
    t_conn *raw = t_conn_create(cfd, loop);
    T_ASSERT_NOT_NULL(raw);
    g_raw_push_n = 0;
    t_conn_set_on_msg(raw, raw_on_push, NULL);
    T_ASSERT_EQ(raw_send_open_consumer(raw, "jobs"), 0);

    t_client *prod = t_client_create("p");
    T_ASSERT_EQ(t_client_dial(prod, loop, "127.0.0.1", port), 0);
    unsigned pseq = t_client_ack_seq(prod);
    T_ASSERT_EQ(t_client_open_queue(prod, "jobs", T_CLIENT_OPEN_PRODUCER), 0);
    T_ASSERT(wait_ack_status(prod, pseq, 0, 500));
    T_ASSERT_EQ(t_client_post(prod, "jobs", (const uint8_t *)"keep", 4, 0), 0);
    T_ASSERT(wait_flag_ge(&g_raw_push_n, 1, 500));
    T_ASSERT_EQ(g_raw_push_n, 1);
    t_conn_destroy(raw);

    g_got = 0;
    g_payload[0] = 0;
    t_client *cons = t_client_create("c");
    T_ASSERT_EQ(t_client_dial(cons, loop, "127.0.0.1", port), 0);
    unsigned cseq = t_client_ack_seq(cons);
    T_ASSERT_EQ(t_client_subscribe(cons, "jobs", on_net_msg, NULL), 0);
    T_ASSERT(wait_ack_status(cons, cseq, 0, 500));
    T_ASSERT(wait_flag_ge(&g_got, 1, 500));
    T_ASSERT_EQ(g_got, 1);
    T_ASSERT_STR_EQ(g_payload, "keep");

    t_evloop_stop(loop);
    T_ASSERT_EQ(t_thread_join(&th), 0);
    t_client_destroy(prod);
    t_client_destroy(cons);
    t_server_destroy(srv);
    t_broker_destroy(b);
    t_evloop_destroy(loop);
}

T_TEST(server_join_requires_consumer_open) {
    t_evloop *loop = t_evloop_create();
    t_broker *b = t_broker_create("n0");
    t_broker_start(b);
    t_server_config cfg;
    t_server_config_init(&cfg);
    cfg.port = 0;
    cfg.idle_timeout_ms = 0;
    t_server *srv = t_server_create(loop, b, &cfg);
    t_server_start(srv);
    uint16_t port = t_server_port(srv);

    t_thread th;
    T_ASSERT_EQ(t_thread_spawn(&th, loop_runner, loop), 0);
    t_time_sleep_us(20000);

    t_client *c = t_client_create("c");
    T_ASSERT_EQ(t_client_dial(c, loop, "127.0.0.1", port), 0);
    unsigned seq = t_client_ack_seq(c);
    T_ASSERT_EQ(t_client_join(c, "workers", "c1", "jobs"), 0);
    T_ASSERT(wait_ack_status(c, seq, (int)T_ERR_PERMISSION, 500));

    t_evloop_stop(loop);
    T_ASSERT_EQ(t_thread_join(&th), 0);
    t_client_destroy(c);
    t_server_destroy(srv);
    t_broker_destroy(b);
    t_evloop_destroy(loop);
}

T_TEST(client_join_stub_fails) {
    t_client *c = t_client_create("stub");
    T_ASSERT_EQ(t_client_connect(c, "127.0.0.1", 1), 0);
    T_ASSERT(t_client_join(c, "g", "c1", "q") != 0);
    T_ASSERT(t_client_join_follow(c, "g", "c1", "q", 50) != 0);
    t_client_destroy(c);
}

static volatile int g_got_a;
static volatile int g_got_b;

static void on_net_a(const char *queue_name, const uint8_t *data, size_t len, void *ud) {
    (void)queue_name;
    (void)data;
    (void)len;
    (void)ud;
    g_got_a++;
}

static void on_net_b(const char *queue_name, const uint8_t *data, size_t len, void *ud) {
    (void)queue_name;
    (void)data;
    (void)len;
    (void)ud;
    g_got_b++;
}

T_TEST(server_join_round_robin) {
    t_evloop *loop = t_evloop_create();
    t_broker *b = t_broker_create("n0");
    t_broker_start(b);
    t_server_config cfg;
    t_server_config_init(&cfg);
    cfg.port = 0;
    cfg.idle_timeout_ms = 0;
    t_server *srv = t_server_create(loop, b, &cfg);
    t_server_start(srv);
    uint16_t port = t_server_port(srv);

    t_thread th;
    T_ASSERT_EQ(t_thread_spawn(&th, loop_runner, loop), 0);
    t_time_sleep_us(20000);

    t_client *prod = t_client_create("p");
    t_client *a = t_client_create("a");
    t_client *bcli = t_client_create("b");
    T_ASSERT_EQ(t_client_dial(prod, loop, "127.0.0.1", port), 0);
    T_ASSERT_EQ(t_client_dial(a, loop, "127.0.0.1", port), 0);
    T_ASSERT_EQ(t_client_dial(bcli, loop, "127.0.0.1", port), 0);

    unsigned aseq = t_client_ack_seq(a);
    T_ASSERT_EQ(t_client_open_queue(a, "jobs", T_CLIENT_OPEN_CONSUMER), 0);
    T_ASSERT(wait_ack_status(a, aseq, 0, 500));
    T_ASSERT_EQ(t_client_subscribe(a, "jobs", on_net_a, NULL), 0);
    aseq = t_client_ack_seq(a);
    T_ASSERT_EQ(t_client_join(a, "workers", "ca", "jobs"), 0);
    T_ASSERT(wait_ack_status(a, aseq, 0, 500));

    unsigned bseq = t_client_ack_seq(bcli);
    T_ASSERT_EQ(t_client_open_queue(bcli, "jobs", T_CLIENT_OPEN_CONSUMER), 0);
    T_ASSERT(wait_ack_status(bcli, bseq, 0, 500));
    T_ASSERT_EQ(t_client_subscribe(bcli, "jobs", on_net_b, NULL), 0);
    bseq = t_client_ack_seq(bcli);
    T_ASSERT_EQ(t_client_join(bcli, "workers", "cb", "jobs"), 0);
    T_ASSERT(wait_ack_status(bcli, bseq, 0, 500));

    unsigned pseq = t_client_ack_seq(prod);
    T_ASSERT_EQ(t_client_open_queue(prod, "jobs", T_CLIENT_OPEN_PRODUCER), 0);
    T_ASSERT(wait_ack_status(prod, pseq, 0, 500));

    g_got_a = 0;
    g_got_b = 0;
    const uint8_t x[] = "x";
    T_ASSERT_EQ(t_client_post(prod, "jobs", x, 1, 0), 0);
    T_ASSERT_EQ(t_client_post(prod, "jobs", x, 1, 0), 0);
    {
        int64_t start = t_time_now_ms();
        while ((g_got_a + g_got_b) < 2 && t_time_now_ms() - start < 500)
            t_time_sleep_ms(5);
    }
    T_ASSERT_EQ(g_got_a, 1);
    T_ASSERT_EQ(g_got_b, 1);

    t_evloop_stop(loop);
    T_ASSERT_EQ(t_thread_join(&th), 0);
    t_client_destroy(prod);
    t_client_destroy(a);
    t_client_destroy(bcli);
    t_server_destroy(srv);
    t_broker_destroy(b);
    t_evloop_destroy(loop);
}

T_TEST(server_join_duplicate_consumer) {
    t_evloop *loop = t_evloop_create();
    t_broker *b = t_broker_create("n0");
    t_broker_start(b);
    t_server_config cfg;
    t_server_config_init(&cfg);
    cfg.port = 0;
    cfg.idle_timeout_ms = 0;
    t_server *srv = t_server_create(loop, b, &cfg);
    t_server_start(srv);
    uint16_t port = t_server_port(srv);

    t_thread th;
    T_ASSERT_EQ(t_thread_spawn(&th, loop_runner, loop), 0);
    t_time_sleep_us(20000);

    t_client *a = t_client_create("a");
    t_client *bcli = t_client_create("b");
    T_ASSERT_EQ(t_client_dial(a, loop, "127.0.0.1", port), 0);
    T_ASSERT_EQ(t_client_dial(bcli, loop, "127.0.0.1", port), 0);
    unsigned aseq = t_client_ack_seq(a);
    T_ASSERT_EQ(t_client_open_queue(a, "jobs", T_CLIENT_OPEN_CONSUMER), 0);
    T_ASSERT(wait_ack_status(a, aseq, 0, 500));
    aseq = t_client_ack_seq(a);
    T_ASSERT_EQ(t_client_join(a, "workers", "same", "jobs"), 0);
    T_ASSERT(wait_ack_status(a, aseq, 0, 500));
    aseq = t_client_ack_seq(a);
    T_ASSERT_EQ(t_client_join(a, "workers", "same", "jobs"), 0);
    T_ASSERT(wait_ack_status(a, aseq, 0, 500));

    unsigned bseq = t_client_ack_seq(bcli);
    T_ASSERT_EQ(t_client_open_queue(bcli, "jobs", T_CLIENT_OPEN_CONSUMER), 0);
    T_ASSERT(wait_ack_status(bcli, bseq, 0, 500));
    bseq = t_client_ack_seq(bcli);
    T_ASSERT_EQ(t_client_join(bcli, "workers", "same", "jobs"), 0);
    T_ASSERT(wait_ack_status(bcli, bseq, (int)T_ERR_EXISTS, 500));
    bseq = t_client_ack_seq(bcli);
    T_ASSERT_EQ(t_client_join(bcli, "other", "cb", "jobs"), 0);
    T_ASSERT(wait_ack_status(bcli, bseq, (int)T_ERR_BUSY, 500));

    t_evloop_stop(loop);
    T_ASSERT_EQ(t_thread_join(&th), 0);
    t_client_destroy(a);
    t_client_destroy(bcli);
    t_server_destroy(srv);
    t_broker_destroy(b);
    t_evloop_destroy(loop);
}

T_TEST(server_join_empty_holds) {
    t_evloop *loop = t_evloop_create();
    t_broker *b = t_broker_create("n0");
    t_broker_start(b);
    t_server_config cfg;
    t_server_config_init(&cfg);
    cfg.port = 0;
    cfg.idle_timeout_ms = 0;
    t_server *srv = t_server_create(loop, b, &cfg);
    t_server_start(srv);
    uint16_t port = t_server_port(srv);

    t_thread th;
    T_ASSERT_EQ(t_thread_spawn(&th, loop_runner, loop), 0);
    t_time_sleep_us(20000);

    t_client *prod = t_client_create("p");
    t_client *member = t_client_create("m");
    t_client *outsider = t_client_create("o");
    T_ASSERT_EQ(t_client_dial(prod, loop, "127.0.0.1", port), 0);
    T_ASSERT_EQ(t_client_dial(member, loop, "127.0.0.1", port), 0);
    T_ASSERT_EQ(t_client_dial(outsider, loop, "127.0.0.1", port), 0);

    unsigned pseq = t_client_ack_seq(prod);
    T_ASSERT_EQ(t_client_open_queue(prod, "jobs", T_CLIENT_OPEN_PRODUCER), 0);
    T_ASSERT(wait_ack_status(prod, pseq, 0, 500));

    unsigned mseq = t_client_ack_seq(member);
    T_ASSERT_EQ(t_client_open_queue(member, "jobs", T_CLIENT_OPEN_CONSUMER), 0);
    T_ASSERT(wait_ack_status(member, mseq, 0, 500));
    mseq = t_client_ack_seq(member);
    T_ASSERT_EQ(t_client_join(member, "workers", "m1", "jobs"), 0);
    T_ASSERT(wait_ack_status(member, mseq, 0, 500));
    T_ASSERT_EQ(t_client_disconnect(member), 0);

    unsigned oseq = t_client_ack_seq(outsider);
    T_ASSERT_EQ(t_client_open_queue(outsider, "jobs", T_CLIENT_OPEN_CONSUMER), 0);
    T_ASSERT(wait_ack_status(outsider, oseq, 0, 500));
    T_ASSERT_EQ(t_client_subscribe(outsider, "jobs", on_net_msg, NULL), 0);

    g_got = 0;
    T_ASSERT_EQ(t_client_post(prod, "jobs", (const uint8_t *)"hold", 4, 0), 0);
    t_time_sleep_ms(80);
    T_ASSERT_EQ(g_got, 0);
    t_domain *d = t_broker_get_domain(b, "default");
    t_queue *q = (t_queue *)t_domain_get_queue(d, "jobs");
    T_ASSERT_NOT_NULL(q);
    T_ASSERT_EQ((int)t_queue_pending_count(q), 1);

    t_evloop_stop(loop);
    T_ASSERT_EQ(t_thread_join(&th), 0);
    t_client_destroy(prod);
    t_client_destroy(member);
    t_client_destroy(outsider);
    t_server_destroy(srv);
    t_broker_destroy(b);
    t_evloop_destroy(loop);
}

T_TEST(server_join_survives_last_close) {
    t_evloop *loop = t_evloop_create();
    t_broker *b = t_broker_create("n0");
    t_broker_start(b);
    t_server_config cfg;
    t_server_config_init(&cfg);
    cfg.port = 0;
    cfg.idle_timeout_ms = 0;
    t_server *srv = t_server_create(loop, b, &cfg);
    t_server_start(srv);
    uint16_t port = t_server_port(srv);

    t_thread th;
    T_ASSERT_EQ(t_thread_spawn(&th, loop_runner, loop), 0);
    t_time_sleep_us(20000);

    t_client *prod = t_client_create("p");
    t_client *member = t_client_create("m");
    T_ASSERT_EQ(t_client_dial(prod, loop, "127.0.0.1", port), 0);
    T_ASSERT_EQ(t_client_dial(member, loop, "127.0.0.1", port), 0);

    unsigned pseq = t_client_ack_seq(prod);
    T_ASSERT_EQ(t_client_open_queue(prod, "jobs", T_CLIENT_OPEN_PRODUCER), 0);
    T_ASSERT(wait_ack_status(prod, pseq, 0, 500));
    unsigned mseq = t_client_ack_seq(member);
    T_ASSERT_EQ(t_client_open_queue(member, "jobs", T_CLIENT_OPEN_CONSUMER), 0);
    T_ASSERT(wait_ack_status(member, mseq, 0, 500));
    mseq = t_client_ack_seq(member);
    T_ASSERT_EQ(t_client_join(member, "workers", "m1", "jobs"), 0);
    T_ASSERT(wait_ack_status(member, mseq, 0, 500));
    T_ASSERT_EQ(t_client_disconnect(member), 0);
    T_ASSERT_EQ(t_client_disconnect(prod), 0);
    t_time_sleep_ms(40);

    t_client *outsider = t_client_create("o");
    t_client *prod2 = t_client_create("p2");
    T_ASSERT_EQ(t_client_dial(outsider, loop, "127.0.0.1", port), 0);
    T_ASSERT_EQ(t_client_dial(prod2, loop, "127.0.0.1", port), 0);
    unsigned oseq = t_client_ack_seq(outsider);
    T_ASSERT_EQ(t_client_open_queue(outsider, "jobs", T_CLIENT_OPEN_CONSUMER), 0);
    T_ASSERT(wait_ack_status(outsider, oseq, 0, 500));
    T_ASSERT_EQ(t_client_subscribe(outsider, "jobs", on_net_msg, NULL), 0);
    oseq = t_client_ack_seq(outsider);
    T_ASSERT_EQ(t_client_join(outsider, "other", "o1", "jobs"), 0);
    T_ASSERT(wait_ack_status(outsider, oseq, (int)T_ERR_BUSY, 500));

    unsigned p2seq = t_client_ack_seq(prod2);
    T_ASSERT_EQ(t_client_open_queue(prod2, "jobs", T_CLIENT_OPEN_PRODUCER), 0);
    T_ASSERT(wait_ack_status(prod2, p2seq, 0, 500));
    g_got = 0;
    T_ASSERT_EQ(t_client_post(prod2, "jobs", (const uint8_t *)"hold", 4, 0), 0);
    t_time_sleep_ms(80);
    T_ASSERT_EQ(g_got, 0);
    t_domain *d = t_broker_get_domain(b, "default");
    t_queue *q = (t_queue *)t_domain_get_queue(d, "jobs");
    T_ASSERT_NOT_NULL(q);
    T_ASSERT(strcmp(t_queue_group(q), "workers") == 0);
    T_ASSERT_EQ((int)t_queue_pending_count(q), 1);

    t_client *rejoin = t_client_create("r");
    T_ASSERT_EQ(t_client_dial(rejoin, loop, "127.0.0.1", port), 0);
    unsigned rseq = t_client_ack_seq(rejoin);
    T_ASSERT_EQ(t_client_open_queue(rejoin, "jobs", T_CLIENT_OPEN_CONSUMER), 0);
    T_ASSERT(wait_ack_status(rejoin, rseq, 0, 500));
    T_ASSERT_EQ(t_client_subscribe(rejoin, "jobs", on_net_msg, NULL), 0);
    rseq = t_client_ack_seq(rejoin);
    T_ASSERT_EQ(t_client_join(rejoin, "workers", "m2", "jobs"), 0);
    T_ASSERT(wait_ack_status(rejoin, rseq, 0, 500));
    T_ASSERT(wait_flag_ge(&g_got, 1, 500));

    t_evloop_stop(loop);
    T_ASSERT_EQ(t_thread_join(&th), 0);
    t_client_destroy(prod);
    t_client_destroy(member);
    t_client_destroy(outsider);
    t_client_destroy(prod2);
    t_client_destroy(rejoin);
    t_server_destroy(srv);
    t_broker_destroy(b);
    t_evloop_destroy(loop);
}

int main(void) {
    return t_run_all_tests();
}
