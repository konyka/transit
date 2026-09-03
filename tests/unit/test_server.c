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
#include <pthread.h>
#include <unistd.h>
#include <string.h>
#include <stdint.h>

static void timer_stop(void *ud) {
    t_evloop_stop((t_evloop *)ud);
}

static void *loop_runner(void *arg) {
    t_evloop *loop = (t_evloop *)arg;
    t_evloop_timer_add(loop, 400, 0, timer_stop, loop);
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

    pthread_t th;
    pthread_create(&th, NULL, loop_runner, loop);
    usleep(20000);

    t_client *c1 = t_client_create("a");
    t_client *c2 = t_client_create("b");
    T_ASSERT_EQ(t_client_dial(c1, loop, "127.0.0.1", port), 0);
    usleep(30000);
    T_ASSERT_EQ(t_client_dial(c2, loop, "127.0.0.1", port), 0);
    usleep(50000);
    T_ASSERT_EQ((int)t_server_conn_count(srv), 1);
    T_ASSERT(t_server_dropped_conns(srv) >= 1);

    t_evloop_stop(loop);
    pthread_join(th, NULL);
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

    pthread_t th;
    pthread_create(&th, NULL, loop_runner, loop);
    usleep(20000);

    t_client *c = t_client_create("c");
    T_ASSERT_EQ(t_client_dial(c, loop, "127.0.0.1", port), 0);
    /* Force a POST without OPEN by using stub-style local queue then net send:
     * open locally is required by API, so open then the server checks producer
     * mode. Use a raw socket-level check: open_queue sends OPEN; instead
     * subscribe-only consumer then post should be permission denied. */
    T_ASSERT_EQ(t_client_open_queue(c, "only.consume", T_CLIENT_OPEN_CONSUMER), 0);
    usleep(30000);
    T_ASSERT_EQ(t_client_post(c, "only.consume", (const uint8_t *)"x", 1, 0), 0);
    usleep(30000);
    T_ASSERT_EQ(t_client_last_status(c), (int)T_ERR_PERMISSION);

    t_evloop_stop(loop);
    pthread_join(th, NULL);
    t_client_destroy(c);
    t_server_destroy(srv);
    t_broker_destroy(b);
    t_evloop_destroy(loop);
}

static int g_got;
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

    pthread_t th;
    pthread_create(&th, NULL, loop_runner, loop);
    usleep(20000);

    t_client *prod = t_client_create("p");
    t_client *cons = t_client_create("c");
    T_ASSERT_EQ(t_client_dial(prod, loop, "127.0.0.1", port), 0);
    T_ASSERT_EQ(t_client_dial(cons, loop, "127.0.0.1", port), 0);
    T_ASSERT_EQ(t_client_open_queue(cons, "jobs", T_CLIENT_OPEN_CONSUMER), 0);
    T_ASSERT_EQ(t_client_subscribe(cons, "jobs", on_net_msg, NULL), 0);
    T_ASSERT_EQ(t_client_open_queue(prod, "jobs", T_CLIENT_OPEN_PRODUCER), 0);
    usleep(40000);
    g_got = 0;
    g_payload[0] = 0;
    T_ASSERT_EQ(t_client_post(prod, "jobs", (const uint8_t *)"hello", 5, 0), 0);
    usleep(60000);
    T_ASSERT(g_got >= 1);
    T_ASSERT_STR_EQ(g_payload, "hello");
    T_ASSERT_EQ(t_client_last_status(prod), 0);

    t_evloop_stop(loop);
    pthread_join(th, NULL);
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

    pthread_t th;
    pthread_create(&th, NULL, loop_runner, loop);
    usleep(20000);

    t_client *c = t_client_create("rl");
    T_ASSERT_EQ(t_client_dial(c, loop, "127.0.0.1", port), 0);
    T_ASSERT_EQ(t_client_open_queue(c, "rl.q", T_CLIENT_OPEN_PRODUCER), 0);
    usleep(30000);
    T_ASSERT_EQ(t_client_post(c, "rl.q", (const uint8_t *)"a", 1, 0), 0);
    usleep(30000);
    T_ASSERT_EQ(t_client_last_status(c), (int)T_ERR_BUSY);
    T_ASSERT(t_server_msgs_dropped(srv) >= 1);

    t_evloop_stop(loop);
    pthread_join(th, NULL);
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

    pthread_t th;
    pthread_create(&th, NULL, loop_runner, loop);
    usleep(20000);

    t_client *c = t_client_create("d");
    T_ASSERT_EQ(t_client_dial(c, loop, "127.0.0.1", port), 0);
    T_ASSERT_EQ(t_client_open_queue(c, "dur.q",
                                    T_CLIENT_OPEN_PRODUCER | T_CLIENT_QFLAG_DURABLE), 0);
    usleep(30000);
    T_ASSERT_EQ(t_client_last_status(c), (int)T_ERR_IO);

    t_evloop_stop(loop);
    pthread_join(th, NULL);
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

    pthread_t th;
    pthread_create(&th, NULL, loop_runner, loop);
    usleep(20000);

    t_client *c1 = t_client_create("ex1");
    t_client *c2 = t_client_create("ex2");
    T_ASSERT_EQ(t_client_dial(c1, loop, "127.0.0.1", port), 0);
    T_ASSERT_EQ(t_client_dial(c2, loop, "127.0.0.1", port), 0);
    T_ASSERT_EQ(t_client_open_queue(c1, "ex.q",
                                    T_CLIENT_OPEN_CONSUMER | T_CLIENT_QFLAG_EXCLUSIVE), 0);
    usleep(30000);
    T_ASSERT_EQ(t_client_last_status(c1), 0);
    T_ASSERT_EQ(t_client_open_queue(c2, "ex.q",
                                    T_CLIENT_OPEN_CONSUMER | T_CLIENT_QFLAG_EXCLUSIVE), 0);
    usleep(30000);
    T_ASSERT_EQ(t_client_last_status(c2), (int)T_ERR_BUSY);

    t_evloop_stop(loop);
    pthread_join(th, NULL);
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

    pthread_t th;
    pthread_create(&th, NULL, loop_runner, loop);
    usleep(20000);

    t_client *c = t_client_create("ad");
    T_ASSERT_EQ(t_client_dial(c, loop, "127.0.0.1", port), 0);
    T_ASSERT_EQ(t_client_open_queue(c, "tmp.q",
                                    T_CLIENT_OPEN_PRODUCER | T_CLIENT_QFLAG_AUTODELETE), 0);
    usleep(30000);
    T_ASSERT_EQ(t_client_last_status(c), 0);
    t_domain *d = t_broker_get_domain(b, "default");
    T_ASSERT_NOT_NULL(t_domain_get_queue(d, "tmp.q"));
    T_ASSERT_EQ(t_client_close_queue(c, "tmp.q"), 0);
    usleep(30000);
    T_ASSERT_NULL(t_domain_get_queue(d, "tmp.q"));

    t_evloop_stop(loop);
    pthread_join(th, NULL);
    t_client_destroy(c);
    t_server_destroy(srv);
    t_broker_destroy(b);
    t_evloop_destroy(loop);
}

T_TEST(server_follower_post_redirect_hint) {
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

    pthread_t th;
    pthread_create(&th, NULL, loop_runner, loop);
    usleep(20000);

    t_client *c = t_client_create("c");
    T_ASSERT_EQ(t_client_dial(c, loop, "127.0.0.1", port), 0);
    T_ASSERT_EQ(t_client_open_queue(c, "jobs", T_CLIENT_OPEN_PRODUCER), 0);
    usleep(30000);
    T_ASSERT_EQ(t_client_post(c, "jobs", (const uint8_t *)"x", 1, 0), 0);
    usleep(30000);
    T_ASSERT_EQ(t_client_last_status(c), (int)T_ERR_AGAIN);
    T_ASSERT_STR_EQ(t_client_last_ack_name(c), "127.0.0.1_9999");

    t_evloop_stop(loop);
    pthread_join(th, NULL);
    t_client_destroy(c);
    t_server_destroy(srv);
    t_broker_destroy(b);
    t_cluster_destroy(cl);
    t_evloop_destroy(loop);
}

int main(void) {
    return t_run_all_tests();
}
