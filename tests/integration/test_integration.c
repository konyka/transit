#include "t_test.h"
#include "t_broker.h"
#include "t_queue.h"
#include "t_router.h"
#include "t_cluster.h"
#include "t_raft.h"
#include "t_node.h"
#include "t_proto.h"
#include "t_storage.h"
#include "t_coro.h"
#include "t_tpool.h"
#include <string.h>
#include <stdlib.h>

static void q_count_cb(const t_msg *msg, void *ud) {
    (void)msg;
    int *cnt = (int *)ud;
    if (cnt) (*cnt)++;
}

static void broker_count_cb(const char *queue_name, const uint8_t *data, size_t len, void *ud) {
    (void)queue_name; (void)data; (void)len;
    int *cnt = (int *)ud;
    if (cnt) (*cnt)++;
}

static void coro_inc_fn(void *arg) {
    int *cnt = (int *)arg;
    (*cnt)++;
    t_coro_yield();
    (*cnt)++;
    t_coro_yield();
    (*cnt)++;
}

static void tpool_inc_fn(void *arg) {
    int *p = (int *)arg;
    __sync_add_and_fetch(p, 1);
}

T_TEST(integration_broker_broadcast) {
    t_broker *b = t_broker_create("int-broker");
    T_ASSERT_NOT_NULL(b);
    T_ASSERT_EQ(t_broker_start(b), 0);
    T_ASSERT_EQ(t_broker_create_queue(b, "default", "bcst.q", T_QUEUE_BROADCAST, 0), 0);

    int d0 = 0, d1 = 0, d2 = 0;
    t_broker_subscribe(b, "bcst.q", broker_count_cb, &d0);
    t_broker_subscribe(b, "bcst.q", broker_count_cb, &d1);
    t_broker_subscribe(b, "bcst.q", broker_count_cb, &d2);

    uint8_t msg[] = "hello";
    T_ASSERT_EQ(t_broker_publish(b, "bcst.q", msg, 5, 0), 0);
    T_ASSERT_EQ(d0, 1);
    T_ASSERT_EQ(d1, 1);
    T_ASSERT_EQ(d2, 1);
    T_ASSERT_EQ((int)t_broker_total_messages(b), 1);

    t_broker_stop(b);
    t_broker_destroy(b);
}

T_TEST(integration_router_queue) {
    t_router *r = t_router_create();
    t_queue *q1 = t_queue_create("prices", T_QUEUE_FIFO, 0);
    t_queue *q2 = t_queue_create("alerts", T_QUEUE_FIFO, 0);
    T_ASSERT_NOT_NULL(r);
    T_ASSERT_NOT_NULL(q1);
    T_ASSERT_NOT_NULL(q2);

    int c1 = 0, c2 = 0;
    t_queue_add_consumer(q1, q_count_cb, &c1);
    t_queue_add_consumer(q2, q_count_cb, &c2);

    t_router_bind(r, "stock.prices.*", q1);
    t_router_bind(r, "alerts.#", q2);

    void *targets[8];
    size_t n;

    n = t_router_route(r, "stock.prices.nasdaq", targets, 8);
    T_ASSERT_EQ((int)n, 1);
    uint8_t d1[] = {1};
    t_queue_post((t_queue *)targets[0], d1, sizeof(d1), 0);
    T_ASSERT_EQ(c1, 1);

    n = t_router_route(r, "alerts.critical.system", targets, 8);
    T_ASSERT_EQ((int)n, 1);
    uint8_t d2[] = {9};
    t_queue_post((t_queue *)targets[0], d2, sizeof(d2), 0);
    T_ASSERT_EQ(c2, 1);

    t_queue_destroy(q1);
    t_queue_destroy(q2);
    t_router_destroy(r);
}

T_TEST(integration_cluster_raft) {
    t_cluster *c = t_cluster_create(1);
    T_ASSERT_NOT_NULL(c);
    t_cluster_add_node(c, 1, "127.0.0.1", 9999);
    t_cluster_add_node(c, 2, "127.0.0.2", 1111);
    t_cluster_add_node(c, 3, "127.0.0.3", 2222);
    T_ASSERT_EQ((int)t_cluster_node_count(c), 3);

    t_raft_config cfg = {1, 150, 50};
    t_raft *raft = t_raft_create(&cfg);
    T_ASSERT_NOT_NULL(raft);

    t_raft_become_candidate(raft);
    T_ASSERT_EQ((int)t_raft_current_term(raft), 1);
    T_ASSERT_EQ((int)t_raft_voted_for(raft), 1);

    t_raft_become_leader(raft);
    T_ASSERT_EQ((int)t_raft_state(raft), T_NODE_LEADER);

    t_cluster_set_leader(c, 1);
    T_ASSERT(t_cluster_is_leader(c));

    uint8_t data[] = "cluster-entry";
    t_raft_append_entry(raft, 2, data, sizeof(data));
    t_raft_advance_commit(raft, 1);
    t_raft_apply_entries(raft);
    T_ASSERT_EQ((int)t_raft_log_count(raft), 1);
    T_ASSERT_EQ((int)t_raft_applied_count(raft), 1);

    t_raft_destroy(raft);
    t_cluster_destroy(c);
}

T_TEST(integration_proto_storage) {
    uint8_t buf[T_PROTO_HEADER_SIZE];
    t_proto_header hdr;
    t_proto_header_init(&hdr, T_MSG_POST, 100);
    T_ASSERT_EQ(t_proto_header_encode(&hdr, buf, sizeof(buf)), 0);

    t_proto_header dec;
    T_ASSERT_EQ(t_proto_header_decode(&dec, buf, sizeof(buf)), 0);
    T_ASSERT_EQ((int)dec.type, T_MSG_POST);
    T_ASSERT_EQ((int)dec.payload_len, 100);

    t_storage *s = t_storage_create(T_STORAGE_MEM, NULL);
    T_ASSERT_NOT_NULL(s);

    uint8_t payload[] = {0xCA, 0xFE, 0xBA, 0xBE};
    T_ASSERT_EQ(t_storage_put(s, 42, payload, sizeof(payload)), 0);

    const void *out = NULL;
    size_t out_len = 0;
    T_ASSERT_EQ(t_storage_get(s, 42, &out, &out_len), 0);
    T_ASSERT_EQ((int)out_len, (int)sizeof(payload));
    T_ASSERT(memcmp(out, payload, sizeof(payload)) == 0);

    t_storage_destroy(s);
}

T_TEST(integration_coroutine_threadpool) {
    int coro_count = 0;
    t_coro *c = t_coro_create(coro_inc_fn, &coro_count, 8192);
    T_ASSERT_NOT_NULL(c);

    t_coro_resume(c);
    T_ASSERT_EQ(coro_count, 1);
    t_coro_resume(c);
    T_ASSERT_EQ(coro_count, 2);
    t_coro_resume(c);
    T_ASSERT_EQ(coro_count, 3);

    t_tpool *tp = t_tpool_create(2);
    T_ASSERT_NOT_NULL(tp);

    int task_count = 0;
    for (int i = 0; i < 4; i++) {
        t_tpool_submit(tp, tpool_inc_fn, (void *)&task_count);
    }
    t_tpool_wait(tp);
    T_ASSERT_EQ(__sync_add_and_fetch(&task_count, 0), 4);

    t_tpool_destroy(tp);
    t_coro_destroy(c);
}

int main(void) {
    return t_run_all_tests();
}
