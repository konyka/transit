#include "t_test.h"
#include "t_queue.h"
#include "t_router.h"
#include <string.h>

T_TEST(queue_create_destroy) {
    t_queue *q = t_queue_create("test.q", T_QUEUE_FIFO, T_QUEUE_FLAG_NONE);
    T_ASSERT_NOT_NULL(q);
    T_ASSERT(strcmp(t_queue_name(q), "test.q") == 0);
    T_ASSERT_EQ((int)t_queue_get_type(q), T_QUEUE_FIFO);
    t_queue_destroy(q);
}

T_TEST(queue_fifo_post_consume) {
    t_queue *q = t_queue_create("test.fifo", T_QUEUE_FIFO, 0);
    t_queue_post(q, (const uint8_t *)"hello", 5, 0);
    t_queue_post(q, (const uint8_t *)"world", 5, 0);
    T_ASSERT_EQ((int)t_queue_pending_count(q), 2);
    t_msg msg;
    T_ASSERT_EQ(t_queue_consume(q, &msg), 0);
    T_ASSERT(memcmp(msg.data, "hello", 5) == 0);
    T_ASSERT_EQ(t_queue_consume(q, &msg), 0);
    T_ASSERT(memcmp(msg.data, "world", 5) == 0);
    t_queue_destroy(q);
}

T_TEST(queue_priority_order) {
    t_queue *q = t_queue_create("test.pri", T_QUEUE_PRIORITY, 0);
    t_queue_post(q, (const uint8_t *)"low", 3, 10);
    t_queue_post(q, (const uint8_t *)"high", 4, 1);
    t_queue_post(q, (const uint8_t *)"mid", 3, 5);
    t_msg msg;
    t_queue_consume(q, &msg);
    T_ASSERT(memcmp(msg.data, "high", 4) == 0);
    t_queue_consume(q, &msg);
    T_ASSERT(memcmp(msg.data, "mid", 3) == 0);
    t_queue_consume(q, &msg);
    T_ASSERT(memcmp(msg.data, "low", 3) == 0);
    t_queue_destroy(q);
}

T_TEST(queue_stats) {
    t_queue *q = t_queue_create("test.stats", T_QUEUE_FIFO, 0);
    T_ASSERT_EQ((int)t_queue_total_published(q), 0);
    t_queue_post(q, (const uint8_t *)"x", 1, 0);
    T_ASSERT_EQ((int)t_queue_total_published(q), 1);
    t_queue_destroy(q);
}

static int g_broadcast_count;
static void broadcast_cb(const t_msg *msg, void *ud) {
    (void)msg;
    (*(int *)ud) += 1;
}

T_TEST(queue_broadcast) {
    t_queue *q = t_queue_create("test.bc", T_QUEUE_BROADCAST, 0);
    g_broadcast_count = 0;
    t_queue_add_consumer(q, broadcast_cb, &g_broadcast_count);
    t_queue_add_consumer(q, broadcast_cb, &g_broadcast_count);
    t_queue_post(q, (const uint8_t *)"msg", 3, 0);
    T_ASSERT_EQ(g_broadcast_count, 2);
    t_queue_destroy(q);
}

static int g_fifo_cb_count;
static void fifo_cb(const t_msg *msg, void *ud) {
    (void)msg;
    (*(int *)ud) += 1;
}

T_TEST(queue_fifo_subscribe_no_pending_growth) {
    /* Regression: push-style FIFO must not also enqueue to pending. */
    t_queue *q = t_queue_create("test.fifo.sub", T_QUEUE_FIFO, 0);
    g_fifo_cb_count = 0;
    t_queue_add_consumer(q, fifo_cb, &g_fifo_cb_count);
    for (int i = 0; i < 100; i++) {
        T_ASSERT_EQ(t_queue_post(q, (const uint8_t *)"x", 1, 0), 0);
    }
    T_ASSERT_EQ(g_fifo_cb_count, 100);
    T_ASSERT_EQ((int)t_queue_pending_count(q), 0);
    t_queue_destroy(q);
}

T_TEST(queue_priority_pending_count) {
    t_queue *q = t_queue_create("test.pri.count", T_QUEUE_PRIORITY, 0);
    t_queue_post(q, (const uint8_t *)"a", 1, 5);
    t_queue_post(q, (const uint8_t *)"b", 1, 1);
    T_ASSERT_EQ((int)t_queue_pending_count(q), 2);
    t_msg msg;
    t_queue_consume(q, &msg);
    T_ASSERT_EQ((int)t_queue_pending_count(q), 1);
    t_queue_destroy(q);
}

T_TEST(queue_priority_destroy_drains) {
    t_queue *q = t_queue_create("test.pri.drain", T_QUEUE_PRIORITY, 0);
    T_ASSERT_EQ(t_queue_post(q, (const uint8_t *)"a", 1, 2), 0);
    T_ASSERT_EQ(t_queue_post(q, (const uint8_t *)"b", 1, 1), 0);
    t_queue_destroy(q); /* must free pending priority msgs */
}

static void queue_noop_cb(const t_msg *msg, void *ud) {
    (void)msg;
    (*(int *)ud)++;
}

T_TEST(queue_remove_consumer_by_id) {
    t_queue *q = t_queue_create("test.cons", T_QUEUE_FIFO, 0);
    int a = 0, b = 0;
    uint64_t id1 = t_queue_add_consumer(q, queue_noop_cb, &a);
    uint64_t id2 = t_queue_add_consumer(q, queue_noop_cb, &b);
    T_ASSERT(id1 != 0 && id2 != 0);
    T_ASSERT_EQ(t_queue_remove_consumer(q, id1), 0);
    T_ASSERT_EQ(t_queue_remove_consumer(q, id2), 0);
    T_ASSERT_EQ((int)t_queue_consumer_count(q), 0);
    t_queue_destroy(q);
}

T_TEST(router_create_destroy) {
    t_router *r = t_router_create();
    T_ASSERT_NOT_NULL(r);
    t_router_destroy(r);
}

T_TEST(router_exact_match) {
    t_router *r = t_router_create();
    int target1 = 1, target2 = 2;
    t_router_bind(r, "stock.NYSE", &target1);
    t_router_bind(r, "stock.LSE", &target2);
    void *results[4];
    size_t n = t_router_route(r, "stock.NYSE", results, 4);
    T_ASSERT_EQ((int)n, 1);
    T_ASSERT_EQ(*(int *)results[0], 1);
    t_router_destroy(r);
}

T_TEST(router_wildcard_star) {
    t_router *r = t_router_create();
    int target = 42;
    t_router_bind(r, "stock.*.price", &target);
    void *results[4];
    size_t n = t_router_route(r, "stock.NYSE.price", results, 4);
    T_ASSERT_EQ((int)n, 1);
    n = t_router_route(r, "stock.NYSE.volume", results, 4);
    T_ASSERT_EQ((int)n, 0);
    t_router_destroy(r);
}

T_TEST(router_wildcard_hash) {
    t_router *r = t_router_create();
    int target = 99;
    t_router_bind(r, "stock.#", &target);
    void *results[4];
    size_t n = t_router_route(r, "stock.NYSE.price.bid", results, 4);
    T_ASSERT_EQ((int)n, 1);
    n = t_router_route(r, "stock", results, 4);
    T_ASSERT_EQ((int)n, 1);
    t_router_destroy(r);
}

T_TEST(router_multiple_matches) {
    t_router *r = t_router_create();
    int t1 = 1, t2 = 2;
    t_router_bind(r, "stock.NYSE.*", &t1);
    t_router_bind(r, "stock.#", &t2);
    void *results[4];
    size_t n = t_router_route(r, "stock.NYSE.price", results, 4);
    T_ASSERT_EQ((int)n, 2);
    t_router_destroy(r);
}

T_TEST(router_unbind) {
    t_router *r = t_router_create();
    int target = 1;
    t_router_bind(r, "test.topic", &target);
    void *results[4];
    T_ASSERT_EQ((int)t_router_route(r, "test.topic", results, 4), 1);
    t_router_unbind(r, "test.topic");
    T_ASSERT_EQ((int)t_router_route(r, "test.topic", results, 4), 0);
    t_router_destroy(r);
}

int main(void) {
    return t_run_all_tests();
}
