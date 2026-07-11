#include "t_test.h"
#include "t_broker.h"
#include <string.h>

T_TEST(broker_create_destroy) {
    t_broker *b = t_broker_create("broker-1");
    T_ASSERT_NOT_NULL(b);
    T_ASSERT(strcmp(t_broker_id(b), "broker-1") == 0);
    t_broker_destroy(b);
}

T_TEST(broker_start_stop) {
    t_broker *b = t_broker_create("broker-2");
    T_ASSERT_EQ(t_broker_start(b), 0);
    T_ASSERT(t_broker_is_running(b));
    T_ASSERT_EQ(t_broker_stop(b), 0);
    T_ASSERT(!t_broker_is_running(b));
    t_broker_destroy(b);
}

T_TEST(broker_domain_mgmt) {
    t_broker *b = t_broker_create("broker-3");
    /* broker auto-creates "default" domain, so count starts at 1 */
    T_ASSERT_EQ((int)t_broker_domain_count(b), 1);
    t_domain *d = t_broker_create_domain(b, "test-domain");
    T_ASSERT_NOT_NULL(d);
    T_ASSERT_EQ((int)t_broker_domain_count(b), 2);
    t_domain *d2 = t_broker_get_domain(b, "test-domain");
    T_ASSERT(d == d2);
    T_ASSERT_EQ(t_broker_remove_domain(b, "test-domain"), 0);
    T_ASSERT_EQ((int)t_broker_domain_count(b), 1); /* only default remains */
    t_broker_destroy(b);
}

T_TEST(broker_queue_mgmt) {
    t_broker *b = t_broker_create("broker-4");
    T_ASSERT_EQ(t_broker_create_queue(b, "default", "test.q", 0, 0), 0);
    T_ASSERT_EQ((int)t_broker_total_queues(b), 1);
    T_ASSERT_EQ(t_broker_create_queue(b, "default", "test.q", 0, 0), -1);
    T_ASSERT_EQ((int)t_broker_total_queues(b), 1);
    T_ASSERT_EQ(t_broker_delete_queue(b, "default", "test.q"), 0);
    T_ASSERT_EQ((int)t_broker_total_queues(b), 0);
    t_broker_destroy(b);
}

static int g_delivered;
static void on_broker_msg(const char *queue_name, const uint8_t *data, size_t len, void *ud) {
    (void)queue_name; (void)data; (void)len;
    (*(int *)ud)++;
}

T_TEST(broker_delete_queue_clears_subs) {
    t_broker *b = t_broker_create("broker-del-sub");
    t_broker_start(b);
    T_ASSERT_EQ(t_broker_create_queue(b, "default", "gone.q", 2, 0), 0);
    g_delivered = 0;
    T_ASSERT_EQ(t_broker_subscribe(b, "gone.q", on_broker_msg, &g_delivered), 0);
    T_ASSERT_EQ(t_broker_delete_queue(b, "default", "gone.q"), 0);
    T_ASSERT_EQ(t_broker_create_queue(b, "default", "gone.q", 2, 0), 0);
    T_ASSERT_EQ(t_broker_subscribe(b, "gone.q", on_broker_msg, &g_delivered), 0);
    t_broker_publish(b, "gone.q", (const uint8_t *)"x", 1, 0);
    T_ASSERT_EQ(g_delivered, 1);
    t_broker_stop(b);
    t_broker_destroy(b);
}

T_TEST(broker_publish_subscribe) {
    t_broker *b = t_broker_create("broker-5");
    t_broker_start(b);
    t_broker_create_queue(b, "default", "pub.q", 2, 0); /* BROADCAST */
    g_delivered = 0;
    t_broker_subscribe(b, "pub.q", on_broker_msg, &g_delivered);
    t_broker_publish(b, "pub.q", (const uint8_t *)"hello", 5, 0);
    T_ASSERT_EQ(g_delivered, 1);
    T_ASSERT_EQ((int)t_broker_total_messages(b), 1);
    T_ASSERT_EQ((int)t_broker_total_delivered(b), 1);
    t_broker_destroy(b);
}

T_TEST(broker_fifo_subscribe_delivered) {
    t_broker *b = t_broker_create("broker-fifo");
    t_broker_start(b);
    t_broker_create_queue(b, "default", "fifo.q", 0, 0); /* FIFO */
    g_delivered = 0;
    t_broker_subscribe(b, "fifo.q", on_broker_msg, &g_delivered);
    for (int i = 0; i < 50; i++) {
        t_broker_publish(b, "fifo.q", (const uint8_t *)"x", 1, 0);
    }
    T_ASSERT_EQ(g_delivered, 50);
    T_ASSERT_EQ((int)t_broker_total_delivered(b), 50);
    t_broker_destroy(b);
}

T_TEST(broker_stats) {
    t_broker *b = t_broker_create("broker-6");
    t_broker_start(b);
    t_broker_create_queue(b, "default", "stats.q", 0, 0);
    t_broker_publish(b, "stats.q", (const uint8_t *)"x", 1, 0);
    t_broker_publish(b, "stats.q", (const uint8_t *)"y", 1, 0);
    T_ASSERT_EQ((int)t_broker_total_messages(b), 2);
    t_broker_destroy(b);
}

int main(void) {
    return t_run_all_tests();
}
