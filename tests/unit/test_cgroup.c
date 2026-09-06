#include "t_test.h"
#include "t_cgroup.h"
#include <string.h>
#include <stdlib.h>

static size_t g_recv_count;
static char g_last_topic[128];
static char g_last_payload[128];
static char g_last_consumer[64];

static void on_deliver_a(const char *topic, const uint8_t *payload, size_t len, void *ud) {
    (void)ud;
    g_recv_count++;
    if (topic) strncpy(g_last_topic, topic, sizeof(g_last_topic) - 1);
    if (payload && len > 0 && len < sizeof(g_last_payload)) {
        memcpy(g_last_payload, payload, len);
        g_last_payload[len] = 0;
    }
    strcpy(g_last_consumer, "A");
}

static void on_deliver_b(const char *topic, const uint8_t *payload, size_t len, void *ud) {
    (void)ud;
    g_recv_count++;
    if (payload && len > 0 && len < sizeof(g_last_payload)) {
        memcpy(g_last_payload, payload, len);
        g_last_payload[len] = 0;
    }
    if (topic) strncpy(g_last_topic, topic, sizeof(g_last_topic) - 1);
    strcpy(g_last_consumer, "B");
}

T_TEST(cgroup_create_destroy) {
    t_cgroup *cg = t_cgroup_create("mygroup");
    T_ASSERT_NOT_NULL(cg);
    T_ASSERT(strcmp(t_cgroup_id(cg), "mygroup") == 0);
    T_ASSERT_EQ((int)t_cgroup_consumer_count(cg), 0);
    t_cgroup_destroy(cg);
}

T_TEST(cgroup_add_remove_consumer) {
    t_cgroup *cg = t_cgroup_create("g1");
    T_ASSERT_EQ(t_cgroup_add_consumer(cg, "c1", on_deliver_a, NULL), 0);
    T_ASSERT_EQ(t_cgroup_add_consumer(cg, "c2", on_deliver_b, NULL), 0);
    T_ASSERT_EQ((int)t_cgroup_consumer_count(cg), 2);
    T_ASSERT_EQ(t_cgroup_remove_consumer(cg, "c1"), 0);
    T_ASSERT_EQ((int)t_cgroup_consumer_count(cg), 1);
    t_cgroup_destroy(cg);
}

T_TEST(cgroup_round_robin_dispatch) {
    t_cgroup *cg = t_cgroup_create("g2");
    t_cgroup_add_consumer(cg, "A", on_deliver_a, NULL);
    t_cgroup_add_consumer(cg, "B", on_deliver_b, NULL);
    g_recv_count = 0;

    t_cgroup_dispatch(cg, "topic", (const uint8_t *)"msg1", 4);
    T_ASSERT(strcmp(g_last_consumer, "A") == 0);
    t_cgroup_dispatch(cg, "topic", (const uint8_t *)"msg2", 4);
    T_ASSERT(strcmp(g_last_consumer, "B") == 0);
    t_cgroup_dispatch(cg, "topic", (const uint8_t *)"msg3", 4);
    T_ASSERT(strcmp(g_last_consumer, "A") == 0);

    T_ASSERT_EQ((int)g_recv_count, 3);
    T_ASSERT_EQ((int)t_cgroup_total_dispatched(cg), 3);
    t_cgroup_destroy(cg);
}

T_TEST(cgroup_duplicate_consumer_rejected) {
    t_cgroup *cg = t_cgroup_create("g3");
    T_ASSERT_EQ(t_cgroup_add_consumer(cg, "c1", on_deliver_a, NULL), 0);
    T_ASSERT(t_cgroup_add_consumer(cg, "c1", on_deliver_b, NULL) != 0);
    t_cgroup_destroy(cg);
}

T_TEST(cgroup_dispatch_empty_fails) {
    t_cgroup *cg = t_cgroup_create("g4");
    T_ASSERT(t_cgroup_dispatch(cg, "t", NULL, 0) != 0);
    T_ASSERT_NULL(t_cgroup_pick(cg));
    t_cgroup_destroy(cg);
}

T_TEST(cgroup_pick_round_robin) {
    static int token_a, token_b;
    t_cgroup *cg = t_cgroup_create("g-pick");
    T_ASSERT_EQ(t_cgroup_add_consumer(cg, "A", on_deliver_a, &token_a), 0);
    T_ASSERT_EQ(t_cgroup_add_consumer(cg, "B", on_deliver_b, &token_b), 0);
    T_ASSERT(t_cgroup_pick(cg) == &token_a);
    T_ASSERT(t_cgroup_pick(cg) == &token_b);
    T_ASSERT(t_cgroup_pick(cg) == &token_a);
    t_cgroup_destroy(cg);
}

static void on_remove_self(const char *topic, const uint8_t *payload, size_t len, void *ud) {
    (void)topic; (void)payload; (void)len;
    t_cgroup *cg = (t_cgroup *)ud;
    (void)t_cgroup_remove_consumer(cg, "solo");
}

T_TEST(cgroup_dispatch_remove_last_in_cb) {
    t_cgroup *cg = t_cgroup_create("g5");
    T_ASSERT_EQ(t_cgroup_add_consumer(cg, "solo", on_remove_self, cg), 0);
    T_ASSERT_EQ(t_cgroup_dispatch(cg, "t", (const uint8_t *)"x", 1), 0);
    T_ASSERT_EQ((int)t_cgroup_consumer_count(cg), 0);
    T_ASSERT_EQ((int)t_cgroup_total_dispatched(cg), 1);
    t_cgroup_destroy(cg);
}

int main(void) {
    return t_run_all_tests();
}
