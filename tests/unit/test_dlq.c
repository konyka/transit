#include "t_test.h"
#include "t_dlq.h"
#include <stdlib.h>
#include <string.h>

T_TEST(dlq_create_destroy) {
    t_dlq *dlq = t_dlq_create(16);
    T_ASSERT_NOT_NULL(dlq);
    T_ASSERT(t_dlq_is_empty(dlq));
    T_ASSERT(!t_dlq_is_full(dlq));
    T_ASSERT_EQ((int)t_dlq_size(dlq), 0);
    T_ASSERT_EQ((int)t_dlq_capacity(dlq), 16);
    t_dlq_destroy(dlq);
}

T_TEST(dlq_push_pop) {
    t_dlq *dlq = t_dlq_create(16);
    T_ASSERT_EQ(t_dlq_push(dlq, "test.topic", (const uint8_t *)"hello", 5, "max_retries"), 0);
    T_ASSERT_EQ((int)t_dlq_size(dlq), 1);
    T_ASSERT(!t_dlq_is_empty(dlq));

    t_dlq_entry e;
    T_ASSERT_EQ(t_dlq_pop(dlq, &e), 0);
    T_ASSERT(strcmp(e.topic, "test.topic") == 0);
    T_ASSERT_EQ((int)e.payload_len, 5);
    T_ASSERT(memcmp(e.payload, "hello", 5) == 0);
    T_ASSERT(strcmp(e.reason, "max_retries") == 0);
    T_ASSERT(e.timestamp_ms > 0);
    free(e.topic);
    free(e.payload);
    free(e.reason);

    T_ASSERT(t_dlq_is_empty(dlq));
    t_dlq_destroy(dlq);
}

T_TEST(dlq_fifo_order) {
    t_dlq *dlq = t_dlq_create(16);
    t_dlq_push(dlq, "a", NULL, 0, NULL);
    t_dlq_push(dlq, "b", NULL, 0, NULL);
    t_dlq_push(dlq, "c", NULL, 0, NULL);

    t_dlq_entry e;
    t_dlq_pop(dlq, &e);
    T_ASSERT(strcmp(e.topic, "a") == 0);
    free(e.topic);
    t_dlq_pop(dlq, &e);
    T_ASSERT(strcmp(e.topic, "b") == 0);
    free(e.topic);
    t_dlq_pop(dlq, &e);
    T_ASSERT(strcmp(e.topic, "c") == 0);
    free(e.topic);
    t_dlq_destroy(dlq);
}

T_TEST(dlq_overflow_drops_oldest) {
    t_dlq *dlq = t_dlq_create(3);
    t_dlq_push(dlq, "first", NULL, 0, NULL);
    t_dlq_push(dlq, "second", NULL, 0, NULL);
    t_dlq_push(dlq, "third", NULL, 0, NULL);
    T_ASSERT(t_dlq_is_full(dlq));
    T_ASSERT_EQ(t_dlq_push(dlq, "fourth", NULL, 0, NULL), 0);
    T_ASSERT_EQ((int)t_dlq_total_dropped(dlq), 1);

    t_dlq_entry e;
    t_dlq_pop(dlq, &e);
    T_ASSERT(strcmp(e.topic, "second") == 0);
    free(e.topic);
    t_dlq_destroy(dlq);
}

T_TEST(dlq_clear) {
    t_dlq *dlq = t_dlq_create(16);
    t_dlq_push(dlq, "a", (const uint8_t *)"x", 1, NULL);
    t_dlq_push(dlq, "b", (const uint8_t *)"y", 1, NULL);
    t_dlq_clear(dlq);
    T_ASSERT(t_dlq_is_empty(dlq));
    T_ASSERT_EQ((int)t_dlq_size(dlq), 0);
    t_dlq_destroy(dlq);
}

T_TEST(dlq_stats) {
    t_dlq *dlq = t_dlq_create(16);
    t_dlq_push(dlq, "a", NULL, 0, NULL);
    t_dlq_push(dlq, "b", NULL, 0, NULL);
    t_dlq_entry e;
    t_dlq_pop(dlq, &e);
    free(e.topic);
    T_ASSERT_EQ((int)t_dlq_total_pushed(dlq), 2);
    T_ASSERT_EQ((int)t_dlq_total_popped(dlq), 1);
    t_dlq_destroy(dlq);
}

int main(void) {
    return t_run_all_tests();
}
