#include "t_test.h"
#include "t_dispatch.h"
#include "t_broker.h"
#include "t_queue.h"
#include "t_session.h"

T_TEST(dispatch_create_destroy) {
    t_broker *b = t_broker_create("test-dispatch");
    T_ASSERT_NOT_NULL(b);
    t_dispatch *d = t_dispatch_create(b);
    T_ASSERT_NOT_NULL(d);
    t_dispatch_destroy(d);
    t_broker_destroy(b);
}

T_TEST(dispatch_register_session) {
    t_broker *b = t_broker_create("test-dispatch");
    t_dispatch *d = t_dispatch_create(b);
    t_session *s = t_session_create(100);

    T_ASSERT_EQ(t_dispatch_register(d, 100, s), 0);
    T_ASSERT_EQ((int)t_dispatch_session_count(d), 1);
    T_ASSERT_EQ(t_dispatch_unregister(d, 100), 0);
    T_ASSERT_EQ((int)t_dispatch_session_count(d), 0);

    t_dispatch_destroy(d);
    t_broker_destroy(b);
    t_session_destroy(s);
}

T_TEST(dispatch_unregister_clears_subs) {
    t_broker *b = t_broker_create("test-dispatch-unreg");
    t_broker_start(b);
    t_broker_create_queue(b, "default", "u.q", T_QUEUE_BROADCAST, 0);
    t_dispatch *d = t_dispatch_create(b);
    t_session *s = t_session_create(7);
    T_ASSERT_EQ(t_dispatch_register(d, 7, s), 0);
    T_ASSERT_EQ(t_dispatch_subscribe(d, 7, "u.q"), 0);
    T_ASSERT_EQ(t_dispatch_subscribe(d, 7, "u.q"), -1);

    uint8_t msg[] = "x";
    T_ASSERT_EQ(t_dispatch_publish(d, 7, "u.q", msg, 1, 0), 0);
    T_ASSERT_EQ((int)t_dispatch_total_delivered(d), 1);

    T_ASSERT_EQ(t_dispatch_unregister(d, 7), 0);
    T_ASSERT_EQ(t_dispatch_publish(d, 7, "u.q", msg, 1, 0), -1);
    T_ASSERT_EQ((int)t_dispatch_total_delivered(d), 1);

    t_dispatch_destroy(d);
    t_broker_stop(b);
    t_broker_destroy(b);
    t_session_destroy(s);
}

T_TEST(dispatch_publish) {
    t_broker *b = t_broker_create("test-dispatch");
    t_broker_start(b);
    t_broker_create_queue(b, "default", "test.q", T_QUEUE_BROADCAST, 0);
    t_dispatch *d = t_dispatch_create(b);
    t_session *s = t_session_create(1);
    t_dispatch_register(d, 1, s);
    t_dispatch_subscribe(d, 1, "test.q");

    uint8_t msg[] = "hello";
    T_ASSERT_EQ(t_dispatch_publish(d, 1, "test.q", msg, sizeof(msg), 0), 0);
    T_ASSERT_EQ((int)t_dispatch_total_published(d), 1);
    T_ASSERT_EQ((int)t_dispatch_total_delivered(d), 1);

    t_dispatch_destroy(d);
    t_broker_stop(b);
    t_broker_destroy(b);
    t_session_destroy(s);
}

T_TEST(dispatch_unsubscribe_stops_delivery) {
    t_broker *b = t_broker_create("test-dispatch-unsub");
    t_broker_start(b);
    t_broker_create_queue(b, "default", "unsub.q", T_QUEUE_BROADCAST, 0);
    t_dispatch *d = t_dispatch_create(b);
    t_session *s = t_session_create(2);
    t_dispatch_register(d, 2, s);
    T_ASSERT_EQ(t_dispatch_subscribe(d, 2, "unsub.q"), 0);

    uint8_t msg[] = "x";
    T_ASSERT_EQ(t_dispatch_publish(d, 2, "unsub.q", msg, 1, 0), 0);
    T_ASSERT_EQ((int)t_dispatch_total_delivered(d), 1);

    T_ASSERT_EQ(t_dispatch_unsubscribe(d, 2, "unsub.q"), 0);
    T_ASSERT_EQ(t_dispatch_publish(d, 2, "unsub.q", msg, 1, 0), -1);
    T_ASSERT_EQ((int)t_dispatch_total_delivered(d), 1);
    T_ASSERT_EQ(t_dispatch_unsubscribe(d, 2, "unsub.q"), -1);

    t_dispatch_destroy(d);
    t_broker_stop(b);
    t_broker_destroy(b);
    t_session_destroy(s);
}

T_TEST(dispatch_destroy_blocks_session_free_until_released) {
    t_broker *b = t_broker_create("test-dispatch-ref");
    t_dispatch *d = t_dispatch_create(b);
    t_session *s = t_session_create(9);
    T_ASSERT_EQ(t_dispatch_register(d, 9, s), 0);
    T_ASSERT_EQ(t_session_destroy(s), -1); /* still registered */
    t_dispatch_destroy(d);                 /* releases ref */
    T_ASSERT_EQ(t_session_destroy(s), 0);
    t_broker_destroy(b);
}

T_TEST(dispatch_heals_after_queue_recreate) {
    t_broker *b = t_broker_create("test-dispatch-heal");
    t_broker_start(b);
    T_ASSERT_EQ(t_broker_create_queue(b, "default", "heal.q", T_QUEUE_FIFO, 0), 0);
    t_dispatch *d = t_dispatch_create(b);
    t_session *s = t_session_create(3);
    T_ASSERT_EQ(t_dispatch_register(d, 3, s), 0);
    T_ASSERT_EQ(t_dispatch_subscribe(d, 3, "heal.q"), 0);
    T_ASSERT_EQ(t_broker_delete_queue(b, "default", "heal.q"), 0);
    T_ASSERT_EQ(t_broker_create_queue(b, "default", "heal.q", T_QUEUE_FIFO, 0), 0);
    uint8_t msg[] = "z";
    T_ASSERT_EQ(t_dispatch_publish(d, 3, "heal.q", msg, 1, 0), 0);
    T_ASSERT_EQ((int)t_dispatch_total_delivered(d), 1);
    t_dispatch_destroy(d);
    t_broker_stop(b);
    t_broker_destroy(b);
    t_session_destroy(s);
}

int main(void) {
    return t_run_all_tests();
}
