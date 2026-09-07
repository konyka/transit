#include "t_test.h"
#include "t_session.h"
#include "t_client.h"
#include <string.h>

T_TEST(session_create_destroy) {
    t_session *s = t_session_create(1);
    T_ASSERT_NOT_NULL(s);
    T_ASSERT_EQ((long long)t_session_id(s), 1);
    T_ASSERT_EQ(t_session_get_state(s), T_SESSION_DISCONNECTED);
    t_session_destroy(s);
}

T_TEST(session_connect_disconnect) {
    t_session *s = t_session_create(2);
    T_ASSERT_EQ((int)t_session_connect(s), 0);
    T_ASSERT_EQ(t_session_get_state(s), T_SESSION_CONNECTED);
    T_ASSERT(t_session_is_active(s));
    T_ASSERT_EQ(t_session_disconnect(s), 0);
    T_ASSERT_EQ(t_session_get_state(s), T_SESSION_DISCONNECTED);
    t_session_destroy(s);
}

T_TEST(session_user_data) {
    t_session *s = t_session_create(3);
    int val = 42;
    t_session_set_user_data(s, &val);
    int *p = (int *)t_session_get_user_data(s);
    T_ASSERT_EQ(*p, 42);
    t_session_destroy(s);
}

T_TEST(session_activity) {
    t_session *s = t_session_create(4);
    t_session_connect(s);
    t_session_update_activity(s);
    T_ASSERT(t_session_last_activity_ns(s) > 0);
    T_ASSERT_EQ(t_session_check_timeout(s, 5000000000LL), 0);
    T_ASSERT_EQ(t_session_check_timeout(s, 0), 0);
    t_session_destroy(s);
}

T_TEST(session_stats) {
    t_session *s = t_session_create(5);
    t_session_record_send(s);
    t_session_record_send(s);
    t_session_record_recv(s);
    T_ASSERT_EQ((int)t_session_msgs_sent(s), 2);
    T_ASSERT_EQ((int)t_session_msgs_received(s), 1);
    t_session_destroy(s);
}

T_TEST(client_create_destroy) {
    t_client *c = t_client_create("test-client");
    T_ASSERT_NOT_NULL(c);
    T_ASSERT(strcmp(t_client_id(c), "test-client") == 0);
    t_client_destroy(c);
}

T_TEST(client_ack_seq_starts_zero) {
    T_ASSERT_EQ((int)t_client_ack_seq(NULL), 0);
    t_client *c = t_client_create("ack");
    T_ASSERT_NOT_NULL(c);
    T_ASSERT_EQ((int)t_client_ack_seq(c), 0);
    T_ASSERT_EQ(t_client_last_status(c), 0);
    T_ASSERT_EQ(t_client_connect(c, "127.0.0.1", 1), 0);
    T_ASSERT_EQ((int)t_client_ack_seq(c), 0);
    t_client_destroy(c);
}

T_TEST(client_connect_disconnect) {
    t_client *c = t_client_create("c1");
    T_ASSERT(!t_client_is_connected(c));
    t_client_connect(c, "127.0.0.1", 8080);
    T_ASSERT(t_client_is_connected(c));
    t_client_disconnect(c);
    T_ASSERT(!t_client_is_connected(c));
    t_client_destroy(c);
}

T_TEST(client_queue_mgmt) {
    t_client *c = t_client_create("c2");
    t_client_connect(c, "localhost", 0);
    T_ASSERT_EQ(t_client_open_queue(c, "test.q", 0), 0);
    T_ASSERT_EQ((int)t_client_queue_count(c), 1);
    T_ASSERT_EQ(t_client_close_queue(c, "test.q"), 0);
    T_ASSERT_EQ((int)t_client_queue_count(c), 0);
    T_ASSERT_EQ(t_client_open_queue(c, "again.q", 0), 0);
    T_ASSERT_EQ(t_client_close_follow(c, "again.q", 50), 0);
    T_ASSERT_EQ((int)t_client_queue_count(c), 0);
    T_ASSERT_EQ(t_client_close_follow(c, "missing.q", 50), -1);
    T_ASSERT_EQ(t_client_set_auto_confirm(c, 0), 0);
    T_ASSERT_EQ(t_client_set_auto_confirm(c, 1), 0);
    T_ASSERT_EQ(t_client_set_auto_confirm(c, -1), -1);
    T_ASSERT_EQ((int)t_client_last_push_id(c), 0);
    T_ASSERT_EQ(t_client_reject(c, "again.q"), -1);
    T_ASSERT_EQ(t_client_confirm(c, "again.q"), -1);
    T_ASSERT_EQ(t_client_reject_follow(c, "again.q", 50), -1);
    T_ASSERT_EQ(t_client_confirm_follow(c, "again.q", 50), -1);
    T_ASSERT_EQ(t_client_open_queue(c, "pri.q", T_CLIENT_QTYPE_PRIORITY), 0);
    T_ASSERT_EQ((int)t_client_queue_count(c), 1);
    T_ASSERT_EQ(t_client_close_follow(c, "pri.q", 50), 0);
    T_ASSERT_EQ(t_client_open_queue(c, "bad.q", 0x30000), -1);
    t_client_destroy(c);
}

static int g_msg_received;
static void on_msg(const char *queue_name, const uint8_t *data, size_t len, void *ud) {
    (void)queue_name; (void)data; (void)len;
    (*(int *)ud)++;
}

T_TEST(client_subscribe_before_open) {
    t_client *c = t_client_create("c4");
    t_client_connect(c, "localhost", 0);
    g_msg_received = 0;
    T_ASSERT_EQ(t_client_subscribe(c, "early.q", on_msg, &g_msg_received), 0);
    T_ASSERT_EQ((int)t_client_queue_count(c), 1);
    T_ASSERT_EQ(t_client_unsubscribe(c, "early.q"), 0);
    T_ASSERT_EQ((int)t_client_queue_count(c), 0);
    T_ASSERT_EQ(t_client_subscribe(c, "early.q", on_msg, &g_msg_received), 0);
    T_ASSERT_EQ(t_client_open_queue(c, "early.q", 0), 0);
    T_ASSERT_EQ(t_client_post(c, "early.q", (const uint8_t *)"hi", 2, 0), 0);
    T_ASSERT_EQ(g_msg_received, 1);
    T_ASSERT_EQ(t_client_close_follow(c, "early.q", 50), 0);
    T_ASSERT_EQ((int)t_client_queue_count(c), 0);
    T_ASSERT_EQ(t_client_subscribe_follow(c, "again.q", on_msg, &g_msg_received,
                                          0, 50), 0);
    T_ASSERT_EQ((int)t_client_queue_count(c), 1);
    T_ASSERT_EQ(t_client_subscribe_follow(c, "again.q", on_msg, &g_msg_received,
                                          0, 50), -1);
    T_ASSERT_EQ(t_client_close_follow(c, "again.q", 50), 0);
    t_client_destroy(c);
}

static int g_stub_pri;
static void on_pri_stub(const char *queue_name, const uint8_t *data, size_t len,
                        void *ud) {
    (void)queue_name;
    (void)data;
    (void)len;
    g_stub_pri = t_client_last_push_priority((t_client *)ud);
}

T_TEST(client_stub_last_push_priority) {
    t_client *c = t_client_create("pri");
    t_client_connect(c, "localhost", 0);
    T_ASSERT_EQ(t_client_last_push_priority(c), 0);
    T_ASSERT_EQ(t_client_open_queue(c, "p.q", T_CLIENT_QTYPE_PRIORITY), 0);
    T_ASSERT_EQ(t_client_subscribe(c, "p.q", on_pri_stub, c), 0);
    g_stub_pri = -1;
    T_ASSERT_EQ(t_client_post(c, "p.q", (const uint8_t *)"a", 1, 7), 0);
    T_ASSERT_EQ(g_stub_pri, 7);
    T_ASSERT_EQ(t_client_post(c, "p.q", (const uint8_t *)"b", 1, -3), 0);
    T_ASSERT_EQ(g_stub_pri, 0);
    T_ASSERT_EQ(t_client_post(c, "p.q", (const uint8_t *)"c", 1, 300), 0);
    T_ASSERT_EQ(g_stub_pri, 255);
    t_client_destroy(c);
}

static const char *g_stub_q;
static void on_q_stub(const char *queue_name, const uint8_t *data, size_t len,
                      void *ud) {
    (void)queue_name;
    (void)data;
    (void)len;
    g_stub_q = t_client_last_push_queue((t_client *)ud);
}

T_TEST(client_stub_last_push_queue) {
    t_client *c = t_client_create("q");
    t_client_connect(c, "localhost", 0);
    T_ASSERT_NULL(t_client_last_push_queue(c));
    T_ASSERT_EQ(t_client_open_queue(c, "p.q", 0), 0);
    T_ASSERT_EQ(t_client_subscribe(c, "p.q", on_q_stub, c), 0);
    g_stub_q = NULL;
    T_ASSERT_EQ(t_client_post(c, "p.q", (const uint8_t *)"a", 1, 0), 0);
    T_ASSERT_NOT_NULL(g_stub_q);
    T_ASSERT_STR_EQ(g_stub_q, "p.q");
    T_ASSERT_STR_EQ(t_client_last_push_queue(c), "p.q");
    t_client_destroy(c);
}

T_TEST(client_publish_stats) {
    t_client *c = t_client_create("c3");
    t_client_connect(c, "localhost", 0);
    t_client_open_queue(c, "test.pub", 0);
    g_msg_received = 0;
    t_client_subscribe(c, "test.pub", on_msg, &g_msg_received);
    t_client_post(c, "test.pub", (const uint8_t *)"hello", 5, 0);
    T_ASSERT_EQ((int)t_client_total_published(c), 1);
    t_client_destroy(c);
}

int main(void) {
    return t_run_all_tests();
}
