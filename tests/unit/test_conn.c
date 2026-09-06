#include "t_test.h"
#include "t_conn.h"
#include "t_proto.h"
#include "t_evloop.h"
#include "t_socket.h"
#include "t_thread.h"
#include <stdlib.h>
#include <string.h>

static size_t g_recv_count;
static char g_last_payload[256];

static void on_msg(t_conn *conn, const t_proto_msg *msg, void *ud) {
    (void)conn; (void)ud;
    g_recv_count++;
    if (msg->payload && msg->payload_len > 0) {
        size_t n = msg->payload_len < 255 ? msg->payload_len : 255;
        memcpy(g_last_payload, msg->payload, n);
        g_last_payload[n] = '\0';
    }
}

static void timer_stop(void *ud) {
    t_evloop_stop((t_evloop *)ud);
}

static void *loop_fn(void *arg) {
    t_evloop *loop = (t_evloop *)arg;
    t_evloop_timer_add(loop, 200, 0, timer_stop, loop);
    t_evloop_run(loop, 100);
    return NULL;
}

T_TEST(conn_multi_send_append) {
    int fds[2];
    T_ASSERT_EQ(t_socket_pair(fds), 0);
    t_evloop *loop = t_evloop_create();
    t_conn *client = t_conn_create(fds[0], loop);
    t_conn *server = t_conn_create(fds[1], loop);
    t_conn_set_on_msg(server, on_msg, NULL);
    g_recv_count = 0;

    const char *p1 = "msg1";
    const char *p2 = "msg2";
    t_proto_msg m1, m2;
    t_proto_header_init(&m1.header, T_MSG_POST, strlen(p1));
    m1.payload = (uint8_t *)p1;
    m1.payload_len = strlen(p1);
    t_proto_header_init(&m2.header, T_MSG_POST, strlen(p2));
    m2.payload = (uint8_t *)p2;
    m2.payload_len = strlen(p2);

    t_conn_send(client, &m1);
    t_conn_send(client, &m2);

    T_ASSERT_EQ(t_conn_msgs_sent(client), (size_t)2);

    t_thread t;
    T_ASSERT_EQ(t_thread_spawn(&t, loop_fn, loop), 0);
    T_ASSERT_EQ(t_thread_join(&t), 0);

    T_ASSERT_EQ(g_recv_count, (size_t)2);

    t_conn_destroy(client);
    t_conn_destroy(server);
    t_evloop_destroy(loop);
}

T_TEST(conn_send_empty_payload) {
    int fds[2];
    T_ASSERT_EQ(t_socket_pair(fds), 0);
    t_evloop *loop = t_evloop_create();
    t_conn *client = t_conn_create(fds[0], loop);
    t_conn *server = t_conn_create(fds[1], loop);
    t_conn_set_on_msg(server, on_msg, NULL);
    g_recv_count = 0;

    t_proto_msg m;
    t_proto_header_init(&m.header, T_MSG_NOP, 0);
    m.payload = NULL;
    m.payload_len = 0;

    t_conn_send(client, &m);
    T_ASSERT_EQ(t_conn_msgs_sent(client), (size_t)1);

    t_thread t;
    T_ASSERT_EQ(t_thread_spawn(&t, loop_fn, loop), 0);
    T_ASSERT_EQ(t_thread_join(&t), 0);

    T_ASSERT_EQ(g_recv_count, (size_t)1);

    t_conn_destroy(client);
    t_conn_destroy(server);
    t_evloop_destroy(loop);
}

T_TEST(conn_create_destroy_no_evloop) {
    int fds[2];
    T_ASSERT_EQ(t_socket_pair(fds), 0);
    t_conn *a = t_conn_create(fds[0], NULL);
    t_conn *b = t_conn_create(fds[1], NULL);
    T_ASSERT_NOT_NULL(a);
    T_ASSERT_NOT_NULL(b);
    T_ASSERT_EQ(t_conn_fd(a), fds[0]);
    T_ASSERT(!t_conn_is_closed(a));
    t_conn_destroy(a);
    t_conn_destroy(b);
}

T_TEST(conn_bytes_sent_once) {
    int fds[2];
    T_ASSERT_EQ(t_socket_pair(fds), 0);
    t_evloop *loop = t_evloop_create();
    t_conn *client = t_conn_create(fds[0], loop);
    t_conn *server = t_conn_create(fds[1], loop);
    t_conn_set_on_msg(server, on_msg, NULL);
    g_recv_count = 0;

    const char *p = "hello";
    t_proto_msg m;
    t_proto_header_init(&m.header, T_MSG_POST, (uint32_t)strlen(p));
    m.payload = (uint8_t *)p;
    m.payload_len = strlen(p);
    t_conn_send(client, &m);

    size_t queued = T_PROTO_HEADER_SIZE + strlen(p);
    /* Before flush, bytes_sent should still be 0 (counted on write). */
    T_ASSERT_EQ(t_conn_bytes_sent(client), (size_t)0);

    t_thread t;
    T_ASSERT_EQ(t_thread_spawn(&t, loop_fn, loop), 0);
    T_ASSERT_EQ(t_thread_join(&t), 0);

    T_ASSERT_EQ(g_recv_count, (size_t)1);
    T_ASSERT_EQ(t_conn_bytes_sent(client), queued);

    t_conn_destroy(client);
    t_conn_destroy(server);
    t_evloop_destroy(loop);
}

int main(void) {
    return t_run_all_tests();
}
