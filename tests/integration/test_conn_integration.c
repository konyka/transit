#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "t_conn.h"
#include "t_proto.h"
#include "t_evloop.h"
#include "t_socket.h"
#include "t_thread.h"
#include "t_time.h"
#include "t_test.h"

static size_t g_msgs_recv;
static char g_last_payload[256];

static void on_msg(t_conn *conn, const t_proto_msg *msg, void *ud) {
    (void)conn; (void)ud;
    g_msgs_recv++;
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

T_TEST(conn_create_destroy) {
    int fds[2];
    T_ASSERT_EQ(t_socket_pair(fds), 0);
    t_evloop *loop = t_evloop_create();
    t_conn *a = t_conn_create(fds[0], loop);
    t_conn *b = t_conn_create(fds[1], loop);
    T_ASSERT_NOT_NULL(a);
    T_ASSERT_NOT_NULL(b);
    T_ASSERT_EQ(t_conn_fd(a), fds[0]);
    T_ASSERT(!t_conn_is_closed(a));
    t_conn_destroy(a);
    t_conn_destroy(b);
    t_evloop_destroy(loop);
}

T_TEST(conn_send_receive) {
    int fds[2];
    T_ASSERT_EQ(t_socket_pair(fds), 0);
    t_evloop *loop = t_evloop_create();
    t_conn *client = t_conn_create(fds[0], loop);
    t_conn *server = t_conn_create(fds[1], loop);
    t_conn_set_on_msg(server, on_msg, NULL);

    g_msgs_recv = 0;
    const char *payload = "hello_transit";
    size_t plen = strlen(payload);
    t_proto_msg msg;
    t_proto_header_init(&msg.header, T_MSG_POST, plen);
    msg.payload = (uint8_t *)malloc(plen);
    memcpy(msg.payload, payload, plen);
    msg.payload_len = plen;

    t_thread t;
    T_ASSERT_EQ(t_thread_spawn(&t, loop_fn, loop), 0);
    t_time_sleep_us(10000);
    t_conn_send(client, &msg);
    free(msg.payload);
    T_ASSERT_EQ(t_thread_join(&t), 0);

    T_ASSERT_EQ(g_msgs_recv, (size_t)1);
    T_ASSERT(strcmp(g_last_payload, payload) == 0);
    t_conn_destroy(client);
    t_conn_destroy(server);
    t_evloop_destroy(loop);
}

T_TEST(conn_stats) {
    int fds[2];
    T_ASSERT_EQ(t_socket_pair(fds), 0);

    t_evloop *loop1 = t_evloop_create();
    t_conn *c1 = t_conn_create(fds[0], loop1);
    t_conn *s1 = t_conn_create(fds[1], loop1);
    t_conn_set_on_msg(s1, on_msg, NULL);
    g_msgs_recv = 0;
    size_t plen1 = 3;
    t_proto_msg m1;
    t_proto_header_init(&m1.header, T_MSG_POST, plen1);
    m1.payload = (uint8_t *)malloc(plen1);
    memcpy(m1.payload, "aaa", plen1);
    m1.payload_len = plen1;
    t_thread t1;
    T_ASSERT_EQ(t_thread_spawn(&t1, loop_fn, loop1), 0);
    t_time_sleep_us(10000);
    t_conn_send(c1, &m1);
    free(m1.payload);
    T_ASSERT_EQ(t_thread_join(&t1), 0);
    T_ASSERT_EQ(g_msgs_recv, (size_t)1);

    t_conn_destroy(c1);
    t_conn_destroy(s1);
    t_evloop_destroy(loop1);

    t_socket_close(fds[0]);
    t_socket_close(fds[1]);

    int fds2[2];
    T_ASSERT_EQ(t_socket_pair(fds2), 0);
    t_evloop *loop2 = t_evloop_create();
    t_conn *c2 = t_conn_create(fds2[0], loop2);
    t_conn *s2 = t_conn_create(fds2[1], loop2);
    t_conn_set_on_msg(s2, on_msg, NULL);
    g_msgs_recv = 0;
    size_t plen2 = 3;
    t_proto_msg m2;
    t_proto_header_init(&m2.header, T_MSG_POST, plen2);
    m2.payload = (uint8_t *)malloc(plen2);
    memcpy(m2.payload, "bbb", plen2);
    m2.payload_len = plen2;
    t_thread t2;
    T_ASSERT_EQ(t_thread_spawn(&t2, loop_fn, loop2), 0);
    t_time_sleep_us(10000);
    t_conn_send(c2, &m2);
    free(m2.payload);
    T_ASSERT_EQ(t_thread_join(&t2), 0);
    T_ASSERT_EQ(g_msgs_recv, (size_t)1);

    t_conn_destroy(c2);
    t_conn_destroy(s2);
    t_evloop_destroy(loop2);
}

int main(void) {
    return t_run_all_tests();
}
