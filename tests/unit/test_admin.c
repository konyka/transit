#include "t_test.h"
#include "t_evloop.h"
#include "t_admin.h"
#include "t_socket.h"
#include "t_thread.h"
#include "t_time.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static void timer_stop(void *ud) {
    t_evloop_stop((t_evloop *)ud);
}

static void *loop_runner(void *arg) {
    t_evloop *loop = (t_evloop *)arg;
    t_evloop_timer_add(loop, 500, 0, timer_stop, loop);
    t_evloop_run(loop, 100);
    return NULL;
}

T_TEST(admin_create_destroy) {
    t_evloop *loop = t_evloop_create();
    T_ASSERT_NOT_NULL(loop);
    t_admin *admin = t_admin_create(loop, NULL, 0);
    T_ASSERT_NOT_NULL(admin);
    t_admin_destroy(admin);
    t_evloop_destroy(loop);
}

T_TEST(admin_start_stop) {
    t_evloop *loop = t_evloop_create();
    T_ASSERT_NOT_NULL(loop);
    t_admin *admin = t_admin_create(loop, "127.0.0.1", 0);
    T_ASSERT_NOT_NULL(admin);
    T_ASSERT_EQ(t_admin_start(admin), 0);
    T_ASSERT(t_admin_is_running(admin));
    T_ASSERT(t_admin_port(admin) > 0);
    t_admin_stop(admin);
    T_ASSERT(!t_admin_is_running(admin));
    t_admin_destroy(admin);
    t_evloop_destroy(loop);
}

T_TEST(admin_stats_endpoint) {
    t_evloop *loop = t_evloop_create();
    T_ASSERT_NOT_NULL(loop);
    t_admin *admin = t_admin_create(loop, "127.0.0.1", 0);
    T_ASSERT_NOT_NULL(admin);
    T_ASSERT_EQ(t_admin_start(admin), 0);
    int port = t_admin_port(admin);
    T_ASSERT(port > 0);

    t_thread t;
    T_ASSERT_EQ(t_thread_spawn(&t, loop_runner, loop), 0);
    t_time_sleep_us(20000);

    int s = t_socket_dial_ipv4("127.0.0.1", (uint16_t)port);
    T_ASSERT(s >= 0);
    T_ASSERT_EQ(t_socket_set_block(s), 0);

    const char *req = "GET /stats HTTP/1.1\r\nHost: localhost\r\n\r\n";
    T_ASSERT_EQ((int)t_socket_write(s, req, strlen(req)), (int)strlen(req));

    char buf[4096];
    size_t total = 0;
    ssize_t n;
    while ((n = t_socket_read(s, buf + total, sizeof(buf) - total - 1)) > 0) {
        total += (size_t)n;
        if (total >= sizeof(buf) - 1) break;
        if (memchr(buf, '}', total)) break;
    }
    buf[total] = '\0';
    T_ASSERT(strstr(buf, "transit") != NULL);
    T_ASSERT(strstr(buf, "version") != NULL);
    t_socket_close(s);

    T_ASSERT_EQ(t_thread_join(&t), 0);
    t_admin_stop(admin);
    t_admin_destroy(admin);
    t_evloop_destroy(loop);
}

int main(void) {
    return t_run_all_tests();
}
