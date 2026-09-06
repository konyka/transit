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

static int admin_http_get(uint16_t port, const char *path, char *buf, size_t cap) {
    if (!path || !buf || cap < 2) return -1;
    int s = t_socket_dial_ipv4("127.0.0.1", port);
    if (s < 0) return -1;
    if (t_socket_set_block(s) != 0) {
        t_socket_close(s);
        return -1;
    }
    char req[256];
    int nreq = snprintf(req, sizeof(req),
                        "GET %s HTTP/1.1\r\nHost: localhost\r\n\r\n", path);
    if (nreq < 0 || (size_t)nreq >= sizeof(req) ||
        t_socket_write(s, req, (size_t)nreq) != nreq) {
        t_socket_close(s);
        return -1;
    }
    size_t total = 0;
    ssize_t n;
    while ((n = t_socket_read(s, buf + total, cap - total - 1)) > 0) {
        total += (size_t)n;
        if (total >= cap - 1) break;
        if (memchr(buf, '}', total)) break;
    }
    t_socket_close(s);
    buf[total] = '\0';
    return (int)total;
}

static void on_ready(t_admin_stats *stats, void *ud) {
    (void)ud;
    stats->ready = 1;
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

    char buf[4096];
    T_ASSERT(admin_http_get((uint16_t)port, "/stats", buf, sizeof(buf)) > 0);
    T_ASSERT(strstr(buf, "transit") != NULL);
    T_ASSERT(strstr(buf, "version") != NULL);
    T_ASSERT(strstr(buf, "\"ready\":false") != NULL);

    T_ASSERT_EQ(t_thread_join(&t), 0);
    t_admin_stop(admin);
    t_admin_destroy(admin);
    t_evloop_destroy(loop);
}

T_TEST(admin_health_endpoint) {
    t_evloop *loop = t_evloop_create();
    t_admin *admin = t_admin_create(loop, "127.0.0.1", 0);
    T_ASSERT_EQ(t_admin_start(admin), 0);
    t_thread t;
    T_ASSERT_EQ(t_thread_spawn(&t, loop_runner, loop), 0);
    t_time_sleep_us(20000);
    char buf[4096];
    T_ASSERT(admin_http_get((uint16_t)t_admin_port(admin), "/health",
                            buf, sizeof(buf)) > 0);
    T_ASSERT(strstr(buf, "HTTP/1.1 200") != NULL);
    T_ASSERT(strstr(buf, "\"status\":\"ok\"") != NULL);
    T_ASSERT_EQ(t_thread_join(&t), 0);
    t_admin_stop(admin);
    t_admin_destroy(admin);
    t_evloop_destroy(loop);
}

T_TEST(admin_ready_fail_closed) {
    t_evloop *loop = t_evloop_create();
    t_admin *admin = t_admin_create(loop, "127.0.0.1", 0);
    T_ASSERT_EQ(t_admin_start(admin), 0);
    t_thread t;
    T_ASSERT_EQ(t_thread_spawn(&t, loop_runner, loop), 0);
    t_time_sleep_us(20000);
    char buf[4096];
    T_ASSERT(admin_http_get((uint16_t)t_admin_port(admin), "/ready",
                            buf, sizeof(buf)) > 0);
    T_ASSERT(strstr(buf, "HTTP/1.1 503") != NULL);
    T_ASSERT(strstr(buf, "\"ready\":false") != NULL);
    T_ASSERT_EQ(t_thread_join(&t), 0);
    t_admin_stop(admin);
    t_admin_destroy(admin);
    t_evloop_destroy(loop);
}

T_TEST(admin_ready_when_marked) {
    t_evloop *loop = t_evloop_create();
    t_admin *admin = t_admin_create(loop, "127.0.0.1", 0);
    t_admin_set_stats_cb(admin, on_ready, NULL);
    T_ASSERT_EQ(t_admin_start(admin), 0);
    t_thread t;
    T_ASSERT_EQ(t_thread_spawn(&t, loop_runner, loop), 0);
    t_time_sleep_us(20000);
    char buf[4096];
    T_ASSERT(admin_http_get((uint16_t)t_admin_port(admin), "/ready",
                            buf, sizeof(buf)) > 0);
    T_ASSERT(strstr(buf, "HTTP/1.1 200") != NULL);
    T_ASSERT(strstr(buf, "\"ready\":true") != NULL);
    T_ASSERT_EQ(t_thread_join(&t), 0);
    t_admin_stop(admin);
    t_admin_destroy(admin);
    t_evloop_destroy(loop);
}

int main(void) {
    return t_run_all_tests();
}
