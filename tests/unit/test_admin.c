#include "t_test.h"
#include "t_evloop.h"
#include "t_admin.h"
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <netinet/in.h>
#include <pthread.h>

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

    pthread_t t;
    pthread_create(&t, NULL, loop_runner, loop);
    usleep(20000);

    int s = socket(AF_INET, SOCK_STREAM, 0);
    T_ASSERT(s >= 0);
    struct sockaddr_in sa;
    memset(&sa, 0, sizeof(sa));
    sa.sin_family = AF_INET;
    sa.sin_port = htons((uint16_t)port);
    inet_pton(AF_INET, "127.0.0.1", &sa.sin_addr);
    int connected = 0;
    for (int i = 0; i < 50 && !connected; i++) {
        if (connect(s, (struct sockaddr *)&sa, sizeof(sa)) == 0) connected = 1;
        else usleep(10000);
    }
    T_ASSERT(connected);

    const char *req = "GET /stats HTTP/1.1\r\nHost: localhost\r\n\r\n";
    T_ASSERT_EQ(write(s, req, strlen(req)), (ssize_t)strlen(req));

    char buf[4096];
    size_t total = 0;
    ssize_t n;
    while ((n = read(s, buf + total, sizeof(buf) - total - 1)) > 0) {
        total += (size_t)n;
        if (total >= sizeof(buf) - 1) break;
        if (memchr(buf, '}', total)) break;
    }
    buf[total] = '\0';
    T_ASSERT(strstr(buf, "transit") != NULL);
    T_ASSERT(strstr(buf, "version") != NULL);
    close(s);

    pthread_join(t, NULL);
    t_admin_stop(admin);
    t_admin_destroy(admin);
    t_evloop_destroy(loop);
}

int main(void) {
    return t_run_all_tests();
}
