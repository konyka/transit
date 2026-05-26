#include "t_test.h"
#include "t_evloop.h"
#include "t_timer.h"
#include "t_time.h"
#include <unistd.h>
#include <string.h>

static void timer_on_fire(void *ud) {
    int *p = (int *)ud;
    *p = 1;
}

static int g_timer_order[4];
static int g_timer_idx;

static void timer_cb1(void *ud) { (void)ud; g_timer_order[g_timer_idx++] = 1; }
static void timer_cb2(void *ud) { (void)ud; g_timer_order[g_timer_idx++] = 2; }
static void timer_cb3(void *ud) { (void)ud; g_timer_order[g_timer_idx++] = 3; }

static void evloop_timer_wrap(void *ud) {
    struct { t_evloop *loop; int *fired; } *ctx = ud;
    *(ctx->fired) = 1;
    t_evloop_stop(ctx->loop);
}

static void evloop_on_read(t_evio *io, int flags, void *ud) {
    (void)io;
    (void)flags;
    struct { t_evloop *loop; int *done; } *ctx = ud;
    *(ctx->done) = 1;
    t_evloop_stop(ctx->loop);
}

T_TEST(timer_create_destroy) {
    t_timer *t = t_timer_create();
    T_ASSERT_NOT_NULL(t);
    t_timer_destroy(t);
}

T_TEST(timer_add_cancel) {
    t_timer *t = t_timer_create();
    int dummy = 0;
    int64_t id = t_timer_add(t, 1000, 0, timer_on_fire, &dummy);
    T_ASSERT(id >= 0);
    T_ASSERT_EQ((int)t_timer_count(t), 1);
    t_timer_cancel(t, id);
    T_ASSERT_EQ((int)t_timer_count(t), 0);
    t_timer_destroy(t);
}

T_TEST(timer_process_immediate) {
    t_timer *t = t_timer_create();
    int fired = 0;
    t_timer_add(t, 0, 0, timer_on_fire, &fired);
    t_time_sleep_ms(1);
    t_timer_process(t);
    T_ASSERT_EQ(fired, 1);
    t_timer_destroy(t);
}

T_TEST(timer_multiple) {
    t_timer *t = t_timer_create();
    g_timer_idx = 0;
    memset(g_timer_order, 0, sizeof(g_timer_order));
    t_timer_add(t, 200, 0, timer_cb3, NULL);
    t_timer_add(t, 0, 0, timer_cb1, NULL);
    t_timer_add(t, 50, 0, timer_cb2, NULL);
    t_time_sleep_ms(300);
    t_timer_process(t);
    T_ASSERT_EQ(g_timer_order[0], 1);
    T_ASSERT_EQ(g_timer_order[1], 2);
    T_ASSERT_EQ(g_timer_order[2], 3);
    t_timer_destroy(t);
}

static int g_repeat_count;

static void on_repeat(void *ud) {
    (void)ud;
    g_repeat_count++;
}

T_TEST(timer_repeat) {
    t_timer *t = t_timer_create();
    g_repeat_count = 0;
    t_timer_add(t, 0, 10, on_repeat, NULL);
    t_time_sleep_ms(1);
    t_timer_process(t);
    T_ASSERT(g_repeat_count >= 1);
    t_time_sleep_ms(15);
    t_timer_process(t);
    T_ASSERT(g_repeat_count >= 2);
    t_timer_destroy(t);
}

T_TEST(evloop_create_destroy) {
    t_evloop *loop = t_evloop_create();
    T_ASSERT_NOT_NULL(loop);
    T_ASSERT(!t_evloop_is_running(loop));
    t_evloop_destroy(loop);
}

T_TEST(evloop_timer_basic) {
    t_evloop *loop = t_evloop_create();
    T_ASSERT_NOT_NULL(loop);
    int fired = 0;
    struct { t_evloop *loop; int *fired; } ctx = { loop, &fired };
    t_evloop_timer_add(loop, 5, 0, evloop_timer_wrap, &ctx);
    t_evloop_run(loop, 1000);
    T_ASSERT_EQ(fired, 1);
    t_evloop_destroy(loop);
}

T_TEST(evloop_io_pipe) {
    t_evloop *loop = t_evloop_create();
    T_ASSERT_NOT_NULL(loop);
    int pfd[2];
    T_ASSERT_EQ(pipe(pfd), 0);
    int done = 0;
    struct { t_evloop *loop; int *done; } ctx = { loop, &done };
    t_evio io = { .fd = pfd[0], .callback = evloop_on_read, .user_data = &ctx, .loop = loop, .events = 0 };
    T_ASSERT_EQ(t_evloop_add(loop, &io, T_EV_READ), 0);
    char c = 'x';
    write(pfd[1], &c, 1);
    t_evloop_run(loop, 1000);
    T_ASSERT_EQ(done, 1);
    t_evloop_del(loop, &io);
    close(pfd[0]);
    close(pfd[1]);
    t_evloop_destroy(loop);
}

T_TEST(evloop_stop) {
    t_evloop *loop = t_evloop_create();
    T_ASSERT_NOT_NULL(loop);
    int fired = 0;
    struct { t_evloop *loop; int *fired; } ctx = { loop, &fired };
    t_evloop_timer_add(loop, 1, 0, evloop_timer_wrap, &ctx);
    t_evloop_run(loop, 5000);
    T_ASSERT(!t_evloop_is_running(loop));
    T_ASSERT_EQ(fired, 1);
    t_evloop_destroy(loop);
}

static void ensure_all_callbacks_referenced(void) {
    (void)timer_on_fire;
    (void)timer_cb1;
    (void)timer_cb2;
    (void)timer_cb3;
    (void)evloop_timer_wrap;
    (void)evloop_on_read;
    (void)on_repeat;
}

int main(void) {
    ensure_all_callbacks_referenced();
    return t_run_all_tests();
}
