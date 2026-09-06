#include "t_test.h"
#include "t_coro.h"

static t_coro *require_coro(t_coro_fn fn, void *arg) {
    t_coro *c = t_coro_create(fn, arg, T_CORO_DEFAULT_STACK);
    if (!c) {
        T_ASSERT_EQ((int)T_HAVE_CORO_ASM, 0);
        return NULL;
    }
    T_ASSERT_EQ((int)T_HAVE_CORO_ASM, 1);
    return c;
}

static void simple_coro_fn(void *arg) {
    int *p = (int *)arg;
    *p = 1;
    t_coro_yield();
    *p = 2;
    t_coro_yield();
    *p = 3;
}

T_TEST(coro_create_destroy) {
    t_coro *c = require_coro(simple_coro_fn, NULL);
    if (!c) return;
    T_ASSERT_EQ(t_coro_get_state(c), T_CORO_READY);
    t_coro_destroy(c);
}

T_TEST(coro_basic_resume_yield) {
    int val = 0;
    t_coro *c = require_coro(simple_coro_fn, &val);
    if (!c) return;
    T_ASSERT_EQ(t_coro_get_state(c), T_CORO_READY);

    t_coro_resume(c);
    T_ASSERT_EQ(val, 1);
    T_ASSERT_EQ(t_coro_get_state(c), T_CORO_SUSPENDED);

    t_coro_resume(c);
    T_ASSERT_EQ(val, 2);
    T_ASSERT_EQ(t_coro_get_state(c), T_CORO_SUSPENDED);

    t_coro_resume(c);
    T_ASSERT_EQ(val, 3);
    T_ASSERT_EQ(t_coro_get_state(c), T_CORO_DEAD);

    t_coro_destroy(c);
}

static void no_yield_coro_fn(void *arg) {
    int *p = (int *)arg;
    *p = 42;
}

T_TEST(coro_no_yield) {
    int val = 0;
    t_coro *c = require_coro(no_yield_coro_fn, &val);
    if (!c) return;
    t_coro_resume(c);
    T_ASSERT_EQ(val, 42);
    T_ASSERT_EQ(t_coro_get_state(c), T_CORO_DEAD);
    t_coro_destroy(c);
}

static void add_coro_fn(void *arg) {
    int *p = (int *)arg;
    *p += 10;
    t_coro_yield();
    *p += 20;
}

T_TEST(coro_multiple) {
    int v1 = 0, v2 = 0;
    t_coro *c1 = require_coro(add_coro_fn, &v1);
    t_coro *c2 = require_coro(add_coro_fn, &v2);
    if (!c1 || !c2) {
        if (c1) t_coro_destroy(c1);
        if (c2) t_coro_destroy(c2);
        return;
    }

    t_coro_resume(c1);
    t_coro_resume(c2);
    T_ASSERT_EQ(v1, 10);
    T_ASSERT_EQ(v2, 10);

    t_coro_resume(c1);
    t_coro_resume(c2);
    T_ASSERT_EQ(v1, 30);
    T_ASSERT_EQ(v2, 30);

    T_ASSERT_EQ(t_coro_get_state(c1), T_CORO_DEAD);
    T_ASSERT_EQ(t_coro_get_state(c2), T_CORO_DEAD);
    t_coro_destroy(c1);
    t_coro_destroy(c2);
}

static void *g_arg_result;

static void arg_check_fn(void *arg) {
    g_arg_result = arg;
    t_coro *c = t_coro_current();
    (void)c;
}

T_TEST(coro_current_and_arg) {
    int secret = 999;
    g_arg_result = NULL;
    t_coro *c = require_coro(arg_check_fn, &secret);
    if (!c) return;
    t_coro_resume(c);
    T_ASSERT_NOT_NULL(g_arg_result);
    T_ASSERT_EQ(*(int *)g_arg_result, 999);
    t_coro_destroy(c);
}

static void deep_yield_fn(void *arg) {
    int *p = (int *)arg;
    for (int i = 0; i < 5; i++) {
        *p = i;
        t_coro_yield();
    }
}

T_TEST(coro_deep_yield) {
    int val = -1;
    t_coro *c = require_coro(deep_yield_fn, &val);
    if (!c) return;
    for (int i = 0; i < 5; i++) {
        t_coro_resume(c);
        T_ASSERT_EQ(val, i);
    }
    t_coro_resume(c);
    T_ASSERT_EQ(t_coro_get_state(c), T_CORO_DEAD);
    t_coro_destroy(c);
}

T_TEST(coro_resume_dead_returns_error) {
    int val = 0;
    t_coro *c = require_coro(no_yield_coro_fn, &val);
    if (!c) return;
    t_coro_resume(c);
    T_ASSERT_EQ(t_coro_get_state(c), T_CORO_DEAD);
    T_ASSERT_EQ(t_coro_resume(c), -1);
    t_coro_destroy(c);
}

int main(void) {
    return t_run_all_tests();
}
