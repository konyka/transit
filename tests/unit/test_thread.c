#include "t_test.h"
#include "t_thread.h"
#include "t_atomic.h"

static void *inc_fn(void *arg) {
    t_atomic_int *n = (t_atomic_int *)arg;
    t_atomic_fetch_add_int(n, 1);
    return NULL;
}

static void *yield_then_inc(void *arg) {
    t_thread_yield();
    t_atomic_int *n = (t_atomic_int *)arg;
    t_atomic_fetch_add_int(n, 1);
    return NULL;
}

T_TEST(thread_null_args_fail_closed) {
    T_ASSERT_EQ(t_thread_spawn(NULL, inc_fn, NULL), -1);
    T_ASSERT_EQ(t_thread_join(NULL), -1);
    {
        t_thread th;
        T_ASSERT_EQ(t_thread_spawn(&th, NULL, NULL), -1);
    }
}

T_TEST(thread_spawn_join_increment) {
    t_atomic_int n = T_ATOMIC_INIT(0);
    t_thread th;
    T_ASSERT_EQ(t_thread_spawn(&th, inc_fn, &n), 0);
    T_ASSERT_EQ(t_thread_join(&th), 0);
    T_ASSERT_EQ(t_atomic_load_int(&n), 1);
}

T_TEST(thread_two_workers) {
    t_atomic_int n = T_ATOMIC_INIT(0);
    t_thread a, b;
    T_ASSERT_EQ(t_thread_spawn(&a, inc_fn, &n), 0);
    T_ASSERT_EQ(t_thread_spawn(&b, inc_fn, &n), 0);
    T_ASSERT_EQ(t_thread_join(&a), 0);
    T_ASSERT_EQ(t_thread_join(&b), 0);
    T_ASSERT_EQ(t_atomic_load_int(&n), 2);
}

T_TEST(thread_yield) {
    t_atomic_int n = T_ATOMIC_INIT(0);
    t_thread th;
    t_thread_yield();
    T_ASSERT_EQ(t_thread_spawn(&th, yield_then_inc, &n), 0);
    T_ASSERT_EQ(t_thread_join(&th), 0);
    T_ASSERT_EQ(t_atomic_load_int(&n), 1);
}

int main(void) {
    return t_run_all_tests();
}
