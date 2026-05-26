#include "t_test.h"
#include "t_tpool.h"
#include "t_atomic.h"
#include "t_time.h"

static t_atomic_int g_task_counter;

static void reset_counter(void) {
    t_atomic_store_int(&g_task_counter, 0);
}

static void increment_task(void *ctx) {
    (void)ctx;
    t_atomic_fetch_add_int(&g_task_counter, 1);
}

static void sum_task(void *ctx) {
    int *val = (int *)ctx;
    *val += 100;
}

T_TEST(tpool_create_destroy) {
    t_tpool *pool = t_tpool_create(4);
    T_ASSERT_NOT_NULL(pool);
    T_ASSERT_EQ((int)t_tpool_worker_count(pool), 4);
    t_tpool_destroy(pool);
}

T_TEST(tpool_default_threads) {
    t_tpool *pool = t_tpool_create(0);
    T_ASSERT_NOT_NULL(pool);
    T_ASSERT(t_tpool_worker_count(pool) > 0);
    t_tpool_destroy(pool);
}

T_TEST(tpool_submit_single) {
    reset_counter();
    t_tpool *pool = t_tpool_create(2);
    t_tpool_submit(pool, increment_task, NULL);
    t_tpool_wait(pool);
    T_ASSERT_EQ(t_atomic_load_int(&g_task_counter), 1);
    t_tpool_destroy(pool);
}

T_TEST(tpool_submit_many) {
    reset_counter();
    t_tpool *pool = t_tpool_create(4);
    for (int i = 0; i < 1000; i++)
        t_tpool_submit(pool, increment_task, NULL);
    t_tpool_wait(pool);
    T_ASSERT_EQ(t_atomic_load_int(&g_task_counter), 1000);
    t_tpool_destroy(pool);
}

T_TEST(tpool_task_with_context) {
    t_tpool *pool = t_tpool_create(4);
    int values[10];
    for (int i = 0; i < 10; i++) values[i] = i;
    for (int i = 0; i < 10; i++)
        t_tpool_submit(pool, sum_task, &values[i]);
    t_tpool_wait(pool);
    for (int i = 0; i < 10; i++)
        T_ASSERT_EQ(values[i], i + 100);
    t_tpool_destroy(pool);
}

T_TEST(tpool_tasks_completed_stat) {
    reset_counter();
    t_tpool *pool = t_tpool_create(2);
    for (int i = 0; i < 100; i++)
        t_tpool_submit(pool, increment_task, NULL);
    t_tpool_wait(pool);
    T_ASSERT_EQ((int)t_tpool_tasks_completed(pool), 100);
    t_tpool_destroy(pool);
}

T_TEST(tpool_submit_to) {
    reset_counter();
    t_tpool *pool = t_tpool_create(4);
    for (int i = 0; i < 50; i++)
        t_tpool_submit_to(pool, i % 4, increment_task, NULL);
    t_tpool_wait(pool);
    T_ASSERT_EQ(t_atomic_load_int(&g_task_counter), 50);
    T_ASSERT_EQ((int)t_tpool_tasks_completed(pool), 50);
    t_tpool_destroy(pool);
}

int main(void) {
    return t_run_all_tests();
}
