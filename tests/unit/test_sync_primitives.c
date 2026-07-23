#include "t_test.h"
#include "t_mutex.h"
#include "t_spinlock.h"
#include "t_rwlock.h"
#include "t_atomic.h"
#include <pthread.h>

static t_atomic_int g_counter = T_ATOMIC_INIT(0);
static t_mutex g_test_mutex;
static t_spinlock g_test_spinlock;
static t_rwlock g_test_rwlock;

#define TDD_THREAD_COUNT 4
#define TDD_ITERATIONS 10000

static void *mutex_thread_fn(void *arg) {
    (void)arg;
    for (int i = 0; i < TDD_ITERATIONS; i++) {
        t_mutex_lock(&g_test_mutex);
        t_atomic_fetch_add_int(&g_counter, 1);
        t_mutex_unlock(&g_test_mutex);
    }
    return NULL;
}

T_TEST(mutex_protects_counter) {
    t_atomic_int store = T_ATOMIC_INIT(0);
    g_counter = store;
    t_mutex_init(&g_test_mutex);
    pthread_t threads[TDD_THREAD_COUNT];
    for (int i = 0; i < TDD_THREAD_COUNT; i++)
        pthread_create(&threads[i], NULL, mutex_thread_fn, NULL);
    for (int i = 0; i < TDD_THREAD_COUNT; i++)
        pthread_join(threads[i], NULL);
    t_mutex_destroy(&g_test_mutex);
    T_ASSERT_EQ(t_atomic_load_int(&g_counter), TDD_THREAD_COUNT * TDD_ITERATIONS);
}

static void *spinlock_thread_fn(void *arg) {
    (void)arg;
    for (int i = 0; i < TDD_ITERATIONS; i++) {
        t_spinlock_lock(&g_test_spinlock);
        t_atomic_fetch_add_int(&g_counter, 1);
        t_spinlock_unlock(&g_test_spinlock);
    }
    return NULL;
}

T_TEST(spinlock_protects_counter) {
    t_atomic_int store = T_ATOMIC_INIT(0);
    g_counter = store;
    t_spinlock_init(&g_test_spinlock);
    pthread_t threads[TDD_THREAD_COUNT];
    for (int i = 0; i < TDD_THREAD_COUNT; i++)
        pthread_create(&threads[i], NULL, spinlock_thread_fn, NULL);
    for (int i = 0; i < TDD_THREAD_COUNT; i++)
        pthread_join(threads[i], NULL);
    T_ASSERT_EQ(t_atomic_load_int(&g_counter), TDD_THREAD_COUNT * TDD_ITERATIONS);
}

static void *rwlock_read_thread_fn(void *arg) {
    (void)arg;
    for (int i = 0; i < TDD_ITERATIONS; i++) {
        t_rwlock_read_lock(&g_test_rwlock);
        volatile int v = t_atomic_load_int(&g_counter);
        (void)v;
        t_rwlock_read_unlock(&g_test_rwlock);
    }
    return NULL;
}

static void *rwlock_write_thread_fn(void *arg) {
    (void)arg;
    for (int i = 0; i < TDD_ITERATIONS; i++) {
        t_rwlock_write_lock(&g_test_rwlock);
        t_atomic_fetch_add_int(&g_counter, 1);
        t_rwlock_write_unlock(&g_test_rwlock);
    }
    return NULL;
}

T_TEST(rwlock_allows_concurrent_reads) {
    t_atomic_int store = T_ATOMIC_INIT(0);
    g_counter = store;
    t_rwlock_init(&g_test_rwlock);
    pthread_t readers[2];
    pthread_t writers[2];
    for (int i = 0; i < 2; i++)
        pthread_create(&readers[i], NULL, rwlock_read_thread_fn, NULL);
    for (int i = 0; i < 2; i++)
        pthread_create(&writers[i], NULL, rwlock_write_thread_fn, NULL);
    for (int i = 0; i < 2; i++)
        pthread_join(readers[i], NULL);
    for (int i = 0; i < 2; i++)
        pthread_join(writers[i], NULL);
    t_rwlock_destroy(&g_test_rwlock);
    T_ASSERT_EQ(t_atomic_load_int(&g_counter), 2 * TDD_ITERATIONS);
}

T_TEST(spinlock_trylock) {
    t_spinlock s = T_SPINLOCK_INIT;
    T_ASSERT(t_spinlock_trylock(&s));
    T_ASSERT(!t_spinlock_trylock(&s));
    t_spinlock_unlock(&s);
    T_ASSERT(t_spinlock_trylock(&s));
    t_spinlock_unlock(&s);
}

T_TEST(mutex_trylock) {
    t_mutex m;
    t_mutex_init(&m);
    T_ASSERT(t_mutex_trylock(&m));
    T_ASSERT(!t_mutex_trylock(&m));
    t_mutex_unlock(&m);
    T_ASSERT(t_mutex_trylock(&m));
    t_mutex_unlock(&m);
    t_mutex_destroy(&m);
}

int main(void) {
    return t_run_all_tests();
}
