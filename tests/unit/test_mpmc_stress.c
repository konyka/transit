#include "t_test.h"
#include "t_mpmc.h"
#include "t_atomic.h"
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <stdint.h>
#include <sched.h>

#define STRESS_ITERS 50000
#define NUM_THREADS 2

typedef struct {
    t_mpmc      *q;
    t_atomic_int *total;
    t_atomic_int *done;
    int          count;
} thread_ctx;

static void *producer_fn(void *arg) {
    thread_ctx *ctx = (thread_ctx *)arg;
    for (int i = 0; i < ctx->count; i++) {
        t_mpmc_push(ctx->q, (void *)(uintptr_t)(i + 1));
    }
    return NULL;
}

static void *consumer_fn(void *arg) {
    thread_ctx *ctx = (thread_ctx *)arg;
    int local = 0;
    for (;;) {
        void *val;
        if (t_mpmc_pop(ctx->q, &val)) {
            local++;
        } else if (t_atomic_load_int(ctx->done)) {
            break;
        } else {
            sched_yield();
        }
    }
    t_atomic_fetch_add_int(ctx->total, local);
    return NULL;
}

T_TEST(mpmc_spsc_sequential) {
    t_mpmc q;
    T_ASSERT_EQ(t_mpmc_init(&q, 1024), 0);
    int produced = 0, consumed = 0;
    for (int i = 0; i < 10000; i++) {
        T_ASSERT(t_mpmc_push(&q, (void *)(uintptr_t)(i + 1)));
        produced++;
        void *val;
        if (t_mpmc_pop(&q, &val)) consumed++;
    }
    void *val;
    while (t_mpmc_pop(&q, &val)) consumed++;
    T_ASSERT_EQ(produced, 10000);
    T_ASSERT_EQ(consumed, 10000);
    t_mpmc_destroy(&q);
}

T_TEST(mpmc_mpmc_concurrent) {
    t_mpmc q;
    T_ASSERT_EQ(t_mpmc_init(&q, 131072), 0);
    t_atomic_int total;
    t_atomic_int done;
    t_atomic_store_int(&total, 0);
    t_atomic_store_int(&done, 0);

    thread_ctx pctx = { &q, &total, &done, STRESS_ITERS };
    thread_ctx cctx = { &q, &total, &done, 0 };

    pthread_t prod[NUM_THREADS], cons[NUM_THREADS];
    for (int i = 0; i < NUM_THREADS; i++) {
        pthread_create(&cons[i], NULL, consumer_fn, &cctx);
    }
    for (int i = 0; i < NUM_THREADS; i++) {
        pthread_create(&prod[i], NULL, producer_fn, &pctx);
    }
    for (int i = 0; i < NUM_THREADS; i++) {
        pthread_join(prod[i], NULL);
    }
    t_atomic_store_int(&done, 1);
    for (int i = 0; i < NUM_THREADS; i++) {
        pthread_join(cons[i], NULL);
    }

    void *val;
    while (t_mpmc_pop(&q, &val)) {
        t_atomic_fetch_add_int(&total, 1);
    }

    T_ASSERT_EQ(t_atomic_load_int(&total), NUM_THREADS * STRESS_ITERS);
    t_mpmc_destroy(&q);
}

T_TEST(mpmc_empty_pop_fails) {
    t_mpmc q;
    T_ASSERT_EQ(t_mpmc_init(&q, 16), 0);
    void *val;
    T_ASSERT(!t_mpmc_pop(&q, &val));
    t_mpmc_destroy(&q);
}

T_TEST(mpmc_full_push_fails) {
    t_mpmc q;
    T_ASSERT_EQ(t_mpmc_init(&q, 4), 0);
    for (int i = 0; i < 4; i++) {
        T_ASSERT(t_mpmc_push(&q, (void *)(uintptr_t)(i + 1)));
    }
    T_ASSERT(!t_mpmc_push(&q, (void *)999));
    t_mpmc_destroy(&q);
}

int main(void) {
    return t_run_all_tests();
}
