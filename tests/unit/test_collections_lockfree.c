#include "t_test.h"
#include "t_ringbuf.h"
#include "t_mpmc.h"
#include "t_pqueue.h"
#include "t_atomic.h"
#include <string.h>

/* Tests */
T_TEST(ringbuf_init_destroy) {
    t_ringbuf rb;
    T_ASSERT_EQ(t_ringbuf_init(&rb, 1024), 0);
    T_ASSERT(rb.buf != NULL);
    t_ringbuf_destroy(&rb);
}

T_TEST(ringbuf_write_read) {
    t_ringbuf rb;
    t_ringbuf_init(&rb, 256);
    const char *msg = "hello";
    size_t written = t_ringbuf_write(&rb, msg, 5);
    T_ASSERT_EQ((int)written, 5);
    T_ASSERT_EQ((int)t_ringbuf_used(&rb), 5);
    char buf[16] = {0};
    size_t read = t_ringbuf_read(&rb, buf, 5);
    T_ASSERT_EQ((int)read, 5);
    T_ASSERT(memcmp(buf, "hello", 5) == 0);
    T_ASSERT_EQ((int)t_ringbuf_used(&rb), 0);
    t_ringbuf_destroy(&rb);
}

T_TEST(ringbuf_full) {
    t_ringbuf rb;
    t_ringbuf_init(&rb, 64);
    char data[128];
    memset(data, 'A', 128);
    size_t w = t_ringbuf_write(&rb, data, 128);
    T_ASSERT(w > 0);
    T_ASSERT(w < 128);
    t_ringbuf_destroy(&rb);
}

T_TEST(ringbuf_empty_read) {
    t_ringbuf rb;
    t_ringbuf_init(&rb, 64);
    char buf[16];
    T_ASSERT_EQ((int)t_ringbuf_read(&rb, buf, 16), 0);
    t_ringbuf_destroy(&rb);
}

T_TEST(ringbuf_wrap_around) {
    t_ringbuf rb;
    t_ringbuf_init(&rb, 64);
    char data[33];
    memset(data, 'X', 33);
    t_ringbuf_write(&rb, data, 33);
    t_ringbuf_read(&rb, data, 33);
    T_ASSERT_EQ((int)t_ringbuf_used(&rb), 0);
    t_ringbuf_write(&rb, data, 33);
    T_ASSERT_EQ((int)t_ringbuf_used(&rb), 33);
    t_ringbuf_destroy(&rb);
}

T_TEST(ringbuf_reset) {
    t_ringbuf rb;
    t_ringbuf_init(&rb, 64);
    char data[32] = {0};
    t_ringbuf_write(&rb, data, 32);
    t_ringbuf_reset(&rb);
    T_ASSERT_EQ((int)t_ringbuf_used(&rb), 0);
    t_ringbuf_destroy(&rb);
}

T_TEST(mpmc_init_destroy) {
    t_mpmc q;
    T_ASSERT_EQ(t_mpmc_init(&q, 16), 0);
    t_mpmc_destroy(&q);
}

T_TEST(mpmc_push_pop) {
    t_mpmc q;
    t_mpmc_init(&q, 16);
    int val = 42;
    T_ASSERT(t_mpmc_push(&q, &val));
    void *out = NULL;
    T_ASSERT(t_mpmc_pop(&q, &out));
    T_ASSERT(out == &val);
    t_mpmc_destroy(&q);
}

T_TEST(mpmc_full) {
    t_mpmc q;
    t_mpmc_init(&q, 4);
    int vals[4];
    for (int i = 0; i < 4; i++) {
        T_ASSERT(t_mpmc_push(&q, &vals[i]));
    }
    T_ASSERT(!t_mpmc_push(&q, (void*)0x1));
    t_mpmc_destroy(&q);
}

T_TEST(mpmc_empty) {
    t_mpmc q;
    t_mpmc_init(&q, 4);
    void *out;
    T_ASSERT(!t_mpmc_pop(&q, &out));
    t_mpmc_destroy(&q);
}

T_TEST(mpmc_fifo_order) {
    t_mpmc q;
    t_mpmc_init(&q, 16);
    int vals[] = {1, 2, 3, 4, 5};
    for (int i = 0; i < 5; i++) t_mpmc_push(&q, &vals[i]);
    for (int i = 0; i < 5; i++) {
        void *out;
        T_ASSERT(t_mpmc_pop(&q, &out));
        T_ASSERT_EQ(*(int*)out, vals[i]);
    }
    t_mpmc_destroy(&q);
}

T_TEST(mpmc_single_thread_bulk) {
    t_mpmc q;
    t_mpmc_init(&q, 1024);
    int values[1024];
    for (int i = 0; i < 1024; i++) {
        values[i] = i;
        while (!t_mpmc_push(&q, &values[i]))
            ;
    }
    int got = 0;
    void *out;
    while (t_mpmc_pop(&q, &out)) {
        got++;
    }
    T_ASSERT_EQ(got, 1024);
    t_mpmc_destroy(&q);
}

T_TEST(pqueue_init_destroy) {
    t_pqueue pq;
    T_ASSERT_EQ(t_pqueue_init(&pq, 16), 0);
    T_ASSERT_EQ((int)t_pqueue_len(&pq), 0);
    t_pqueue_destroy(&pq);
}

T_TEST(pqueue_push_pop) {
    t_pqueue pq;
    t_pqueue_init(&pq, 16);
    t_pqueue_push(&pq, 3, (void*)"c");
    t_pqueue_push(&pq, 1, (void*)"a");
    t_pqueue_push(&pq, 2, (void*)"b");
    T_ASSERT_EQ((int)t_pqueue_len(&pq), 3);
    t_pq_entry e;
    T_ASSERT_EQ(t_pqueue_pop(&pq, &e), 0);
    T_ASSERT_EQ((int)e.priority, 1);
    T_ASSERT_STR_EQ((char*)e.data, "a");
    T_ASSERT_EQ(t_pqueue_pop(&pq, &e), 0);
    T_ASSERT_EQ((int)e.priority, 2);
    T_ASSERT_EQ(t_pqueue_pop(&pq, &e), 0);
    T_ASSERT_EQ((int)e.priority, 3);
    T_ASSERT_EQ((int)t_pqueue_len(&pq), 0);
    t_pqueue_destroy(&pq);
}

T_TEST(pqueue_peek) {
    t_pqueue pq;
    t_pqueue_init(&pq, 16);
    t_pqueue_push(&pq, 5, (void*)"low");
    t_pqueue_push(&pq, 1, (void*)"high");
    t_pq_entry e;
    T_ASSERT_EQ(t_pqueue_peek(&pq, &e), 0);
    T_ASSERT_EQ((int)e.priority, 1);
    T_ASSERT_EQ((int)t_pqueue_len(&pq), 2);
    t_pqueue_destroy(&pq);
}

T_TEST(pqueue_many) {
    t_pqueue pq;
    t_pqueue_init(&pq, 4);
    for (int i = 1000; i >= 0; i--) t_pqueue_push(&pq, i, NULL);
    T_ASSERT_EQ((int)t_pqueue_len(&pq), 1001);
    t_pq_entry e;
    int64_t prev = -1;
    while (t_pqueue_pop(&pq, &e) == 0) {
        T_ASSERT(e.priority > prev);
        prev = e.priority;
    }
    t_pqueue_destroy(&pq);
}

T_TEST(pqueue_clear) {
    t_pqueue pq;
    t_pqueue_init(&pq, 16);
    t_pqueue_push(&pq, 1, NULL);
    t_pqueue_push(&pq, 2, NULL);
    t_pqueue_clear(&pq);
    T_ASSERT_EQ((int)t_pqueue_len(&pq), 0);
    t_pqueue_destroy(&pq);
}

int main(void) {
    return t_run_all_tests();
}
