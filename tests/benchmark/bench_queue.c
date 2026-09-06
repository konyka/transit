#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include "t_thread.h"
#include <stdbool.h>
#include <string.h>
#include "t_time.h"
#include "t_test.h"

#define BENCH_ITERS 1000000

typedef struct {
    int *buf;
    size_t cap;
    size_t head;
    size_t tail;
} fifo_t;

static fifo_t* fifo_create(size_t cap){
    fifo_t* f = (fifo_t*)malloc(sizeof(fifo_t));
    f->cap = cap;
    f->buf = (int*)malloc(cap * sizeof(int));
    f->head = 0;
    f->tail = 0;
    return f;
}

static void fifo_free(fifo_t* f){ if(!f) return; free(f->buf); free(f); }

static void fifo_push(fifo_t* f, int v){ f->buf[f->tail % f->cap] = v; f->tail++; }

typedef struct { int *buf; size_t size; size_t count; } heap_t;

static heap_t* heap_create(size_t cap){ heap_t* h = (heap_t*)malloc(sizeof(heap_t)); h->buf = (int*)malloc(cap * sizeof(int)); h->size = 0; h->count = cap; return h; }
static void heap_free(heap_t* h){ if(!h) return; free(h->buf); free(h); }
static void heap_push(heap_t* h, int v){ if(h->size >= (size_t)h->count){ size_t nc = h->count * 2; h->buf = (int*)realloc(h->buf, nc * sizeof(int)); h->count = nc; } h->buf[h->size] = v; size_t i = h->size; h->size++; while(i>0){ size_t p = (i-1)/2; if(h->buf[p] <= h->buf[i]) break; int t = h->buf[p]; h->buf[p] = h->buf[i]; h->buf[i] = t; i = p; } }
static int heap_pop(heap_t* h){ int ret = h->buf[0]; h->buf[0] = h->buf[h->size-1]; h->size--; size_t i = 0; while(true){ size_t l = 2*i+1; size_t r = l+1; size_t m = i; if(l < h->size && h->buf[l] < h->buf[m]) m = l; if(r < h->size && h->buf[r] < h->buf[m]) m = r; if(m == i) break; int t = h->buf[i]; h->buf[i] = h->buf[m]; h->buf[m] = t; i = m; } return ret; }

typedef struct { int *buf; size_t len; } vec4_t;

static void* consumer(void* arg){ vec4_t* v = (vec4_t*)arg; volatile int dummy = 0; for(size_t i=0; i<v->len; ++i){ dummy = v->buf[i]; } (void)dummy; return NULL; }

static void bench_fifo_throughput(void){
    size_t cap = BENCH_ITERS * 2;
    fifo_t* f = fifo_create(cap);
    uint64_t t0 = t_time_now_ms();
    for(size_t i=0; i<BENCH_ITERS; ++i){ fifo_push(f, (int)i); }
    uint64_t t1 = t_time_now_ms();
    uint64_t dt = t1 - t0; if(dt == 0) dt = 1;
    uint64_t ops = BENCH_ITERS; uint64_t qps = (ops * 1000) / dt;
    printf("[bench_queue_fifo] iterations=%lu time_ms=%lu ops/sec=%lu\n", (unsigned long)BENCH_ITERS, (unsigned long)dt, (unsigned long)qps);
    fifo_free(f);
}

static void bench_priority_throughput(void){
    heap_t* h = heap_create(1024);
    uint64_t t0 = t_time_now_ms();
    for(size_t i=0; i<BENCH_ITERS; ++i){ heap_push(h, (int)(rand() & 0x7fffffff)); }
    for(size_t i=0; i<BENCH_ITERS; ++i){ (void)heap_pop(h); }
    uint64_t t1 = t_time_now_ms();
    uint64_t dt = t1 - t0; if(dt == 0) dt = 1;
    uint64_t ops = BENCH_ITERS; uint64_t qps = (ops * 1000) / dt;
    printf("[bench_queue_priority] iterations=%lu time_ms=%lu ops/sec=%lu\n", (unsigned long)BENCH_ITERS, (unsigned long)dt, (unsigned long)qps);
    heap_free(h);
}

static void bench_broadcast(void){
    const size_t per = BENCH_ITERS / 4; // 4 consumers
    int *buf0 = (int*)malloc(per * sizeof(int));
    int *buf1 = (int*)malloc(per * sizeof(int));
    int *buf2 = (int*)malloc(per * sizeof(int));
    int *buf3 = (int*)malloc(per * sizeof(int));
    for(size_t i=0; i<per; ++i){ buf0[i] = (int)i; buf1[i] = (int)i; buf2[i] = (int)i; buf3[i] = (int)i; }
    vec4_t vbuf[4] = { {buf0, per}, {buf1, per}, {buf2, per}, {buf3, per} };
    t_thread th[4];
    uint64_t t0 = t_time_now_ms();
    for(int i=0; i<4; ++i) t_thread_spawn(&th[i], consumer, &vbuf[i]);
    for(int i=0; i<4; ++i) t_thread_join(&th[i]);
    uint64_t t1 = t_time_now_ms();
    uint64_t dt = t1 - t0; if(dt == 0) dt = 1;
    uint64_t ops = BENCH_ITERS; uint64_t qps = (ops * 1000) / dt;
    printf("[bench_queue_broadcast] iterations=%lu time_ms=%lu ops/sec=%lu\n", (unsigned long)BENCH_ITERS, (unsigned long)dt, (unsigned long)qps);
    free(buf0); free(buf1); free(buf2); free(buf3);
}

int main(void){
    srand(12345);
    bench_fifo_throughput();
    bench_priority_throughput();
    bench_broadcast();
    return 0;
}
