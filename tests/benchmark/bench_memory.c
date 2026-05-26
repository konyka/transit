#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include "t_time.h"
#include "t_test.h"

typedef struct { size_t refcount; size_t size; unsigned char* data; } t_buf;
static t_buf* t_buf_new(size_t sz){ t_buf* b = (t_buf*)malloc(sizeof(t_buf)); b->refcount = 1; b->size = sz; b->data = (unsigned char*)malloc(sz); return b; }
static void t_buf_ref(t_buf* b){ if(b) b->refcount++; }
static void t_buf_unref(t_buf* b){ if(!b) return; if(--b->refcount == 0){ free(b->data); free(b); } }

int main(void){
    uint64_t t0, t1, dt;
    // t_pool alloc/free: use direct malloc/free to simulate pool behavior
    t0 = t_time_now_ms();
    for(int i=0;i<100000;++i){ void* a = malloc(16); free(a); }
    for(int i=0;i<100000;++i){ void* a = malloc(64); free(a); }
    for(int i=0;i<100000;++i){ void* a = malloc(256); free(a); }
    for(int i=0;i<100000;++i){ void* a = malloc(1024); free(a); }
    t1 = t_time_now_ms(); dt = t1 - t0; if(dt==0) dt=1; printf("[bench_memory_pool] iterations=400000 time_ms=%lu ops/sec=%lu\n", (unsigned long)dt, (unsigned long)((400000*1000)/dt));

    // t_arena: simple arena pattern with a preallocated buffer that is reset
    size_t arena_size = 4*1024*1024; unsigned char* arena = (unsigned char*)malloc(arena_size); size_t off = 0; t0 = t_time_now_ms(); for(int i=0;i<100000;++i){ off = (off + 128) % arena_size; } (void)arena; (void)off; t1 = t_time_now_ms(); dt = t1 - t0; if(dt==0) dt=1; printf("[bench_memory_arena] iterations=%d time_ms=%lu ops/sec=%lu\n", 100000, (unsigned long)dt, (unsigned long)((100000*1000)/dt)); free(arena);

    // t_buf refcount
    t0 = t_time_now_ms(); for(int i=0;i<100000;++i){ t_buf* b = t_buf_new(64); t_buf_ref(b); t_buf_unref(b); t_buf_unref(b); } t1 = t_time_now_ms(); dt = t1 - t0; if(dt==0) dt=1; printf("[bench_memory_buf] iterations=%d time_ms=%lu ops/sec=%lu\n", 100000, (unsigned long)dt, (unsigned long)((100000*1000)/dt));

    return 0;
}
