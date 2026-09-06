#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include "t_time.h"
#include "t_test.h"
#include "t_thread.h"
#include "t_mpmc.h"

#define MAP_CAP 131101
#define MAP_N   100000
#define VEC_N   1000000

static unsigned long map_hash(const char* s){ unsigned long hash = 5381; int c; while((c = (unsigned char)*s++)) hash = ((hash << 5) + hash) + c; return hash; }

static void bench_map(void){
    char (*keys)[32] = (char (*)[32])malloc(MAP_CAP * 32);
    int *vals = (int*)malloc(MAP_CAP * sizeof(int));
    unsigned char *used = (unsigned char*)calloc(MAP_CAP, 1);
    for(int i=0;i<MAP_N;++i){ char key[32]; snprintf(key,32,"k-%06d",i); unsigned long h = map_hash(key) % MAP_CAP; size_t idx = h; for(size_t j=0;j<MAP_CAP;++j){ if(!used[idx]){ strncpy(keys[idx], key, 32); vals[idx] = i; used[idx] = 1; break; } idx = (idx+1)%MAP_CAP; } }
    for(int i=0;i<MAP_N;++i){ char key[32]; snprintf(key,32,"k-%06d",i); unsigned long h = map_hash(key) % MAP_CAP; size_t idx = h; int found = 0; for(size_t j=0;j<MAP_CAP;++j){ if(used[idx] && strncmp(keys[idx], key, 32)==0){ (void)vals[idx]; found = 1; break; } idx = (idx+1)%MAP_CAP; } /* ignore not-found */ (void)found; }
    uint64_t t0 = t_time_now_ms(); uint64_t t1 = t_time_now_ms() ; (void)t1; // warmup avoidance
    t0 = t_time_now_ms(); for(int i=0;i<MAP_N;++i){ char key[32]; snprintf(key,32,"k-%06d",i); unsigned long h = map_hash(key) % MAP_CAP; size_t idx = h; for(size_t j=0;j<MAP_CAP;++j){ if(used[idx] && strncmp(keys[idx], key, 32)==0){ break; } idx = (idx+1)%MAP_CAP; } }
    uint64_t t_end = t_time_now_ms(); uint64_t dt = t_end - t0; if(dt==0) dt=1; uint64_t qps = (MAP_N * 1000) / dt; printf("[bench_collections_map] iterations=%d time_ms=%lu ops/sec=%lu\n", MAP_N, (unsigned long)dt, (unsigned long)qps);
    free(vals); free(keys); free(used);
}

static void bench_vec_push(void){ int *data = NULL; size_t cap = 1024; size_t len = 0; data = malloc(cap * sizeof(int)); uint64_t t0 = t_time_now_ms(); for(size_t i=0;i<VEC_N;++i){ if(len==cap){ cap*=2; data = (int*)realloc(data, cap*sizeof(int)); } data[len++] = (int)i; } uint64_t t1 = t_time_now_ms(); uint64_t dt = t1 - t0; if(dt==0) dt=1; uint64_t qps = (VEC_N * 1000) / dt; printf("[bench_collections_vec_push] iterations=%zu time_ms=%lu ops/sec=%lu\n", (size_t)VEC_N, (unsigned long)dt, (unsigned long)qps); free(data); }

static void* prod(void* arg){ t_mpmc* q = (t_mpmc*)arg; for(int i=0;i<1000000;++i){ while(!t_mpmc_push(q, (void*)(uintptr_t)(i+1))) t_thread_yield(); } return NULL; }
static void* cons(void* arg){ t_mpmc* q = (t_mpmc*)arg; int got = 0; while(got < 1000000){ void *x; if(t_mpmc_pop(q, &x)) got++; else t_thread_yield(); } return NULL; }

static void bench_mpmc_queue(void){
    t_mpmc q;
    t_thread pth, cth;
    uint64_t t0, t1, dt, qps;
    if (t_mpmc_init(&q, 1024*1024) != 0) return;
    t0 = t_time_now_ms();
    t_thread_spawn(&pth, prod, &q);
    t_thread_spawn(&cth, cons, &q);
    t_thread_join(&pth);
    t_thread_join(&cth);
    t1 = t_time_now_ms();
    t_mpmc_destroy(&q);
    dt = t1 - t0; if(dt==0) dt=1;
    qps = (1000000ULL * 1000) / dt;
    printf("[bench_collections_mpmc] iterations=%d time_ms=%lu ops/sec=%lu\n", 1000000, (unsigned long)dt, (unsigned long)qps);
}

int main(void){ bench_map(); bench_vec_push(); bench_mpmc_queue(); return 0; }
