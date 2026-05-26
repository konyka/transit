#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <pthread.h>
#include "t_time.h"
#include "t_test.h"

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

typedef struct{ int *buf; size_t cap; size_t head; size_t tail; pthread_mutex_t m; pthread_cond_t not_empty; pthread_cond_t not_full; } mp_queue_t;

static mp_queue_t* mpq_create(size_t cap){ mp_queue_t* q = (mp_queue_t*)malloc(sizeof(mp_queue_t)); q->buf = (int*)malloc(cap*sizeof(int)); q->cap = cap; q->head = 0; q->tail = 0; pthread_mutex_init(&q->m, NULL); pthread_cond_init(&q->not_empty, NULL); pthread_cond_init(&q->not_full, NULL); return q; }
static void mpq_free(mp_queue_t* q){ if(!q) return; free(q->buf); pthread_mutex_destroy(&q->m); pthread_cond_destroy(&q->not_empty); pthread_cond_destroy(&q->not_full); free(q); }
static void mpq_push(mp_queue_t* q, int v){ pthread_mutex_lock(&q->m); while((q->tail - q->head) >= q->cap) pthread_cond_wait(&q->not_full, &q->m); q->buf[q->tail % q->cap] = v; q->tail++; pthread_cond_signal(&q->not_empty); pthread_mutex_unlock(&q->m); }
static int mpq_pop(mp_queue_t* q){ pthread_mutex_lock(&q->m); while(q->tail == q->head) pthread_cond_wait(&q->not_empty, &q->m); int v = q->buf[q->head % q->cap]; q->head++; pthread_cond_signal(&q->not_full); pthread_mutex_unlock(&q->m); return v; }

static void* prod(void* arg){ mp_queue_t* q = (mp_queue_t*)arg; for(int i=0;i<1000000;++i) mpq_push(q, i); return NULL; }
static void* cons(void* arg){ mp_queue_t* q = (mp_queue_t*)arg; for(int i=0;i<1000000;++i){ int x = mpq_pop(q); (void)x; } return NULL; }

static void bench_mpmc_queue(void){ mp_queue_t* q = mpq_create(1024*1024); pthread_t pth, cth; uint64_t t0 = t_time_now_ms(); pthread_create(&pth, NULL, prod, q); pthread_create(&cth, NULL, cons, q); pthread_join(pth, NULL); pthread_join(cth, NULL); uint64_t t1 = t_time_now_ms(); mpq_free(q); uint64_t dt = t1 - t0; if(dt==0) dt=1; uint64_t qps = (1000000ULL * 1000) / dt; printf("[bench_collections_mpmc] iterations=%d time_ms=%lu ops/sec=%lu\n", 1000000, (unsigned long)dt, (unsigned long)qps); }

int main(void){ bench_map(); bench_vec_push(); bench_mpmc_queue(); return 0; }
