#include "t_tpool.h"
#include "t_mpmc.h"
#include <stdlib.h>
#include <pthread.h>
#include <sched.h>
#include <unistd.h>

typedef struct t_worker {
    t_tpool  *pool;
    size_t    id;
    pthread_t thread;
    t_mpmc    queue;
} t_worker;

struct t_tpool {
    t_worker       *workers;
    size_t          num_workers;
    size_t          total_submitted;
    size_t          total_completed;
    size_t          total_stolen;
    int             stopping;
    pthread_mutex_t wait_mutex;
    pthread_cond_t  wait_cond;
};

static void run_task(t_task *task, t_tpool *pool) {
    if (!task || !task->fn) { free(task); return; }
    t_task_fn fn = task->fn;
    void *ctx = task->context;
    free(task);
    fn(ctx);
    pthread_mutex_lock(&pool->wait_mutex);
    pool->total_completed++;
    pthread_cond_broadcast(&pool->wait_cond);
    pthread_mutex_unlock(&pool->wait_mutex);
}

static int try_pop_task(t_mpmc *q, t_task **out) {
    if (!q || !q->cells) return 0;
    void *raw = NULL;
    bool got = t_mpmc_pop(q, &raw);
    if (got) {
        *out = (t_task *)raw;
        return 1;
    }
    return 0;
}

static void *worker_main(void *arg) {
    t_worker *w = (t_worker *)arg;
    t_tpool *pool = w->pool;
    int spins = 0;

    while (!pool->stopping) {
        t_task *task = NULL;
        if (try_pop_task(&w->queue, &task)) {
            spins = 0;
            run_task(task, pool);
            continue;
        }

        int stolen = 0;
        for (size_t i = 1; i < pool->num_workers && !stolen; ++i) {
            size_t victim = (w->id + i) % pool->num_workers;
            if (try_pop_task(&pool->workers[victim].queue, &task)) {
                spins = 0;
                pthread_mutex_lock(&pool->wait_mutex);
                pool->total_stolen++;
                pthread_mutex_unlock(&pool->wait_mutex);
                run_task(task, pool);
                stolen = 1;
            }
        }

        if (!stolen) {
            spins++;
            if (spins > 100) {
                pthread_mutex_lock(&pool->wait_mutex);
                if (pool->total_completed < pool->total_submitted) {
                    pthread_cond_wait(&pool->wait_cond, &pool->wait_mutex);
                }
                pthread_mutex_unlock(&pool->wait_mutex);
                spins = 0;
            } else {
                sched_yield();
            }
        }
    }
    return NULL;
}

t_tpool *t_tpool_create(size_t num_threads) {
    if (num_threads == 0) {
        long nproc = sysconf(_SC_NPROCESSORS_ONLN);
        num_threads = (nproc > 0) ? (size_t)nproc : 1;
    }

    t_tpool *pool = (t_tpool *)calloc(1, sizeof(t_tpool));
    if (!pool) return NULL;
    pool->num_workers = num_threads;
    pool->workers = (t_worker *)calloc(num_threads, sizeof(t_worker));
    if (!pool->workers) { free(pool); return NULL; }
    pthread_mutex_init(&pool->wait_mutex, NULL);
    pthread_cond_init(&pool->wait_cond, NULL);

    for (size_t i = 0; i < pool->num_workers; ++i) {
        pool->workers[i].pool = pool;
        pool->workers[i].id = i;
        if (t_mpmc_init(&pool->workers[i].queue, 1024) != 0) {
            pool->stopping = 1;
            for (size_t j = 0; j < i; ++j) {
                pthread_join(pool->workers[j].thread, NULL);
                t_mpmc_destroy(&pool->workers[j].queue);
            }
            free(pool->workers);
            pthread_mutex_destroy(&pool->wait_mutex);
            pthread_cond_destroy(&pool->wait_cond);
            free(pool);
            return NULL;
        }
        if (pthread_create(&pool->workers[i].thread, NULL, worker_main,
                           &pool->workers[i]) != 0) {
            pool->stopping = 1;
            t_mpmc_destroy(&pool->workers[i].queue);
            for (size_t j = 0; j < i; ++j) {
                pthread_join(pool->workers[j].thread, NULL);
                t_mpmc_destroy(&pool->workers[j].queue);
            }
            free(pool->workers);
            pthread_mutex_destroy(&pool->wait_mutex);
            pthread_cond_destroy(&pool->wait_cond);
            free(pool);
            return NULL;
        }
    }
    return pool;
}

int t_tpool_submit(t_tpool *pool, t_task_fn fn, void *context) {
    if (!pool || !fn) return -1;
    t_task *task = (t_task *)malloc(sizeof(t_task));
    if (!task) return -1;
    task->fn = fn;
    task->context = context;
    static size_t rr = 0;
    size_t idx = __sync_fetch_and_add(&rr, 1) % pool->num_workers;
    if (!t_mpmc_push(&pool->workers[idx].queue, task)) {
        free(task);
        return -1;
    }
    pthread_mutex_lock(&pool->wait_mutex);
    pool->total_submitted++;
    pthread_cond_broadcast(&pool->wait_cond);
    pthread_mutex_unlock(&pool->wait_mutex);
    return 0;
}

int t_tpool_submit_to(t_tpool *pool, size_t worker_id, t_task_fn fn, void *context) {
    if (!pool || !fn || worker_id >= pool->num_workers) return -1;
    t_task *task = (t_task *)malloc(sizeof(t_task));
    if (!task) return -1;
    task->fn = fn;
    task->context = context;
    if (!t_mpmc_push(&pool->workers[worker_id].queue, task)) {
        free(task);
        return -1;
    }
    pthread_mutex_lock(&pool->wait_mutex);
    pool->total_submitted++;
    pthread_cond_broadcast(&pool->wait_cond);
    pthread_mutex_unlock(&pool->wait_mutex);
    return 0;
}

size_t t_tpool_worker_count(const t_tpool *pool) {
    return pool ? pool->num_workers : 0;
}

void t_tpool_wait(t_tpool *pool) {
    if (!pool) return;
    pthread_mutex_lock(&pool->wait_mutex);
    while (pool->total_completed < pool->total_submitted) {
        pthread_cond_wait(&pool->wait_cond, &pool->wait_mutex);
    }
    pthread_mutex_unlock(&pool->wait_mutex);
}

size_t t_tpool_tasks_completed(const t_tpool *pool) {
    return pool ? pool->total_completed : 0;
}

size_t t_tpool_tasks_stolen(const t_tpool *pool) {
    return pool ? pool->total_stolen : 0;
}

void t_tpool_destroy(t_tpool *pool) {
    if (!pool) return;
    pool->stopping = 1;
    pthread_mutex_lock(&pool->wait_mutex);
    pthread_cond_broadcast(&pool->wait_cond);
    pthread_mutex_unlock(&pool->wait_mutex);

    for (size_t i = 0; i < pool->num_workers; ++i) {
        pthread_join(pool->workers[i].thread, NULL);
        t_mpmc_destroy(&pool->workers[i].queue);
    }
    free(pool->workers);
    pthread_mutex_destroy(&pool->wait_mutex);
    pthread_cond_destroy(&pool->wait_cond);
    free(pool);
}
