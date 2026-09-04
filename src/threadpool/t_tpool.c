#include "t_tpool.h"
#include "t_mpmc.h"
#include "t_mutex.h"
#include "t_compiler.h"
#include <stdlib.h>

#if T_PLATFORM_WINDOWS
#include <windows.h>
typedef struct tpool_thrd {
    HANDLE h;
    DWORD  tid;
} tpool_thrd;
typedef CONDITION_VARIABLE tpool_cnd;
#else
#include <pthread.h>
#include <sched.h>
#include <unistd.h>
typedef pthread_t tpool_thrd;
typedef pthread_cond_t tpool_cnd;
#endif

#if T_PLATFORM_WINDOWS
typedef struct tpool_start {
    void *(*fn)(void *);
    void *arg;
} tpool_start;

static DWORD WINAPI tpool_win_main(void *p) {
    tpool_start s = *(tpool_start *)p;
    free(p);
    (void)s.fn(s.arg);
    return 0;
}
#endif

static int tpool_thrd_spawn(tpool_thrd *out, void *(*fn)(void *), void *arg) {
#if T_PLATFORM_WINDOWS
    tpool_start *s;
    HANDLE h;
    DWORD tid = 0;
    if (!out) return -1;
    s = (tpool_start *)malloc(sizeof(*s));
    if (!s) return -1;
    s->fn = fn;
    s->arg = arg;
    h = CreateThread(NULL, 0, tpool_win_main, s, 0, &tid);
    if (!h) {
        free(s);
        return -1;
    }
    out->h = h;
    out->tid = tid;
    return 0;
#else
    return pthread_create(out, NULL, fn, arg) == 0 ? 0 : -1;
#endif
}

static void tpool_thrd_join(tpool_thrd t) {
#if T_PLATFORM_WINDOWS
    WaitForSingleObject(t.h, INFINITE);
    CloseHandle(t.h);
#else
    pthread_join(t, NULL);
#endif
}

static int tpool_thrd_is_self(tpool_thrd t) {
#if T_PLATFORM_WINDOWS
    return t.tid == GetCurrentThreadId();
#else
    return pthread_equal(t, pthread_self());
#endif
}

static void tpool_thrd_detach(tpool_thrd t) {
#if T_PLATFORM_WINDOWS
    CloseHandle(t.h);
#else
    pthread_detach(t);
#endif
}

static void tpool_thrd_yield(void) {
#if T_PLATFORM_WINDOWS
    SwitchToThread();
#else
    sched_yield();
#endif
}

static size_t tpool_ncpu(void) {
#if T_PLATFORM_WINDOWS
    SYSTEM_INFO si;
    GetSystemInfo(&si);
    return si.dwNumberOfProcessors ? (size_t)si.dwNumberOfProcessors : 1;
#else
    long nproc = sysconf(_SC_NPROCESSORS_ONLN);
    return (nproc > 0) ? (size_t)nproc : 1;
#endif
}

static void tpool_cnd_init(tpool_cnd *c) {
#if T_PLATFORM_WINDOWS
    InitializeConditionVariable(c);
#else
    pthread_cond_init(c, NULL);
#endif
}

static void tpool_cnd_destroy(tpool_cnd *c) {
#if T_PLATFORM_WINDOWS
    (void)c;
#else
    pthread_cond_destroy(c);
#endif
}

static void tpool_cnd_wait(tpool_cnd *c, t_mutex *m) {
#if T_PLATFORM_WINDOWS
    (void)SleepConditionVariableSRW(c, m, INFINITE, 0);
#else
    pthread_cond_wait(c, m);
#endif
}

static void tpool_cnd_broadcast(tpool_cnd *c) {
#if T_PLATFORM_WINDOWS
    WakeAllConditionVariable(c);
#else
    pthread_cond_broadcast(c);
#endif
}

typedef struct t_worker {
    t_tpool   *pool;
    size_t     id;
    tpool_thrd thread;
    t_mpmc     queue;
} t_worker;

struct t_tpool {
    t_worker       *workers;
    size_t          num_workers;
    size_t          total_submitted;
    size_t          total_completed;
    size_t          total_stolen;
    size_t          rr;
    int             stopping;
    int             destroying;   /* one-shot destroy gate */
    int             free_pending; /* in-task teardown done; shell free on next destroy */
    t_mutex         wait_mutex;
    tpool_cnd       wait_cond;
};

/* Run task body without touching pool (safe while tearing down). */
static void run_task_raw(t_task *task) {
    if (!task) return;
    if (task->fn) {
        t_task_fn fn = task->fn;
        void *ctx = task->context;
        free(task);
        fn(ctx);
    } else {
        free(task);
    }
}

/* Returns 1 if pool was freed inside (caller must not touch pool). */
static int run_task(t_task *task, t_tpool *pool) {
    if (!task || !task->fn) { free(task); return 0; }
    t_task_fn fn = task->fn;
    void *ctx = task->context;
    free(task);
    fn(ctx);
    /* In-task destroy leaves the shell for a later t_tpool_destroy (no UAF). */
    if (pool->free_pending) return 1;
    t_mutex_lock(&pool->wait_mutex);
    pool->total_completed++;
    tpool_cnd_broadcast(&pool->wait_cond);
    t_mutex_unlock(&pool->wait_mutex);
    return 0;
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
            if (run_task(task, pool)) return NULL;
            continue;
        }

        int stolen = 0;
        for (size_t i = 1; i < pool->num_workers && !stolen; ++i) {
            size_t victim = (w->id + i) % pool->num_workers;
            if (try_pop_task(&pool->workers[victim].queue, &task)) {
                spins = 0;
                t_mutex_lock(&pool->wait_mutex);
                pool->total_stolen++;
                t_mutex_unlock(&pool->wait_mutex);
                if (run_task(task, pool)) return NULL;
                stolen = 1;
            }
        }

        if (!stolen) {
            spins++;
            if (spins > 100) {
                t_mutex_lock(&pool->wait_mutex);
                if (pool->total_completed < pool->total_submitted) {
                    tpool_cnd_wait(&pool->wait_cond, &pool->wait_mutex);
                }
                t_mutex_unlock(&pool->wait_mutex);
                spins = 0;
            } else {
                tpool_thrd_yield();
            }
        }
    }
    return NULL;
}

t_tpool *t_tpool_create(size_t num_threads) {
    if (num_threads == 0)
        num_threads = tpool_ncpu();

    t_tpool *pool = (t_tpool *)calloc(1, sizeof(t_tpool));
    if (!pool) return NULL;
    pool->num_workers = num_threads;
    pool->workers = (t_worker *)calloc(num_threads, sizeof(t_worker));
    if (!pool->workers) { free(pool); return NULL; }
    t_mutex_init(&pool->wait_mutex);
    tpool_cnd_init(&pool->wait_cond);

    for (size_t i = 0; i < pool->num_workers; ++i) {
        pool->workers[i].pool = pool;
        pool->workers[i].id = i;
        if (t_mpmc_init(&pool->workers[i].queue, 1024) != 0) {
            pool->stopping = 1;
            for (size_t j = 0; j < i; ++j) {
                tpool_thrd_join(pool->workers[j].thread);
                t_mpmc_destroy(&pool->workers[j].queue);
            }
            free(pool->workers);
            t_mutex_destroy(&pool->wait_mutex);
            tpool_cnd_destroy(&pool->wait_cond);
            free(pool);
            return NULL;
        }
        if (tpool_thrd_spawn(&pool->workers[i].thread, worker_main,
                             &pool->workers[i]) != 0) {
            pool->stopping = 1;
            /* Join before destroying any queue: peers may still steal from i. */
            for (size_t j = 0; j < i; ++j)
                tpool_thrd_join(pool->workers[j].thread);
            for (size_t j = 0; j <= i; ++j)
                t_mpmc_destroy(&pool->workers[j].queue);
            free(pool->workers);
            t_mutex_destroy(&pool->wait_mutex);
            tpool_cnd_destroy(&pool->wait_cond);
            free(pool);
            return NULL;
        }
    }
    return pool;
}

int t_tpool_submit(t_tpool *pool, t_task_fn fn, void *context) {
    if (!pool || !fn || !pool->workers) return -1;
    t_task *task = (t_task *)malloc(sizeof(t_task));
    if (!task) return -1;
    task->fn = fn;
    task->context = context;
    t_mutex_lock(&pool->wait_mutex);
    if (pool->stopping) {
        t_mutex_unlock(&pool->wait_mutex);
        free(task);
        return -1;
    }
    size_t idx = pool->rr++ % pool->num_workers;
    /* Count + enqueue under lock so destroy cannot free the queue mid-push. */
    pool->total_submitted++;
    if (!t_mpmc_push(&pool->workers[idx].queue, task)) {
        pool->total_submitted--;
        t_mutex_unlock(&pool->wait_mutex);
        free(task);
        return -1;
    }
    tpool_cnd_broadcast(&pool->wait_cond);
    t_mutex_unlock(&pool->wait_mutex);
    return 0;
}

int t_tpool_submit_to(t_tpool *pool, size_t worker_id, t_task_fn fn, void *context) {
    if (!pool || !fn || !pool->workers || worker_id >= pool->num_workers) return -1;
    t_task *task = (t_task *)malloc(sizeof(t_task));
    if (!task) return -1;
    task->fn = fn;
    task->context = context;
    t_mutex_lock(&pool->wait_mutex);
    if (pool->stopping) {
        t_mutex_unlock(&pool->wait_mutex);
        free(task);
        return -1;
    }
    pool->total_submitted++;
    if (!t_mpmc_push(&pool->workers[worker_id].queue, task)) {
        pool->total_submitted--;
        t_mutex_unlock(&pool->wait_mutex);
        free(task);
        return -1;
    }
    tpool_cnd_broadcast(&pool->wait_cond);
    t_mutex_unlock(&pool->wait_mutex);
    return 0;
}

size_t t_tpool_worker_count(const t_tpool *pool) {
    return pool ? pool->num_workers : 0;
}

void t_tpool_wait(t_tpool *pool) {
    if (!pool || !pool->workers) return;
    t_mutex_lock(&pool->wait_mutex);
    /* Exit when destroy starts so waiters release wait_mutex before it is
     * destroyed (in-task destroy joins peers then tears down the cond). */
    while (!pool->destroying &&
           pool->total_completed < pool->total_submitted) {
        tpool_cnd_wait(&pool->wait_cond, &pool->wait_mutex);
    }
    t_mutex_unlock(&pool->wait_mutex);
}

size_t t_tpool_tasks_completed(const t_tpool *pool) {
    return pool ? pool->total_completed : 0;
}

size_t t_tpool_tasks_stolen(const t_tpool *pool) {
    return pool ? pool->total_stolen : 0;
}

void t_tpool_destroy(t_tpool *pool) {
    if (!pool) return;
    if (!pool->workers) {
        /* Shell left after in-task teardown — free once from the waiter. */
        free(pool);
        return;
    }

    int self_idx = -1;
    for (size_t i = 0; i < pool->num_workers; ++i) {
        if (tpool_thrd_is_self(pool->workers[i].thread)) {
            self_idx = (int)i;
            break;
        }
    }

    t_mutex_lock(&pool->wait_mutex);
    if (pool->destroying) {
        t_mutex_unlock(&pool->wait_mutex);
        return;
    }
    pool->destroying = 1;
    pool->stopping = 1;
    tpool_cnd_broadcast(&pool->wait_cond);
    t_mutex_unlock(&pool->wait_mutex);

    if (self_idx >= 0) {
        /* In-task destroy: cannot join self — join peers, tear down,
         * leave shell for a subsequent t_tpool_destroy from another thread. */
        tpool_thrd self = pool->workers[self_idx].thread;
        for (size_t i = 0; i < pool->num_workers; ++i) {
            if ((int)i == self_idx) continue;
            tpool_thrd_join(pool->workers[i].thread);
        }
        for (size_t i = 0; i < pool->num_workers; ++i) {
            void *raw = NULL;
            while (t_mpmc_pop(&pool->workers[i].queue, &raw)) {
                run_task_raw((t_task *)raw);
            }
            t_mpmc_destroy(&pool->workers[i].queue);
        }
        free(pool->workers);
        pool->workers = NULL;
        pool->num_workers = 0;
        t_mutex_destroy(&pool->wait_mutex);
        tpool_cnd_destroy(&pool->wait_cond);
        tpool_thrd_detach(self);
        pool->free_pending = 1;
        return;
    }

    for (size_t i = 0; i < pool->num_workers; ++i) {
        tpool_thrd_join(pool->workers[i].thread);
    }
    for (size_t i = 0; i < pool->num_workers; ++i) {
        void *raw = NULL;
        while (t_mpmc_pop(&pool->workers[i].queue, &raw)) {
            /* Drain leftover work instead of silently dropping it. */
            run_task_raw((t_task *)raw);
        }
        t_mpmc_destroy(&pool->workers[i].queue);
    }
    free(pool->workers);
    pool->workers = NULL;
    t_mutex_destroy(&pool->wait_mutex);
    tpool_cnd_destroy(&pool->wait_cond);
    free(pool);
}
