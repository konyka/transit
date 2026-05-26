#ifndef T_TPOOL_H
#define T_TPOOL_H

#include "t_task.h"
#include <stddef.h>
#include <stdint.h>

typedef struct t_tpool t_tpool;

/* Create a thread pool with num_threads workers.
   If num_threads == 0, use number of CPU cores. */
t_tpool *t_tpool_create(size_t num_threads);
void     t_tpool_destroy(t_tpool *pool);

/* Submit a task to the pool. Returns 0 on success, -1 on failure. */
int      t_tpool_submit(t_tpool *pool, t_task_fn fn, void *context);

/* Submit task to a specific worker's queue (for locality) */
int      t_tpool_submit_to(t_tpool *pool, size_t worker_id, t_task_fn fn, void *context);

/* Get number of workers */
size_t   t_tpool_worker_count(const t_tpool *pool);

/* Wait for all submitted tasks to complete */
void     t_tpool_wait(t_tpool *pool);

/* Statistics */
size_t   t_tpool_tasks_completed(const t_tpool *pool);
size_t   t_tpool_tasks_stolen(const t_tpool *pool);

#endif /* T_TPOOL_H */
