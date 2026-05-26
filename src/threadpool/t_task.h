#ifndef T_TASK_H
#define T_TASK_H

#include <stddef.h>

typedef void (*t_task_fn)(void *context);

typedef struct t_task {
    t_task_fn  fn;
    void      *context;
} t_task;

#define T_TASK(fn, ctx) { (fn), (ctx) }

static inline t_task t_task_make(t_task_fn fn, void *context) {
    t_task t;
    t.fn = fn;
    t.context = context;
    return t;
}

#endif /* T_TASK_H */
