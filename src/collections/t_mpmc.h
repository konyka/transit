#ifndef T_MPMC_H
#define T_MPMC_H

#include "t_compiler.h"
#include "t_atomic.h"
#include <stdint.h>
#include <stddef.h>
#include <stdbool.h>

/* MPMC (Multi-Producer Multi-Consumer) lock-free queue.
   Based on Dmitry Vyukov's bounded MPMC queue algorithm. */

typedef struct t_mpmc_cell {
    t_atomic_int sequence;
    void        *data;
} t_mpmc_cell;

typedef struct t_mpmc {
    t_mpmc_cell *cells;
    size_t       cap;
    size_t       mask;
    t_atomic_int enqueue_pos;
    t_atomic_int dequeue_pos;
} t_mpmc;

int     t_mpmc_init(t_mpmc *q, size_t capacity);
void    t_mpmc_destroy(t_mpmc *q);
bool    t_mpmc_push(t_mpmc *q, void *item);
bool    t_mpmc_pop(t_mpmc *q, void **item);
size_t  t_mpmc_capacity(const t_mpmc *q);

#endif /* T_MPMC_H */
