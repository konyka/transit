#include "t_mpmc.h"
#include <stdlib.h>
#include <string.h>
#include <stdint.h>

static int is_power_of_two(size_t x) { return x && ((x & (x - 1)) == 0); }

int t_mpmc_init(t_mpmc *q, size_t capacity) {
    if (!q) return -1;
    size_t cap = 1;
    while (cap < capacity) {
        if (cap > SIZE_MAX / 2) return -1;
        cap <<= 1;
    }
    if (!is_power_of_two(cap)) cap = 1u << 1; /* fallback */
    q->cells = (t_mpmc_cell*)calloc(cap, sizeof(t_mpmc_cell));
    if (!q->cells) return -1;
    q->cap = cap;
    q->mask = cap - 1;
    t_atomic_store_int(&q->enqueue_pos, 0);
    t_atomic_store_int(&q->dequeue_pos, 0);
    for (size_t i = 0; i < cap; ++i) {
        t_atomic_store_int(&q->cells[i].sequence, (int)i);
        q->cells[i].data = NULL;
    }
    return 0;
}

void t_mpmc_destroy(t_mpmc *q) {
    if (!q) return;
    free(q->cells);
    q->cells = NULL;
    q->cap = 0;
    q->mask = 0;
    t_atomic_store_int(&q->enqueue_pos, 0);
    t_atomic_store_int(&q->dequeue_pos, 0);
}

bool t_mpmc_push(t_mpmc *q, void *item) {
    if (!q) return false;
    while (true) {
        /* Unsigned wrap keeps Vyukov sequence math defined past INT_MAX. */
        uint32_t pos = (uint32_t)t_atomic_load_int(&q->enqueue_pos);
        t_mpmc_cell *cell = &q->cells[pos & (uint32_t)q->mask];
        uint32_t seq = (uint32_t)t_atomic_load_int(&cell->sequence);
        int32_t dif = (int32_t)(seq - pos);
        if (dif == 0) {
            if (t_atomic_cas_int(&q->enqueue_pos, (int)pos, (int)(pos + 1u))) {
                cell->data = item;
                t_atomic_store_int(&cell->sequence, (int)(pos + 1u));
                return true;
            }
        } else if (dif < 0) {
            return false; /* full */
        }
    }
}

bool t_mpmc_pop(t_mpmc *q, void **item) {
    if (!q || !item) return false;
    while (true) {
        uint32_t pos = (uint32_t)t_atomic_load_int(&q->dequeue_pos);
        t_mpmc_cell *cell = &q->cells[pos & (uint32_t)q->mask];
        uint32_t seq = (uint32_t)t_atomic_load_int(&cell->sequence);
        int32_t dif = (int32_t)(seq - (pos + 1u));
        if (dif == 0) {
            if (t_atomic_cas_int(&q->dequeue_pos, (int)pos, (int)(pos + 1u))) {
                void *d = cell->data;
                cell->data = NULL;
                t_atomic_store_int(&cell->sequence, (int)(pos + (uint32_t)q->mask + 1u));
                *item = d;
                return true;
            }
        } else if (dif < 0) {
            return false; /* empty */
        }
    }
}

size_t t_mpmc_capacity(const t_mpmc *q) {
    if (!q) return 0;
    return q->cap;
}
