#include "t_mpmc.h"
#include <stdlib.h>
#include <string.h>

static inline int t_mpmc_advance_pos(int p) { return p; }

static int is_power_of_two(size_t x) { return x && ((x & (x - 1)) == 0); }

int t_mpmc_init(t_mpmc *q, size_t capacity) {
    if (!q) return -1;
    size_t cap = 1;
    while (cap < capacity) cap <<= 1;
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
        int pos = t_atomic_load_int(&q->enqueue_pos);
        t_mpmc_cell *cell = &q->cells[(size_t)pos & q->mask];
        int seq = t_atomic_load_int(&cell->sequence);
        if (seq == pos) {
            if (t_atomic_cas_int(&q->enqueue_pos, pos, pos + 1)) {
                cell->data = item;
                t_atomic_store_int(&cell->sequence, pos + 1);
                return true;
            } else {
                continue;
            }
        } else if (seq < pos) {
            // Queue is full
            return false;
        } else {
            // Not ready yet, retry with updated pos
            continue;
        }
    }
}

bool t_mpmc_pop(t_mpmc *q, void **item) {
    if (!q || !item) return false;
    while (true) {
        int pos = t_atomic_load_int(&q->dequeue_pos);
        t_mpmc_cell *cell = &q->cells[(size_t)pos & q->mask];
        int seq = t_atomic_load_int(&cell->sequence);
        if (seq == pos + 1) {
            if (t_atomic_cas_int(&q->dequeue_pos, pos, pos + 1)) {
                void *d = cell->data;
                cell->data = NULL;
                t_atomic_store_int(&cell->sequence, pos + q->mask + 1);
                *item = d;
                return true;
            } else {
                continue;
            }
        } else if (seq < pos + 1) {
            // Empty
            return false;
        } else {
            // Not ready yet
            continue;
        }
    }
}

size_t t_mpmc_capacity(const t_mpmc *q) {
    if (!q) return 0;
    return q->cap;
}
