#include "t_pqueue.h"
#include <stdlib.h>
#include <string.h>

static size_t t_pqueue_next_cap(size_t cap) {
    if (cap == 0) return 16;
    size_t n = cap;
    // grow to next power-of-two for simplicity
    n--;
    n |= n >> 1;
    n |= n >> 2;
    n |= n >> 4;
    n |= n >> 8;
    n |= n >> 16;
#if SIZE_MAX > 0xFFFFFFFFULL
    n |= n >> 32;
#endif
    return n + 1;
}

int t_pqueue_init(t_pqueue *pq, size_t capacity) {
    if (!pq) return -1;
    size_t cap = t_pqueue_next_cap(capacity);
    pq->entries = (t_pq_entry*)malloc(cap * sizeof(t_pq_entry));
    if (!pq->entries) return -1;
    pq->len = 0;
    pq->cap = cap;
    return 0;
}

void t_pqueue_destroy(t_pqueue *pq) {
    if (!pq) return;
    free(pq->entries);
    pq->entries = NULL;
    pq->len = 0;
    pq->cap = 0;
}

static void t_pqueue_swap(t_pq_entry *a, t_pq_entry *b) {
    t_pq_entry tmp = *a; *a = *b; *b = tmp;
}

static void t_pqueue_sift_up(t_pqueue *pq, size_t idx) {
    while (idx > 0) {
        size_t parent = (idx - 1) >> 1;
        if (pq->entries[parent].priority <= pq->entries[idx].priority) break;
        t_pqueue_swap(&pq->entries[parent], &pq->entries[idx]);
        idx = parent;
    }
}

static void t_pqueue_sift_down(t_pqueue *pq, size_t idx) {
    while (1) {
        size_t left = (idx << 1) + 1;
        size_t right = left + 1;
        size_t smallest = idx;
        if (left < pq->len && pq->entries[left].priority < pq->entries[smallest].priority) {
            smallest = left;
        }
        if (right < pq->len && pq->entries[right].priority < pq->entries[smallest].priority) {
            smallest = right;
        }
        if (smallest == idx) break;
        t_pqueue_swap(&pq->entries[smallest], &pq->entries[idx]);
        idx = smallest;
    }
}

int t_pqueue_push(t_pqueue *pq, int64_t priority, void *data) {
    if (!pq) return -1;
    if (pq->len >= pq->cap) {
        size_t newcap = pq->cap ? pq->cap * 2 : 16;
        t_pq_entry *newbuf = (t_pq_entry*)realloc(pq->entries, newcap * sizeof(t_pq_entry));
        if (!newbuf) return -1;
        pq->entries = newbuf;
        pq->cap = newcap;
    }
    size_t idx = pq->len++;
    pq->entries[idx].priority = priority;
    pq->entries[idx].data = data;
    t_pqueue_sift_up(pq, idx);
    return 0;
}

int t_pqueue_pop(t_pqueue *pq, t_pq_entry *out) {
    if (!pq || pq->len == 0) return -1;
    if (out) *out = pq->entries[0];
    pq->entries[0] = pq->entries[--pq->len];
    t_pqueue_sift_down(pq, 0);
    return 0;
}

int t_pqueue_peek(const t_pqueue *pq, t_pq_entry *out) {
    if (!pq || pq->len == 0) return -1;
    if (out) *out = pq->entries[0];
    return 0;
}

size_t t_pqueue_len(const t_pqueue *pq) {
    if (!pq) return 0;
    return pq->len;
}

void t_pqueue_clear(t_pqueue *pq) {
    if (!pq) return;
    pq->len = 0;
}
