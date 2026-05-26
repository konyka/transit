#ifndef T_PQUEUE_H
#define T_PQUEUE_H

#include <stddef.h>
#include <stdint.h>

/* Min-heap priority queue. Lower priority value = higher priority. */

typedef struct t_pq_entry {
    int64_t  priority;
    void    *data;
} t_pq_entry;

typedef struct t_pqueue {
    t_pq_entry *entries;
    size_t      len;
    size_t      cap;
} t_pqueue;

int     t_pqueue_init(t_pqueue *pq, size_t capacity);
void    t_pqueue_destroy(t_pqueue *pq);
int     t_pqueue_push(t_pqueue *pq, int64_t priority, void *data);
int     t_pqueue_pop(t_pqueue *pq, t_pq_entry *out);
int     t_pqueue_peek(const t_pqueue *pq, t_pq_entry *out);
size_t  t_pqueue_len(const t_pqueue *pq);
void    t_pqueue_clear(t_pqueue *pq);

#endif /* T_PQUEUE_H */
