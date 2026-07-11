/**
 * Queue implementation with multiple strategies and consumer callbacks
 * for the transit project.
 *
 * This file relies on the existing collection utilities in /src/collections:
 *  - t_vec: dynamic array of void*
 *  - t_list: intrusive list (used for inflight tracking)
 *  - t_pqueue: min-heap for priority queues
 */

#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <stddef.h>

/* public headers for API */
#include "t_queue.h"
#include "../collections/t_vec.h"
#include "../collections/t_list.h"
#include "../collections/t_pqueue.h"

/* Internal: in-flight entry. */
typedef struct t_inflight {
    t_msg *msg;
    t_list_node node;
} t_inflight;

/* Internal: consumer callback wrapper. */
typedef struct t_cons_cb {
    t_queue_msg_cb cb;
    void *cb_ud;
    uint64_t id;
} t_cons_cb;

/* Our opaque queue structure. */
struct t_queue {
    char *name;
    t_qtype type;
    int flags;
    int closed;

    uint64_t next_msg_id;
    uint64_t next_consumer_id;

    t_vec pending;        /* t_msg* waiting to be consumed (FIFO) */
    t_vec consumers;      /* t_queue_consumer* (opaque) */
    t_list inflight;        /* t_inflight entries */
    t_pqueue pri;          /* used when type == T_QUEUE_PRIORITY */
    int has_prio;

    size_t total_published;
    size_t total_consumed;
};

/* Helpers */
static void t_msg_free(t_msg *m) {
    if (!m) return;
    if (m->data) {
        free((void*)m->data);
    }
    free(m);
}

/* Create a new t_message with copied payload. */
static t_msg *t_msg_copy(const t_msg *src) {
    if (!src) return NULL;
    t_msg *m = (t_msg *)malloc(sizeof(t_msg));
    if (!m) return NULL;
    m->msg_id = src->msg_id;
    m->queue_name = src->queue_name;
    m->data_len = src->data_len;
    m->data = NULL;
    if (src->data_len && src->data) {
        m->data = (uint8_t *)malloc(src->data_len);
        if (!m->data) {
            free(m);
            return NULL;
        }
        memcpy((void *)m->data, src->data, src->data_len);
    }
    m->priority = src->priority;
    m->timestamp_ns = src->timestamp_ns;
    return m;
}

static int t_queue_deliver_to_consumers(t_queue *q, t_msg *m) {
    /* Push-style: fire-and-forget. Copies are freed after the callback so
     * pending/inflight do not grow unbounded when callers never ack. */
    size_t cons = q->consumers.len;
    for (size_t i = 0; i < cons; ++i) {
        t_cons_cb *cb = (t_cons_cb *)q->consumers.items[i];
        t_msg *copym = t_msg_copy(m);
        if (!copym) continue;
        if (cb && cb->cb) cb->cb(copym, cb->cb_ud);
        t_msg_free(copym);
    }
    return 0;
}

/* Create a new t_queue */
t_queue *t_queue_create(const char *name, t_qtype type, int flags) {
    t_queue *q = (t_queue *)malloc(sizeof(t_queue));
    if (!q) return NULL;
    q->name = strdup(name ? name : "");
    if (!q->name) {
        free(q);
        return NULL;
    }
    q->type = type;
    q->flags = flags;
    q->closed = 0;
    q->next_msg_id = 0;
    q->next_consumer_id = 0;
    q->total_published = 0;
    q->total_consumed = 0;

    t_vec_init(&q->pending);
    t_vec_init(&q->consumers);
    t_list_init(&q->inflight);
    q->has_prio = (type == T_QUEUE_PRIORITY) ? 1 : 0;
    if (q->has_prio) {
        if (t_pqueue_init(&q->pri, 16) != 0) {
            free(q->name);
            free(q);
            return NULL;
        }
    }

    return q;
}

void t_queue_destroy(t_queue *q) {
    if (!q) return;
    /* Free pending messages */
    for (size_t i = 0; i < q->pending.len; ++i) {
        t_msg *m = (t_msg *)q->pending.items[i];
        t_msg_free(m);
    }
    t_vec_destroy(&q->pending);

    /* Free inflight messages */
    t_list_node *cursor = q->inflight.head;
    while (cursor) {
        t_inflight *inf = (t_inflight *)((char*)cursor - offsetof(t_inflight, node));
        t_msg_free(inf->msg);
        t_list_node *next = cursor->next;
        free(inf);
        cursor = next;
    }
    t_list_init(&q->inflight); /* reset */

    /* Free consumers wrappers */
    for (size_t i = 0; i < q->consumers.len; ++i) {
        free(q->consumers.items[i]);
    }
    t_vec_destroy(&q->consumers);

    /* Free name and queue struct */
    free(q->name);
    if (q->has_prio) {
        t_pq_entry top;
        while (t_pqueue_pop(&q->pri, &top) == 0) {
            t_msg_free((t_msg *)top.data);
        }
        t_pqueue_destroy(&q->pri);
    }
    free(q);
}

const char *t_queue_name(const t_queue *q) {
    return q ? q->name : NULL;
}

t_qtype t_queue_get_type(const t_queue *q) {
    return q ? q->type : T_QUEUE_FIFO;
}

int t_queue_post(t_queue *q, const uint8_t *data, size_t len, int priority) {
    if (!q || q->closed) return -1;
    if (len > 0 && !data) return -1;

    t_msg *m = (t_msg *)malloc(sizeof(t_msg));
    if (!m) return -1;
    m->msg_id = ++q->next_msg_id;
    m->queue_name = q->name;
    m->data_len = len;
    m->priority = priority;
    m->data = NULL;
    m->timestamp_ns = 0;
    if (len) {
        m->data = malloc(len);
        if (!m->data) {
            free(m);
            return -1;
        }
        memcpy((void *)m->data, data, len);
    }

    /* BROADCAST: deliver copies to all consumers; drop if none. */
    if (q->type == T_QUEUE_BROADCAST) {
        q->total_published++;
        if (q->consumers.len == 0) {
            t_msg_free(m);
            return 0;
        }
        t_queue_deliver_to_consumers(q, m);
        t_msg_free(m);
        return 0;
    }

    q->total_published++;
    if (q->type == T_QUEUE_PRIORITY) {
        if (t_pqueue_push(&q->pri, (int64_t)priority, m) != 0) {
            t_msg_free(m);
            return -1;
        }
        return 0;
    }

    /* FIFO: push-style (consumers) delivers + inflight only — do NOT also
     * enqueue to pending (that caused unbounded memory growth). Pull-style
     * (no consumers) stores in pending for t_queue_consume. */
    if (q->consumers.len > 0) {
        t_queue_deliver_to_consumers(q, m);
        t_msg_free(m);
        return 0;
    }

    if (t_vec_push(&q->pending, m) != 0) {
        t_msg_free(m);
        return -1;
    }
    return 0;
}

int t_queue_consume(t_queue *q, t_msg *out_msg) {
    if (!q || q->closed) return -1;
    /* Priority-based path */
    if (q->type == T_QUEUE_PRIORITY) {
        t_pq_entry top;
        if (t_pqueue_pop(&q->pri, &top) != 0) {
            return -1;
        }
        t_msg *m = (t_msg *)top.data;
        t_inflight *inf = (t_inflight *)malloc(sizeof(t_inflight));
        if (!inf) {
            /* Roll back into heap so the message is not lost. */
            if (t_pqueue_push(&q->pri, (int64_t)m->priority, m) != 0) {
                t_msg_free(m);
            }
            return -1;
        }
        inf->msg = m;
        t_list_push_back(&q->inflight, &inf->node);
        if (out_msg) *out_msg = *m;
        q->total_consumed++;
        return 0;
    }
    /* FIFO path */
    if (q->pending.len == 0) {
        return -1;
    }
    t_msg *m = (t_msg *)t_vec_remove(&q->pending, 0);
    if (!m) return -1;
    t_inflight *inf = (t_inflight *)malloc(sizeof(t_inflight));
    if (!inf) {
        if (t_vec_push(&q->pending, m) != 0) {
            t_msg_free(m);
        }
        return -1;
    }
    inf->msg = m;
    t_list_push_back(&q->inflight, &inf->node);
    if (out_msg) *out_msg = *m;
    q->total_consumed++;
    return 0;
}

size_t t_queue_pending_count(const t_queue *q) {
    if (!q) return 0;
    if (q->type == T_QUEUE_PRIORITY) return t_pqueue_len(&q->pri);
    return q->pending.len;
}

uint64_t t_queue_add_consumer(t_queue *q, t_queue_msg_cb cb, void *ud) {
    if (!q || !cb) return 0;
    t_cons_cb *wrapper = (t_cons_cb *)malloc(sizeof(t_cons_cb));
    if (!wrapper) return 0;
    wrapper->cb = cb;
    wrapper->cb_ud = ud;
    wrapper->id = ++q->next_consumer_id;

    if (t_vec_push(&q->consumers, wrapper) != 0) {
        free(wrapper);
        return 0;
    }
    return wrapper->id;
}

int t_queue_remove_consumer(t_queue *q, uint64_t consumer_id) {
    if (!q || consumer_id == 0) return -1;
    for (size_t i = 0; i < q->consumers.len; ++i) {
        t_cons_cb *cb = (t_cons_cb *)q->consumers.items[i];
        if (!cb || cb->id != consumer_id) continue;
        free(cb);
        for (size_t j = i; j + 1 < q->consumers.len; ++j) {
            q->consumers.items[j] = q->consumers.items[j + 1];
        }
        q->consumers.len--;
        return 0;
    }
    return -1;
}

int t_queue_remove_consumer_ud(t_queue *q, void *ud) {
    if (!q) return -1;
    for (size_t i = 0; i < q->consumers.len; ++i) {
        t_cons_cb *cb = (t_cons_cb *)q->consumers.items[i];
        if (cb && cb->cb_ud == ud) {
            free(cb);
            for (size_t j = i; j + 1 < q->consumers.len; ++j) {
                q->consumers.items[j] = q->consumers.items[j + 1];
            }
            q->consumers.len--;
            return 0;
        }
    }
    return -1;
}

size_t t_queue_consumer_count(const t_queue *q) {
    return q ? q->consumers.len : 0;
}

int t_queue_ack(t_queue *q, uint64_t msg_id) {
    if (!q) return -1;
    /* Find inflight entries with matching id, free them */
    t_list_node *cur = q->inflight.head;
    int freed = 0;
    while (cur) {
        t_inflight *inf = (t_inflight *)((char*)cur - offsetof(t_inflight, node));
        if (inf && inf->msg && inf->msg->msg_id == msg_id) {
            t_list_node *next = cur->next;
            /* free message data and struct */
            t_msg_free(inf->msg);
            free(inf);
            /* unlink from list: simple approach since we don't have prev pointer access */
            if (cur->prev) cur->prev->next = next; else q->inflight.head = next;
            if (next) next->prev = cur->prev; else q->inflight.tail = cur->prev;
            cur = next;
            freed++;
        } else {
            cur = cur->next;
        }
    }
    return freed ? 0 : -1;
}

int t_queue_nack(t_queue *q, uint64_t msg_id) {
    if (!q) return -1;
    t_list_node *cur = q->inflight.head;
    int moved = 0;
    while (cur) {
        t_inflight *inf = (t_inflight *)((char *)cur - offsetof(t_inflight, node));
        t_list_node *next = cur->next;
        if (inf && inf->msg && inf->msg->msg_id == msg_id) {
            if (cur->prev) cur->prev->next = next; else q->inflight.head = next;
            if (next) next->prev = cur->prev; else q->inflight.tail = cur->prev;
            if (q->type == T_QUEUE_PRIORITY) {
                if (t_pqueue_push(&q->pri, (int64_t)inf->msg->priority, inf->msg) != 0) {
                    t_msg_free(inf->msg);
                }
            } else {
                if (t_vec_push(&q->pending, inf->msg) != 0) {
                    t_msg_free(inf->msg);
                }
            }
            free(inf);
            moved++;
        }
        cur = next;
    }
    return moved ? 0 : -1;
}

int t_queue_requeue(t_queue *q, uint64_t msg_id) {
    return t_queue_nack(q, msg_id);
}

void t_queue_close(t_queue *q) {
    if (q) q->closed = 1;
}

int t_queue_is_closed(const t_queue *q) {
    return q ? q->closed : 1;
}

size_t t_queue_total_published(const t_queue *q) {
    return q ? q->total_published : 0;
}

size_t t_queue_total_consumed(const t_queue *q) {
    return q ? q->total_consumed : 0;
}
