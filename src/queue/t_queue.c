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
    int delivering;        /* nest count while fanout callbacks run */
    int free_pending;      /* destroy deferred until delivering == 0 */
    int owner_held;        /* map owner (domain) completes destroy */

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

typedef struct t_deliver_snap {
    t_queue_msg_cb fn;
    void *ud;
} t_deliver_snap;

static int t_queue_deliver_to_consumers(t_queue *q, t_msg *m) {
    /* Push-style: fire-and-forget. Copies are freed after the callback so
     * pending/inflight do not grow unbounded when callers never ack.
     * Snapshot (fn, ud) values — not t_cons_cb* — so a callback that
     * unsubscribes another consumer cannot UAF freed wrappers. */
    size_t cons = q->consumers.len;
    if (cons == 0) return 0;
    t_msg **copies = (t_msg **)calloc(cons, sizeof(t_msg *));
    t_deliver_snap *snaps = (t_deliver_snap *)calloc(cons, sizeof(t_deliver_snap));
    if (!copies || !snaps) {
        free(copies);
        free(snaps);
        return -1;
    }
    for (size_t i = 0; i < cons; ++i) {
        t_cons_cb *cb = (t_cons_cb *)q->consumers.items[i];
        if (cb) {
            snaps[i].fn = cb->cb;
            snaps[i].ud = cb->cb_ud;
        }
        copies[i] = t_msg_copy(m);
        if (!copies[i]) {
            for (size_t j = 0; j < i; ++j) t_msg_free(copies[j]);
            free(copies);
            free(snaps);
            return -1;
        }
    }
    q->delivering++;
    for (size_t i = 0; i < cons; ++i) {
        if (snaps[i].fn) snaps[i].fn(copies[i], snaps[i].ud);
        t_msg_free(copies[i]);
    }
    q->delivering--;
    free(copies);
    free(snaps);
    /* Do not destroy here: callers (post/drain) still touch q. */
    return 0;
}

/* Returns 1 if q was freed (caller must not touch it). */
static int queue_try_complete_destroy(t_queue *q) {
    if (!q || !q->free_pending || q->delivering != 0 || q->owner_held) return 0;
    t_queue_destroy(q);
    return 1;
}

/* Deliver any stranded backlog after a partial subscribe flush.
 * Dequeue before deliver so a reentrant consume cannot UAF the same msg.
 * Returns 0 if empty/drained, -1 if deliver failed (messages stay queued). */
static int t_queue_drain_backlog(t_queue *q) {
    if (!q || q->consumers.len == 0) return 0;
    if (q->type == T_QUEUE_PRIORITY) {
        t_pq_entry top;
        while (!q->free_pending && t_pqueue_peek(&q->pri, &top) == 0) {
            if (t_pqueue_pop(&q->pri, &top) != 0) return -1;
            t_msg *m = (t_msg *)top.data;
            if (t_queue_deliver_to_consumers(q, m) != 0) {
                if (t_pqueue_push(&q->pri, top.priority, m) != 0) {
                    t_msg_free(m);
                }
                return -1;
            }
            t_msg_free(m);
        }
        return 0;
    }
    if (q->type == T_QUEUE_FIFO) {
        while (!q->free_pending && q->pending.len > 0) {
            t_msg *m = (t_msg *)t_vec_remove(&q->pending, 0);
            if (!m) break;
            if (t_queue_deliver_to_consumers(q, m) != 0) {
                if (t_vec_insert(&q->pending, 0, m) != 0) {
                    t_msg_free(m);
                }
                return -1;
            }
            t_msg_free(m);
        }
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
    q->delivering = 0;
    q->free_pending = 0;
    q->owner_held = 0;
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
    if (q->delivering > 0) {
        q->free_pending = 1;
        return;
    }
    /* Map owners (domain) must clear owner_held before free completes. */
    if (q->owner_held) {
        q->free_pending = 1;
        return;
    }
    q->free_pending = 0;
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
    if (!q || q->closed || q->free_pending) return -1;
    if (len > 0 && !data) return -1;
    if (len > T_QUEUE_MAX_PAYLOAD) return -1;
    if (q->next_msg_id == UINT64_MAX) return -1;

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

    /* BROADCAST: deliver copies to all consumers; fail if none. */
    if (q->type == T_QUEUE_BROADCAST) {
        if (q->consumers.len == 0) {
            t_msg_free(m);
            return -1;
        }
        if (t_queue_deliver_to_consumers(q, m) != 0) {
            t_msg_free(m);
            (void)queue_try_complete_destroy(q);
            return -1;
        }
        q->total_published++;
        t_msg_free(m);
        if (queue_try_complete_destroy(q)) return -1;
        return 0;
    }

    if (q->type == T_QUEUE_PRIORITY) {
        /* Always enter the heap so push consumers still see priority order
         * when multiple messages are pending (e.g. after a partial drain). */
        if (t_pqueue_push(&q->pri, (int64_t)priority, m) != 0) {
            t_msg_free(m);
            return -1;
        }
        q->total_published++;
        /* Drain is best-effort: message is already accepted. Returning -1
         * here would make callers retry and risk duplicate delivery — unless
         * the queue itself was freed in a consumer callback. */
        if (q->consumers.len > 0)
            (void)t_queue_drain_backlog(q);
        if (queue_try_complete_destroy(q)) return -1;
        return 0;
    }

    /* FIFO: push-style (consumers) delivers + inflight only — do NOT also
     * enqueue to pending (that caused unbounded memory growth). Pull-style
     * (no consumers) stores in pending for t_queue_consume. */
    if (q->consumers.len > 0) {
        if (t_queue_drain_backlog(q) != 0) {
            t_msg_free(m);
            (void)queue_try_complete_destroy(q);
            return -1;
        }
        if (q->free_pending) {
            t_msg_free(m);
            (void)queue_try_complete_destroy(q);
            return -1;
        }
        if (t_queue_deliver_to_consumers(q, m) != 0) {
            t_msg_free(m);
            (void)queue_try_complete_destroy(q);
            return -1;
        }
        q->total_published++;
        t_msg_free(m);
        if (queue_try_complete_destroy(q)) return -1;
        return 0;
    }

    if (t_vec_push(&q->pending, m) != 0) {
        t_msg_free(m);
        return -1;
    }
    q->total_published++;
    return 0;
}

int t_queue_consume(t_queue *q, t_msg *out_msg) {
    if (!q || q->free_pending || !out_msg) return -1;
    /* Closed queues still allow draining pending/priority backlog. */
    if (q->closed && t_queue_pending_count(q) == 0) return -1;
    if (q->type == T_QUEUE_BROADCAST) return -1;
    /* Priority-based path */
    if (q->type == T_QUEUE_PRIORITY) {
        if (t_pqueue_len(&q->pri) == 0) return -1;
        t_inflight *inf = (t_inflight *)malloc(sizeof(t_inflight));
        if (!inf) return -1;
        t_pq_entry top;
        if (t_pqueue_pop(&q->pri, &top) != 0) {
            free(inf);
            return -1;
        }
        t_msg *m = (t_msg *)top.data;
        inf->msg = m;
        t_list_push_back(&q->inflight, &inf->node);
        *out_msg = *m;
        q->total_consumed++;
        return 0;
    }
    /* FIFO path — allocate inflight before dequeue so OOM cannot drop the msg. */
    if (q->pending.len == 0) {
        return -1;
    }
    t_inflight *inf = (t_inflight *)malloc(sizeof(t_inflight));
    if (!inf) return -1;
    t_msg *m = (t_msg *)t_vec_remove(&q->pending, 0);
    if (!m) {
        free(inf);
        return -1;
    }
    inf->msg = m;
    t_list_push_back(&q->inflight, &inf->node);
    *out_msg = *m;
    q->total_consumed++;
    return 0;
}

size_t t_queue_pending_count(const t_queue *q) {
    if (!q) return 0;
    if (q->type == T_QUEUE_PRIORITY) return t_pqueue_len(&q->pri);
    return q->pending.len;
}

uint64_t t_queue_add_consumer(t_queue *q, t_queue_msg_cb cb, void *ud) {
    if (!q || q->free_pending || !cb || q->closed) return 0;
    if (q->next_consumer_id == UINT64_MAX) return 0;
    t_cons_cb *wrapper = (t_cons_cb *)malloc(sizeof(t_cons_cb));
    if (!wrapper) return 0;
    wrapper->cb = cb;
    wrapper->cb_ud = ud;
    wrapper->id = ++q->next_consumer_id;

    if (t_vec_push(&q->consumers, wrapper) != 0) {
        free(wrapper);
        return 0;
    }
    /* Pull→push: flush backlog so early posts are not stranded.
     * Deliver before dequeue so OOM cannot strand messages.
     * If flush cannot finish, roll back this consumer so post stays pull-mode. */
    if (t_queue_drain_backlog(q) != 0) {
        (void)t_queue_remove_consumer(q, wrapper->id);
        (void)queue_try_complete_destroy(q);
        return 0;
    }
    uint64_t id = wrapper->id;
    if (queue_try_complete_destroy(q)) return 0;
    return id;
}

int t_queue_remove_consumer(t_queue *q, uint64_t consumer_id) {
    if (!q || q->free_pending || consumer_id == 0) return -1;
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
    if (!q || q->free_pending) return -1;
    int removed = 0;
    for (size_t i = 0; i < q->consumers.len; ) {
        t_cons_cb *cb = (t_cons_cb *)q->consumers.items[i];
        if (cb && cb->cb_ud == ud) {
            free(cb);
            for (size_t j = i; j + 1 < q->consumers.len; ++j) {
                q->consumers.items[j] = q->consumers.items[j + 1];
            }
            q->consumers.len--;
            removed++;
            continue;
        }
        i++;
    }
    return removed ? 0 : -1;
}

int t_queue_has_consumer_ud(const t_queue *q, void *ud) {
    if (!q) return 0;
    for (size_t i = 0; i < q->consumers.len; ++i) {
        t_cons_cb *cb = (t_cons_cb *)q->consumers.items[i];
        if (cb && cb->cb_ud == ud) return 1;
    }
    return 0;
}

size_t t_queue_consumer_count(const t_queue *q) {
    return q ? q->consumers.len : 0;
}

int t_queue_ack(t_queue *q, uint64_t msg_id) {
    if (!q || q->free_pending) return -1;
    /* Find inflight entries with matching id, free them */
    t_list_node *cur = q->inflight.head;
    int freed = 0;
    while (cur) {
        t_inflight *inf = (t_inflight *)((char*)cur - offsetof(t_inflight, node));
        t_list_node *next = cur->next;
        if (inf && inf->msg && inf->msg->msg_id == msg_id) {
            t_list_remove(&q->inflight, cur);
            t_msg_free(inf->msg);
            free(inf);
            freed++;
        }
        cur = next;
    }
    return freed ? 0 : -1;
}

int t_queue_nack(t_queue *q, uint64_t msg_id) {
    if (!q || q->free_pending) return -1;
    if (q->type == T_QUEUE_BROADCAST) return -1;
    t_list_node *cur = q->inflight.head;
    int moved = 0;
    int failed = 0;
    while (cur) {
        t_inflight *inf = (t_inflight *)((char *)cur - offsetof(t_inflight, node));
        t_list_node *next = cur->next;
        if (inf && inf->msg && inf->msg->msg_id == msg_id) {
            t_list_remove(&q->inflight, cur);
            int ok = 0;
            if (q->type == T_QUEUE_PRIORITY) {
                ok = (t_pqueue_push(&q->pri, (int64_t)inf->msg->priority, inf->msg) == 0);
            } else {
                /* Restore to head so FIFO order is preserved. */
                ok = (t_vec_insert(&q->pending, 0, inf->msg) == 0);
            }
            if (!ok) {
                /* Restore inflight so the message is not lost on OOM. */
                t_list_push_back(&q->inflight, &inf->node);
                failed = 1;
            } else {
                free(inf);
                if (q->total_consumed > 0) q->total_consumed--;
                moved++;
            }
        }
        cur = next;
    }
    if (failed) return -1;
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

int t_queue_is_delivering(const t_queue *q) {
    return q ? q->delivering > 0 : 0;
}

int t_queue_is_free_pending(const t_queue *q) {
    return q ? q->free_pending != 0 : 0;
}

void t_queue_set_owner_held(t_queue *q, int held) {
    if (q) q->owner_held = held ? 1 : 0;
}

size_t t_queue_total_published(const t_queue *q) {
    return q ? q->total_published : 0;
}

size_t t_queue_total_consumed(const t_queue *q) {
    return q ? q->total_consumed : 0;
}
