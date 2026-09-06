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
#include <stdio.h>

/* public headers for API */
#include "t_queue.h"
#include "../collections/t_vec.h"
#include "../collections/t_list.h"
#include "../collections/t_pqueue.h"
#include "t_wal.h"
#include "t_map.h"

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
    t_wal *wal;
    char *group;           /* sticky consumer group; NULL if none */
};

/* Helpers */
static void t_msg_free(t_msg *m) {
    if (!m) return;
    if (m->data) {
        free((void*)m->data);
    }
    free(m);
}

static int queue_wal_put(t_queue *q, uint64_t id, const uint8_t *data, size_t len, int priority) {
    if (!q->wal) return 0;
    uint8_t pri = 0;
    if (priority > 0) pri = (priority > 255) ? 255 : (uint8_t)priority;
    return t_wal_append(q->wal, T_WAL_PUT, pri, id, data, (uint32_t)len);
}

static void queue_wal_del(t_queue *q, uint64_t id) {
    if (!q || !q->wal) return;
    (void)t_wal_append(q->wal, T_WAL_DEL, 0, id, NULL, 0);
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
    queue_wal_del(q, m->msg_id);
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
    if (type != T_QUEUE_FIFO && type != T_QUEUE_PRIORITY && type != T_QUEUE_BROADCAST)
        return NULL;
    if ((flags & T_QUEUE_FLAG_DURABLE) && type == T_QUEUE_BROADCAST)
        return NULL;
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
    q->wal = NULL;
    q->group = NULL;

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
    t_wal_close(q->wal);
    q->wal = NULL;
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
    free(q->group);
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

int t_queue_get_flags(const t_queue *q) {
    return q ? q->flags : 0;
}

int t_queue_set_group(t_queue *q, const char *group) {
    if (!q || !group || !group[0] || q->type == T_QUEUE_BROADCAST) return -1;
    if (q->group) return strcmp(q->group, group) == 0 ? 0 : -1;
    char *g = strdup(group);
    if (!g) return -1;
    q->group = g;
    return 0;
}

const char *t_queue_group(const t_queue *q) {
    return q ? q->group : NULL;
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
    if (queue_wal_put(q, m->msg_id, data, len, priority) != 0) {
        q->next_msg_id--;
        t_msg_free(m);
        return -1;
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
            /* Keep the new message for later drain/consume instead of dropping. */
            if (q->free_pending || t_vec_push(&q->pending, m) != 0) {
                t_msg_free(m);
                (void)queue_try_complete_destroy(q);
                return -1;
            }
            q->total_published++;
            if (queue_try_complete_destroy(q)) return -1;
            return 0;
        }
        if (q->free_pending) {
            t_msg_free(m);
            (void)queue_try_complete_destroy(q);
            return -1;
        }
        if (t_queue_deliver_to_consumers(q, m) != 0) {
            if (q->free_pending || t_vec_push(&q->pending, m) != 0) {
                t_msg_free(m);
                (void)queue_try_complete_destroy(q);
                return -1;
            }
            q->total_published++;
            if (queue_try_complete_destroy(q)) return -1;
            return 0;
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
    if (queue_try_complete_destroy(q)) return -1;
    return 0;
}

int t_queue_consume(t_queue *q, t_msg *out_msg) {
    if (!q || q->free_pending || !out_msg) return -1;
    /* Closed queues still allow draining pending/priority backlog. */
    if (q->closed && t_queue_pending_count(q) == 0) return -1;
    if (q->type == T_QUEUE_BROADCAST) return -1;
    /* Push consumers own delivery; pull must not steal (even after close). */
    if (q->consumers.len > 0) return -1;
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
    if ((q->flags & T_QUEUE_FLAG_EXCLUSIVE) && q->consumers.len > 0) return 0;
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
    if (freed) queue_wal_del(q, msg_id);
    return freed ? 0 : -1;
}

int t_queue_drop(t_queue *q, uint64_t msg_id) {
    if (!q || q->free_pending) return -1;
    if (t_queue_ack(q, msg_id) == 0) return 0;
    if (q->type == T_QUEUE_PRIORITY) {
        size_t n = t_pqueue_len(&q->pri);
        for (size_t i = 0; i < n; i++) {
            t_msg *m = (t_msg *)q->pri.entries[i].data;
            if (!m || m->msg_id != msg_id) continue;
            if (t_pqueue_remove(&q->pri, m) != 0) return -1;
            t_msg_free(m);
            queue_wal_del(q, msg_id);
            return 0;
        }
        return -1;
    }
    for (size_t i = 0; i < q->pending.len; i++) {
        t_msg *m = (t_msg *)q->pending.items[i];
        if (!m || m->msg_id != msg_id) continue;
        (void)t_vec_remove(&q->pending, i);
        t_msg_free(m);
        queue_wal_del(q, msg_id);
        return 0;
    }
    return -1;
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
    if (!moved) return -1;
    /* Push consumers own delivery; requeued msgs must not sit in backlog. */
    if (q->consumers.len > 0)
        (void)t_queue_drain_backlog(q);
    if (queue_try_complete_destroy(q)) return -1;
    return 0;
}

int t_queue_requeue(t_queue *q, uint64_t msg_id) {
    return t_queue_nack(q, msg_id);
}

void t_queue_close(t_queue *q) {
    if (!q) return;
    q->closed = 1;
    /* Push consumers cannot pull-drain; deliver any stranded backlog now. */
    if (q->consumers.len > 0)
        (void)t_queue_drain_backlog(q);
}

int t_queue_is_closed(const t_queue *q) {
    return q ? q->closed : 1;
}

int t_queue_is_delivering(const t_queue *q) {
    return q ? q->delivering > 0 : 0;
}

int t_queue_has_inflight(const t_queue *q) {
    return q ? t_list_count(&q->inflight) > 0 : 0;
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

int t_queue_restore(t_queue *q, uint64_t msg_id, const uint8_t *data,
                    size_t len, int priority) {
    if (!q || q->closed || q->free_pending) return -1;
    if (len > T_QUEUE_MAX_PAYLOAD) return -1;
    if (len > 0 && !data) return -1;
    t_msg *m = (t_msg *)malloc(sizeof(t_msg));
    if (!m) return -1;
    m->msg_id = msg_id;
    m->queue_name = q->name;
    m->data_len = len;
    m->priority = priority;
    m->timestamp_ns = 0;
    m->data = NULL;
    if (len) {
        m->data = malloc(len);
        if (!m->data) {
            free(m);
            return -1;
        }
        memcpy((void *)m->data, data, len);
    }
    int ok = 0;
    if (q->type == T_QUEUE_PRIORITY) {
        ok = (t_pqueue_push(&q->pri, (int64_t)priority, m) == 0);
    } else {
        ok = (t_vec_push(&q->pending, m) == 0);
    }
    if (!ok) {
        t_msg_free(m);
        return -1;
    }
    if (msg_id > q->next_msg_id) q->next_msg_id = msg_id;
    q->total_published++;
    return 0;
}

typedef struct {
    uint64_t msg_id;
    uint8_t  priority;
    uint8_t *data;
    uint32_t len;
} t_q_live;

static void queue_recover_cb(const t_wal_rec *r, void *ud) {
    t_map *live = (t_map *)ud;
    char key[32];
    snprintf(key, sizeof(key), "%016llx", (unsigned long long)r->msg_id);
    if (r->op == T_WAL_DEL) {
        t_q_live *old = (t_q_live *)t_map_remove(live, key);
        if (old) {
            free(old->data);
            free(old);
        }
        return;
    }
    t_q_live *e = (t_q_live *)calloc(1, sizeof(*e));
    if (!e) return;
    e->msg_id = r->msg_id;
    e->priority = r->priority;
    e->len = r->data_len;
    if (r->data_len) {
        e->data = (uint8_t *)malloc(r->data_len);
        if (!e->data) {
            free(e);
            return;
        }
        memcpy(e->data, r->data, r->data_len);
    }
    t_q_live *old = (t_q_live *)t_map_get(live, key);
    if (t_map_insert(live, key, e) != 0) {
        free(e->data);
        free(e);
        return;
    }
    if (old) {
        free(old->data);
        free(old);
    }
}

static int live_cmp(const void *a, const void *b) {
    const t_q_live *la = *(t_q_live *const *)a;
    const t_q_live *lb = *(t_q_live *const *)b;
    if (la->msg_id < lb->msg_id) return -1;
    if (la->msg_id > lb->msg_id) return 1;
    return 0;
}

static int t_queue_recover(t_queue *q) {
    t_map live;
    t_map_init(&live);
    if (t_wal_replay(q->wal, queue_recover_cb, &live) != 0) {
        t_map_iter it = t_map_iter_begin(&live);
        const char *k;
        void *v;
        while (t_map_iter_next(&it, &k, &v)) {
            t_q_live *e = (t_q_live *)v;
            free(e->data);
            free(e);
        }
        t_map_destroy(&live);
        return -1;
    }
    size_t n = t_map_len(&live);
    t_q_live **arr = NULL;
    if (n > 0) {
        arr = (t_q_live **)malloc(n * sizeof(*arr));
        if (!arr) {
            t_map_iter it = t_map_iter_begin(&live);
            const char *k;
            void *v;
            while (t_map_iter_next(&it, &k, &v)) {
                t_q_live *e = (t_q_live *)v;
                free(e->data);
                free(e);
            }
            t_map_destroy(&live);
            return -1;
        }
        size_t i = 0;
        t_map_iter it = t_map_iter_begin(&live);
        const char *k;
        void *v;
        while (t_map_iter_next(&it, &k, &v) && i < n)
            arr[i++] = (t_q_live *)v;
        qsort(arr, i, sizeof(*arr), live_cmp);
        for (size_t j = 0; j < i; j++) {
            t_q_live *e = arr[j];
            (void)t_queue_restore(q, e->msg_id, e->data, e->len, (int)e->priority);
            free(e->data);
            free(e);
        }
        free(arr);
    }
    t_map_destroy(&live);
    return 0;
}

int t_queue_open_wal(t_queue *q, const char *path, int sync_every) {
    if (!q || q->wal || !path) return -1;
    if (!(q->flags & T_QUEUE_FLAG_DURABLE)) return -1;
    if (q->type == T_QUEUE_BROADCAST) return -1;
    t_wal *w = t_wal_open(path, sync_every);
    if (!w) return -1;
    q->wal = w;
    if (t_queue_recover(q) != 0) {
        t_wal_close(w);
        q->wal = NULL;
        return -1;
    }
    return 0;
}

int t_queue_each_live(const t_queue *q, t_queue_live_fn fn, void *ud) {
    if (!q || !fn) return -1;
    if (q->has_prio) {
        for (size_t i = 0; i < q->pri.len; i++) {
            t_msg *m = (t_msg *)q->pri.entries[i].data;
            if (m && fn(m, (void *)ud) != 0) return -1;
        }
    } else {
        size_t n = t_vec_len((t_vec *)&q->pending);
        for (size_t i = 0; i < n; i++) {
            t_msg *m = (t_msg *)t_vec_get((t_vec *)&q->pending, i);
            if (m && fn(m, (void *)ud) != 0) return -1;
        }
    }
    t_list_node *cur;
    T_LIST_FOR_EACH((t_list *)&q->inflight, cur) {
        t_inflight *inf = T_LIST_ENTRY(cur, t_inflight, node);
        if (inf->msg && fn(inf->msg, (void *)ud) != 0) return -1;
    }
    return 0;
}

int t_queue_flush(t_queue *q) {
    if (!q || !q->wal) return -1;
    return t_wal_flush(q->wal);
}

const char *t_queue_wal_path(const t_queue *q) {
    return q && q->wal ? t_wal_path(q->wal) : NULL;
}
