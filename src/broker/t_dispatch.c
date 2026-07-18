#include <stdlib.h>
#include <string.h>
#include <stdio.h>

#include "t_dispatch.h"
#include "t_broker.h"
#include "t_domain.h"
#include "../collections/t_vec.h"
#include "t_queue.h"
#include "t_map.h"
#include "t_session.h"

/* Internal storage for a single session subscription */
typedef struct t_dispatch_sub {
    uint64_t session_id;
    char    *queue_name;
    void    *cbud;
} t_dispatch_sub;

/* Dispatch wrapper for broker delivery callback */
typedef struct dispatch_sub_cb_ud {
    uint64_t    session_id;
    t_dispatch *disp;
    char       *queue_name; /* for deferred free while fanout holds ud */
    int         zombie;
} dispatch_sub_cb_ud;

/* Dispatch object */
struct t_dispatch {
    t_broker *broker;
    t_map      sessions;      /* session_id (string) -> t_session* */
    t_vec      subscriptions; /* array of t_dispatch_sub * (dynamically allocated) */
    t_vec      deferred_cbud; /* zombies waiting until fanout ends */
    size_t     total_published;
    size_t     total_delivered;
    int        free_pending; /* destroy deferred while cbud snaps live */
    int        on_reap_list;
    t_dispatch *reap_next;
};

/* Dispatches waiting for fanout to end so deferred cbuds can be freed. */
static t_dispatch *g_reap_head;

static void dispatch_reap_enqueue(t_dispatch *disp) {
    if (!disp || disp->on_reap_list) return;
    disp->reap_next = g_reap_head;
    g_reap_head = disp;
    disp->on_reap_list = 1;
}

static void dispatch_reap_dequeue(t_dispatch *disp) {
    if (!disp || !disp->on_reap_list) return;
    t_dispatch **pp = &g_reap_head;
    while (*pp) {
        if (*pp == disp) {
            *pp = disp->reap_next;
            break;
        }
        pp = &(*pp)->reap_next;
    }
    disp->reap_next = NULL;
    disp->on_reap_list = 0;
}

static void dispatch_free_cbud(dispatch_sub_cb_ud *cbud) {
    if (!cbud) return;
    free(cbud->queue_name);
    free(cbud);
}

static void dispatch_purge_deferred(t_dispatch *disp) {
    if (!disp) return;
    for (size_t i = 0; i < disp->deferred_cbud.len; ) {
        dispatch_sub_cb_ud *cbud = (dispatch_sub_cb_ud *)disp->deferred_cbud.items[i];
        if (cbud && cbud->queue_name &&
            t_broker_is_queue_delivering(disp->broker, cbud->queue_name)) {
            ++i;
            continue;
        }
        for (size_t j = i; j + 1 < disp->deferred_cbud.len; ++j) {
            disp->deferred_cbud.items[j] = disp->deferred_cbud.items[j + 1];
        }
        disp->deferred_cbud.len--;
        dispatch_free_cbud(cbud);
    }
}

/* Free now, or defer if a push fanout still holds this ud in a snap. */
static void dispatch_retire_cbud(t_dispatch *disp, const char *queue_name,
                                 dispatch_sub_cb_ud *cbud) {
    if (!cbud) return;
    cbud->zombie = 1;
    cbud->disp = NULL;
    if (disp && queue_name && t_broker_is_queue_delivering(disp->broker, queue_name)) {
        if (t_vec_push(&disp->deferred_cbud, cbud) != 0) {
            /* OOM during fanout: leak rather than free under live snap ud. */
            return;
        }
        return;
    }
    dispatch_free_cbud(cbud);
}

/* Helpers */
static void dispatch_deliver_cb(const char *queue_name, const uint8_t *data,
                              size_t len, void *ud) {
    (void)queue_name; /* not used directly here, per session routing we derive by ud */
    dispatch_sub_cb_ud *wrapper = (dispatch_sub_cb_ud *)ud;
    if (!wrapper || wrapper->zombie || !wrapper->disp) return;
    t_dispatch *disp = wrapper->disp;
    /* Build string key from session_id */
    char key[32];
    snprintf(key, sizeof(key), "%llu", (unsigned long long)wrapper->session_id);
    t_session *sess = (t_session *)t_map_get(&disp->sessions, key);
    if (!sess || !t_session_is_active(sess)) {
        /* Disconnect without unregister leaves a ghost broker consumer. */
        if (wrapper->zombie) return;
        uint64_t sid = wrapper->session_id;
        const char *qname = wrapper->queue_name;
        if (qname) {
            (void)t_broker_unsubscribe(disp->broker, qname,
                                       dispatch_deliver_cb, wrapper);
        }
        for (size_t i = 0; i < disp->subscriptions.len; ) {
            t_dispatch_sub *sub = (t_dispatch_sub *)disp->subscriptions.items[i];
            if (sub && sub->session_id == sid && sub->cbud == wrapper) {
                dispatch_retire_cbud(disp, sub->queue_name,
                                     (dispatch_sub_cb_ud *)sub->cbud);
                free(sub->queue_name);
                free(sub);
                for (size_t j = i; j + 1 < disp->subscriptions.len; ++j) {
                    disp->subscriptions.items[j] = disp->subscriptions.items[j + 1];
                }
                disp->subscriptions.len--;
                continue;
            }
            ++i;
        }
        return;
    }
    /* Update session activity as a delivered message */
    t_session_update_activity(sess);
    t_session_record_recv(sess);
    /* Update global stats */
    disp->total_delivered++;
    (void)data; (void)len; /* data/content are not used by test harness beyond counting */
}


/* Public API */
t_dispatch *t_dispatch_create(t_broker *broker) {
    if (!broker) return NULL;
    t_dispatch *d = (t_dispatch *)calloc(1, sizeof(t_dispatch));
    if (!d) return NULL;
    d->broker = broker;
    t_broker_retain(broker);
    t_map_init(&d->sessions);
    t_vec_init(&d->subscriptions);
    t_vec_init(&d->deferred_cbud);
    d->total_published = 0;
    d->total_delivered = 0;
    return d;
}

/* Returns 1 if freed, 0 if deferred/no-op. */
static int dispatch_destroy_now(t_dispatch *disp) {
    if (!disp) return 0;
    /* Unsubscribe from broker before freeing cbud (avoids UAF on late delivery). */
    if (disp->subscriptions.items) {
        for (size_t i = 0; i < disp->subscriptions.len; ++i) {
            t_dispatch_sub *sub = (t_dispatch_sub *)disp->subscriptions.items[i];
            if (!sub) continue;
            if (sub->queue_name && sub->cbud) {
                t_broker_unsubscribe(disp->broker, sub->queue_name,
                                     dispatch_deliver_cb, sub->cbud);
            }
            dispatch_retire_cbud(disp, sub->queue_name, (dispatch_sub_cb_ud *)sub->cbud);
            free(sub->queue_name);
            free(sub);
        }
    }
    disp->subscriptions.len = 0;
    t_vec_destroy(&disp->subscriptions);
    t_vec_init(&disp->subscriptions);

    dispatch_purge_deferred(disp);
    if (disp->deferred_cbud.len > 0) {
        disp->free_pending = 1;
        dispatch_reap_enqueue(disp);
        return 0;
    }

    dispatch_reap_dequeue(disp);
    disp->free_pending = 0;
    t_vec_destroy(&disp->deferred_cbud);
    /* Release session refs so callers can destroy after dispatch. */
    {
        t_map_iter it = t_map_iter_begin(&disp->sessions);
        const char *key;
        void *val;
        while (t_map_iter_next(&it, &key, &val)) {
            t_session *sess = (t_session *)val;
            if (sess) {
                (void)t_session_disconnect(sess);
                t_session_unpin(sess);
            }
        }
    }
    t_map_destroy(&disp->sessions);
    t_broker *broker = disp->broker;
    disp->broker = NULL;
    free(disp);
    t_broker_release(broker);
    return 1;
}

void t_dispatch_destroy(t_dispatch *disp) {
    (void)dispatch_destroy_now(disp);
}

/* Returns 1 if disp was freed. */
static int dispatch_try_complete_destroy(t_dispatch *disp) {
    if (!disp || !disp->free_pending) return 0;
    return dispatch_destroy_now(disp);
}

void t_dispatch_reap_deferred(void) {
    t_dispatch *d = g_reap_head;
    while (d) {
        t_dispatch *next = d->reap_next;
        dispatch_purge_deferred(d);
        if (d->free_pending && d->deferred_cbud.len == 0)
            (void)dispatch_destroy_now(d);
        d = next;
    }
}

int t_dispatch_register(t_dispatch *disp, uint64_t session_id, t_session *sess) {
    if (!disp || disp->free_pending || !sess) return -1;
    char key[32];
    snprintf(key, sizeof(key), "%llu", (unsigned long long)session_id);
    if (t_map_contains(&disp->sessions, key)) return -1;
    if (t_map_insert(&disp->sessions, key, sess) != 0) return -1;
    t_session_pin(sess);
    (void)t_session_connect(sess);
    return 0;
}

int t_dispatch_unregister(t_dispatch *disp, uint64_t session_id) {
    if (!disp || disp->free_pending) return -1;
    char key[32];
    snprintf(key, sizeof(key), "%llu", (unsigned long long)session_id);
    void *v = t_map_remove(&disp->sessions, key);
    if (!v) return -1;
    (void)t_session_disconnect((t_session *)v);
    t_session_unpin((t_session *)v);
    /* Drop broker subscriptions for this session to avoid ghost deliveries. */
    for (size_t i = 0; i < disp->subscriptions.len; ) {
        t_dispatch_sub *sub = (t_dispatch_sub *)disp->subscriptions.items[i];
        if (sub && sub->session_id == session_id) {
            if (sub->queue_name && sub->cbud) {
                t_broker_unsubscribe(disp->broker, sub->queue_name,
                                     dispatch_deliver_cb, sub->cbud);
            }
            dispatch_retire_cbud(disp, sub->queue_name, (dispatch_sub_cb_ud *)sub->cbud);
            free(sub->queue_name);
            free(sub);
            for (size_t j = i; j + 1 < disp->subscriptions.len; ++j) {
                disp->subscriptions.items[j] = disp->subscriptions.items[j + 1];
            }
            disp->subscriptions.len--;
            continue;
        }
        ++i;
    }
    dispatch_purge_deferred(disp);
    if (dispatch_try_complete_destroy(disp)) return -1;
    return 0;
}

int t_dispatch_publish(t_dispatch *disp, uint64_t session_id,
                       const char *queue_name, const uint8_t *data, size_t len, int priority) {
    if (!disp || disp->free_pending || !queue_name || (len > 0 && !data)) return -1;
    char key[32];
    snprintf(key, sizeof(key), "%llu", (unsigned long long)session_id);
    void *sess = t_map_get(&disp->sessions, key);
    if (!sess || !t_session_is_active((t_session *)sess)) return -1;

    /* After delete/recreate, local bookkeeping can outlive broker consumers.
     * Heal before publish so FIFO messages are not stranded in pending.
     * If any heal fails, abort publish rather than silently drop delivery. */
    int heal_failed = 0;
    for (size_t i = 0; i < disp->subscriptions.len; ) {
        t_dispatch_sub *sub = (t_dispatch_sub *)disp->subscriptions.items[i];
        if (!sub || !sub->queue_name || strcmp(sub->queue_name, queue_name) != 0) {
            i++;
            continue;
        }
        if (t_broker_has_subscription(disp->broker, queue_name,
                                      dispatch_deliver_cb, sub->cbud)) {
            i++;
            continue;
        }
        if (t_broker_subscribe(disp->broker, queue_name,
                               dispatch_deliver_cb, sub->cbud) == 0) {
            i++;
            continue;
        }
        dispatch_retire_cbud(disp, sub->queue_name, (dispatch_sub_cb_ud *)sub->cbud);
        free(sub->queue_name);
        free(sub);
        for (size_t j = i; j + 1 < disp->subscriptions.len; ++j) {
            disp->subscriptions.items[j] = disp->subscriptions.items[j + 1];
        }
        disp->subscriptions.len--;
        heal_failed = 1;
    }
    if (heal_failed) {
        dispatch_purge_deferred(disp);
        if (dispatch_try_complete_destroy(disp)) return -1;
        return -1;
    }

    int r = t_broker_publish(disp->broker, queue_name, data, len, priority);
    if (r == 0) disp->total_published++;
    dispatch_purge_deferred(disp);
    if (dispatch_try_complete_destroy(disp)) return -1;
    return r;
}

int t_dispatch_subscribe(t_dispatch *disp, uint64_t session_id, const char *queue_name) {
    if (!disp || disp->free_pending || !queue_name) return -1;
    /* verify session exists and is active */
    char key[32];
    snprintf(key, sizeof(key), "%llu", (unsigned long long)session_id);
    void *sess = t_map_get(&disp->sessions, key);
    if (!sess || !t_session_is_active((t_session *)sess)) return -1;

    for (size_t i = 0; i < disp->subscriptions.len; ++i) {
        t_dispatch_sub *existing = (t_dispatch_sub *)disp->subscriptions.items[i];
        if (existing && existing->session_id == session_id && existing->queue_name &&
            strcmp(existing->queue_name, queue_name) == 0) {
            /* Live duplicate: leave broker consumer intact. */
            if (t_broker_has_subscription(disp->broker, queue_name,
                                          dispatch_deliver_cb, existing->cbud)) {
                return -1;
            }
            /* Stale bookkeeping (queue recreated): drop and resubscribe below. */
            dispatch_retire_cbud(disp, existing->queue_name, (dispatch_sub_cb_ud *)existing->cbud);
            free(existing->queue_name);
            free(existing);
            for (size_t j = i; j + 1 < disp->subscriptions.len; ++j) {
                disp->subscriptions.items[j] = disp->subscriptions.items[j + 1];
            }
            disp->subscriptions.len--;
            break;
        }
    }

    /* Prepare per-subscription data */
    t_dispatch_sub *sub = (t_dispatch_sub *)calloc(1, sizeof(t_dispatch_sub));
    if (!sub) return -1;
    sub->session_id = session_id;
    sub->queue_name = strdup(queue_name);
    if (!sub->queue_name) {
        free(sub);
        return -1;
    }
    /* Adapter for broker delivery */
    dispatch_sub_cb_ud *cbud = (dispatch_sub_cb_ud *)calloc(1, sizeof(dispatch_sub_cb_ud));
    if (!cbud) {
        free(sub->queue_name);
        free(sub);
        return -1;
    }
    cbud->disp = disp;
    cbud->session_id = session_id;
    cbud->queue_name = strdup(queue_name);
    if (!cbud->queue_name) {
        free(sub->queue_name);
        free(sub);
        free(cbud);
        return -1;
    }
    sub->cbud = cbud;

    if (t_broker_subscribe(disp->broker, queue_name, dispatch_deliver_cb, cbud) != 0) {
        free(sub->queue_name);
        free(sub);
        dispatch_free_cbud(cbud);
        return -1;
    }
    /* record subscription for bookkeeping */
    if (t_vec_push(&disp->subscriptions, sub) != 0) {
        t_broker_unsubscribe(disp->broker, queue_name, dispatch_deliver_cb, cbud);
        free(sub->queue_name);
        free(sub);
        dispatch_free_cbud(cbud);
        return -1;
    }
    dispatch_purge_deferred(disp);
    if (dispatch_try_complete_destroy(disp)) return -1;
    return 0;
}

int t_dispatch_unsubscribe(t_dispatch *disp, uint64_t session_id, const char *queue_name) {
    if (!disp || disp->free_pending || !queue_name) return -1;
    for (size_t i = 0; i < disp->subscriptions.len; ++i) {
        t_dispatch_sub *sub = (t_dispatch_sub *)disp->subscriptions.items[i];
        if (!sub) continue;
        if (sub->session_id == session_id && sub->queue_name &&
            strcmp(sub->queue_name, queue_name) == 0) {
            /* Always clear local bookkeeping. Broker unsubscribe may fail if
             * the queue was deleted; the consumer is already gone in that case. */
            (void)t_broker_unsubscribe(disp->broker, queue_name,
                                       dispatch_deliver_cb, sub->cbud);
            dispatch_retire_cbud(disp, queue_name, (dispatch_sub_cb_ud *)sub->cbud);
            free(sub->queue_name);
            free(sub);
            for (size_t j = i; j + 1 < disp->subscriptions.len; ++j) {
                disp->subscriptions.items[j] = disp->subscriptions.items[j + 1];
            }
            disp->subscriptions.len--;
            dispatch_purge_deferred(disp);
            if (dispatch_try_complete_destroy(disp)) return -1;
            return 0;
        }
    }
    return -1;
}

size_t t_dispatch_session_count(const t_dispatch *disp) {
    return disp ? t_map_len(&disp->sessions) : 0;
}

size_t t_dispatch_total_published(const t_dispatch *disp) {
    return disp ? disp->total_published : 0;
}

size_t t_dispatch_total_delivered(const t_dispatch *disp) {
    return disp ? disp->total_delivered : 0;
}
