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
    uint64_t session_id;
    t_dispatch *disp;
} dispatch_sub_cb_ud;

/* Dispatch object */
struct t_dispatch {
    t_broker *broker;
    t_map      sessions;      /* session_id (string) -> t_session* */
    t_vec      subscriptions; /* array of t_dispatch_sub * (dynamically allocated) */
    size_t     total_published;
    size_t     total_delivered;
};

/* Helpers */
static void dispatch_deliver_cb(const char *queue_name, const uint8_t *data,
                              size_t len, void *ud) {
    (void)queue_name; /* not used directly here, per session routing we derive by ud */
    dispatch_sub_cb_ud *wrapper = (dispatch_sub_cb_ud *)ud;
    if (!wrapper || !wrapper->disp) return;
    t_dispatch *disp = wrapper->disp;
    /* Build string key from session_id */
    char key[32];
    snprintf(key, sizeof(key), "%llu", (unsigned long long)wrapper->session_id);
    t_session *sess = (t_session *)t_map_get(&disp->sessions, key);
    if (!sess) return;
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
    t_map_init(&d->sessions);
    t_vec_init(&d->subscriptions);
    d->total_published = 0;
    d->total_delivered = 0;
    return d;
}

void t_dispatch_destroy(t_dispatch *disp) {
    if (!disp) return;
    /* Unsubscribe from broker before freeing cbud (avoids UAF on late delivery). */
    if (disp->subscriptions.items) {
        for (size_t i = 0; i < disp->subscriptions.len; ++i) {
            t_dispatch_sub *sub = (t_dispatch_sub *)disp->subscriptions.items[i];
            if (!sub) continue;
            if (sub->queue_name && sub->cbud) {
                t_broker_unsubscribe(disp->broker, sub->queue_name,
                                     dispatch_deliver_cb, sub->cbud);
            }
            free(sub->queue_name);
            free(sub->cbud);
            free(sub);
        }
    }
    t_vec_destroy(&disp->subscriptions);
    t_map_destroy(&disp->sessions);
    free(disp);
}

int t_dispatch_register(t_dispatch *disp, uint64_t session_id, t_session *sess) {
    if (!disp || !sess) return -1;
    char key[32];
    snprintf(key, sizeof(key), "%llu", (unsigned long long)session_id);
    if (t_map_contains(&disp->sessions, key)) return -1;
    if (t_map_insert(&disp->sessions, key, sess) != 0) return -1;
    return 0;
}

int t_dispatch_unregister(t_dispatch *disp, uint64_t session_id) {
    if (!disp) return -1;
    char key[32];
    snprintf(key, sizeof(key), "%llu", (unsigned long long)session_id);
    void *v = t_map_remove(&disp->sessions, key);
    if (!v) return -1;
    /* Drop broker subscriptions for this session to avoid ghost deliveries. */
    for (size_t i = 0; i < disp->subscriptions.len; ) {
        t_dispatch_sub *sub = (t_dispatch_sub *)disp->subscriptions.items[i];
        if (sub && sub->session_id == session_id) {
            if (sub->queue_name && sub->cbud) {
                t_broker_unsubscribe(disp->broker, sub->queue_name,
                                     dispatch_deliver_cb, sub->cbud);
            }
            free(sub->queue_name);
            free(sub->cbud);
            free(sub);
            for (size_t j = i; j + 1 < disp->subscriptions.len; ++j) {
                disp->subscriptions.items[j] = disp->subscriptions.items[j + 1];
            }
            disp->subscriptions.len--;
            continue;
        }
        ++i;
    }
    return 0;
}

int t_dispatch_publish(t_dispatch *disp, uint64_t session_id,
                       const char *queue_name, const uint8_t *data, size_t len, int priority) {
    if (!disp || !queue_name) return -1;
    char key[32];
    snprintf(key, sizeof(key), "%llu", (unsigned long long)session_id);
    void *sess = t_map_get(&disp->sessions, key);
    if (!sess) return -1;
    int r = t_broker_publish(disp->broker, queue_name, data, len, priority);
    if (r == 0) disp->total_published++;
    return r;
}

int t_dispatch_subscribe(t_dispatch *disp, uint64_t session_id, const char *queue_name) {
    if (!disp || !queue_name) return -1;
    /* verify session exists */
    char key[32];
    snprintf(key, sizeof(key), "%llu", (unsigned long long)session_id);
    if (!t_map_contains(&disp->sessions, key)) return -1;

    for (size_t i = 0; i < disp->subscriptions.len; ++i) {
        t_dispatch_sub *existing = (t_dispatch_sub *)disp->subscriptions.items[i];
        if (existing && existing->session_id == session_id && existing->queue_name &&
            strcmp(existing->queue_name, queue_name) == 0) {
            return -1;
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
    sub->cbud = cbud;

    if (t_broker_subscribe(disp->broker, queue_name, dispatch_deliver_cb, cbud) != 0) {
        free(sub->queue_name);
        free(sub);
        free(cbud);
        return -1;
    }
    /* record subscription for bookkeeping */
    if (t_vec_push(&disp->subscriptions, sub) != 0) {
        t_broker_unsubscribe(disp->broker, queue_name, dispatch_deliver_cb, cbud);
        free(sub->queue_name);
        free(sub);
        free(cbud);
        return -1;
    }
    return 0;
}

int t_dispatch_unsubscribe(t_dispatch *disp, uint64_t session_id, const char *queue_name) {
    if (!disp || !queue_name) return -1;
    for (size_t i = 0; i < disp->subscriptions.len; ++i) {
        t_dispatch_sub *sub = (t_dispatch_sub *)disp->subscriptions.items[i];
        if (!sub) continue;
        if (sub->session_id == session_id && sub->queue_name &&
            strcmp(sub->queue_name, queue_name) == 0) {
            t_broker_unsubscribe(disp->broker, queue_name, dispatch_deliver_cb, sub->cbud);
            free(sub->queue_name);
            free(sub->cbud);
            free(sub);
            for (size_t j = i; j + 1 < disp->subscriptions.len; ++j) {
                disp->subscriptions.items[j] = disp->subscriptions.items[j + 1];
            }
            disp->subscriptions.len--;
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
