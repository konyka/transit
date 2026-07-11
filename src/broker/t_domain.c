#include "t_domain.h"
#include "t_map.h"
#include "t_vec.h"
#include "t_queue.h"
#include <stdlib.h>
#include <string.h>

typedef struct t_domain {
    char *name;
    t_map queues;
    t_vec sub_wrappers;
    size_t total_messages;
    size_t total_delivered;
    int accepting; /* 0 when owning broker is stopped */
} t_domain;

/* Internal wrapper for converting t_msg to user-provided callback */
typedef struct t_domain_sub_cb {
    void (*cb)(const char *, const uint8_t *, size_t, void *);
    void *ud;
} t_domain_sub_cb;

typedef struct t_domain_sub_ctx {
    t_domain_sub_cb cb;
    t_domain       *domain;
} t_domain_sub_ctx;

static void domain_consume_adapter(const t_msg *msg, void *ud) {
    t_domain_sub_ctx *ctx = (t_domain_sub_ctx *)ud;
    if (!ctx || !ctx->cb.cb) return;
    /* Count before user cb — unsubscribe may free ctx inside the callback. */
    t_domain *d = ctx->domain;
    if (d) d->total_delivered++;
    ctx->cb.cb(msg->queue_name, msg->data, msg->data_len, ctx->cb.ud);
}

t_domain *t_domain_create(const char *name) {
    if (!name) return NULL;
    t_domain *d = (t_domain *)calloc(1, sizeof(t_domain));
    if (!d) return NULL;
    d->name = strdup(name);
    if (!d->name) {
        free(d);
        return NULL;
    }
    t_map_init(&d->queues);
    t_vec_init(&d->sub_wrappers);
    d->total_messages = 0;
    d->total_delivered = 0;
    d->accepting = 1;
    return d;
}

void t_domain_destroy(t_domain *domain) {
    if (!domain) return;
    t_map_iter it = t_map_iter_begin(&domain->queues);
    const char *k; void *v;
    while (t_map_iter_next(&it, &k, &v)) {
        t_queue_destroy((t_queue *)v);
    }
    t_map_destroy(&domain->queues);
    for (size_t i = 0; i < t_vec_len(&domain->sub_wrappers); i++) {
        free(t_vec_get(&domain->sub_wrappers, i));
    }
    t_vec_destroy(&domain->sub_wrappers);
    free(domain->name);
    free(domain);
}

const char *t_domain_name(const t_domain *domain) {
    return domain ? domain->name : NULL;
}

void t_domain_set_accepting(t_domain *domain, int accepting) {
    if (!domain) return;
    domain->accepting = accepting ? 1 : 0;
}

int t_domain_is_accepting(const t_domain *domain) {
    return domain ? domain->accepting != 0 : 0;
}

int t_domain_create_queue(t_domain *domain, const char *queue_name, int type, int flags) {
    if (!domain || !queue_name) return -1;
    if (t_map_get(&domain->queues, queue_name)) return -1;
    t_queue *q = t_queue_create(queue_name, (t_qtype)type, flags);
    if (!q) return -1;
    if (t_map_insert(&domain->queues, queue_name, q) != 0) {
        t_queue_destroy(q);
        return -1;
    }
    return 0;
}

int t_domain_delete_queue(t_domain *domain, const char *queue_name) {
    if (!domain || !queue_name) return -1;
    void *ptr = t_map_get(&domain->queues, queue_name);
    if (!ptr) return -1;
    t_queue *q = (t_queue *)ptr;
    /* Drop domain subscription wrappers bound to this queue before destroy. */
    for (size_t i = 0; i < domain->sub_wrappers.len; ) {
        t_domain_sub_ctx *ctx = (t_domain_sub_ctx *)domain->sub_wrappers.items[i];
        if (ctx && t_queue_remove_consumer_ud(q, ctx) == 0) {
            free(ctx);
            for (size_t j = i; j + 1 < domain->sub_wrappers.len; ++j) {
                domain->sub_wrappers.items[j] = domain->sub_wrappers.items[j + 1];
            }
            domain->sub_wrappers.len--;
            continue;
        }
        ++i;
    }
    t_queue_destroy(q);
    t_map_remove(&domain->queues, queue_name);
    return 0;
}

size_t t_domain_queue_count(const t_domain *domain) {
    return domain ? t_map_len(&domain->queues) : 0;
}

void *t_domain_get_queue(t_domain *domain, const char *queue_name) {
    if (!domain || !queue_name) return NULL;
    return t_map_get(&domain->queues, queue_name);
}

int t_domain_publish(t_domain *domain, const char *queue_name,
                     const uint8_t *data, size_t len, int priority) {
    if (!domain || !domain->accepting || !queue_name) return -1;
    if (len > 0 && !data) return -1;
    t_queue *q = (t_queue *)t_map_get(&domain->queues, queue_name);
    if (!q) return -1;
    int r = t_queue_post(q, data, len, priority);
    if (r == 0) {
        domain->total_messages += 1;
    }
    return r;
}

int t_domain_subscribe(t_domain *domain, const char *queue_name,
                       void (*cb)(const char *, const uint8_t *, size_t, void *), void *ud) {
    if (!domain || !domain->accepting || !queue_name || !cb) return -1;
    t_queue *q = (t_queue *)t_map_get(&domain->queues, queue_name);
    if (!q) return -1;
    for (size_t i = 0; i < domain->sub_wrappers.len; ++i) {
        t_domain_sub_ctx *existing = (t_domain_sub_ctx *)domain->sub_wrappers.items[i];
        if (existing && existing->cb.cb == cb && existing->cb.ud == ud &&
            t_queue_has_consumer_ud(q, existing)) {
            return -1;
        }
    }
    t_domain_sub_ctx *ctx = (t_domain_sub_ctx *)malloc(sizeof(t_domain_sub_ctx));
    if (!ctx) return -1;
    ctx->cb.cb = cb;
    ctx->cb.ud = ud;
    ctx->domain = domain;

    uint64_t cid = t_queue_add_consumer(q, domain_consume_adapter, ctx);
    if (cid == 0) {
        free(ctx);
        return -1;
    }
    if (t_vec_push(&domain->sub_wrappers, ctx) != 0) {
        t_queue_remove_consumer_ud(q, ctx);
        free(ctx);
        return -1;
    }
    return 0;
}

int t_domain_unsubscribe(t_domain *domain, const char *queue_name,
                         void (*cb)(const char *, const uint8_t *, size_t, void *), void *ud) {
    if (!domain || !queue_name || !cb) return -1;
    t_queue *q = (t_queue *)t_map_get(&domain->queues, queue_name);
    if (!q) return -1;
    for (size_t i = 0; i < domain->sub_wrappers.len; ++i) {
        t_domain_sub_ctx *ctx = (t_domain_sub_ctx *)domain->sub_wrappers.items[i];
        if (!ctx || ctx->cb.cb != cb || ctx->cb.ud != ud) continue;
        if (t_queue_remove_consumer_ud(q, ctx) != 0) continue;
        free(ctx);
        for (size_t j = i; j + 1 < domain->sub_wrappers.len; ++j) {
            domain->sub_wrappers.items[j] = domain->sub_wrappers.items[j + 1];
        }
        domain->sub_wrappers.len--;
        return 0;
    }
    return -1;
}

size_t t_domain_total_messages(const t_domain *domain) {
    return domain ? domain->total_messages : 0;
}

size_t t_domain_total_delivered(const t_domain *domain) {
    return domain ? domain->total_delivered : 0;
}
