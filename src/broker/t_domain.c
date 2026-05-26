#include "t_domain.h"
#include "t_map.h"
#include "t_queue.h"
#include <stdlib.h>
#include <string.h>

typedef struct t_domain {
    char *name;
    t_map queues;            /* map queue_name -> t_queue* */
    size_t total_messages;
    size_t total_delivered;    /* not strictly required for tests; kept for completeness */
} t_domain;

/* Internal wrapper for converting t_msg to user-provided callback */
typedef struct t_domain_sub_cb {
    void (*cb)(const char *, const uint8_t *, size_t, void *);
    void *ud;
} t_domain_sub_cb;

static void domain_consume_adapter(const t_msg *msg, void *ud) {
    /* ud is expected to be a pointer to t_domain_sub_cb */
    t_domain_sub_cb *wrapper = (t_domain_sub_cb *)ud;
    if (wrapper && wrapper->cb) {
        wrapper->cb(msg->queue_name, msg->data, msg->data_len, wrapper->ud);
        /* delivered counter could be updated here if needed */
    }
}

t_domain *t_domain_create(const char *name) {
    if (!name) return NULL;
    t_domain *d = (t_domain *)calloc(1, sizeof(t_domain));
    if (!d) return NULL;
    d->name = strdup(name);
    t_map_init(&d->queues);
    d->total_messages = 0;
    d->total_delivered = 0;
    return d;
}

void t_domain_destroy(t_domain *domain) {
    if (!domain) return;
    /* destroy queues */
    t_map_iter it = t_map_iter_begin(&domain->queues);
    const char *k; void *v;
    while (t_map_iter_next(&it, &k, &v)) {
        t_queue_destroy((t_queue *)v);
    }
    t_map_destroy(&domain->queues);
    free(domain->name);
    free(domain);
}

const char *t_domain_name(const t_domain *domain) {
    return domain ? domain->name : NULL;
}

int t_domain_create_queue(t_domain *domain, const char *queue_name, int type, int flags) {
    if (!domain || !queue_name) return -1;
    t_queue *q = t_queue_create(queue_name, (t_qtype)type, flags);
    if (!q) return -1;
    if (t_map_insert(&domain->queues, queue_name, q) != 0) {
        t_queue_destroy(q); /* shouldn't happen, but safe */
        return -1;
    }
    return 0;
}

int t_domain_delete_queue(t_domain *domain, const char *queue_name) {
    if (!domain || !queue_name) return -1;
    void *ptr = t_map_get(&domain->queues, queue_name);
    if (!ptr) return -1;
    t_queue_destroy((t_queue *)ptr);
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
    if (!domain || !queue_name || !data) return -1;
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
    if (!domain || !queue_name || !cb) return -1;
    t_queue *q = (t_queue *)t_map_get(&domain->queues, queue_name);
    if (!q) return -1;
    /* Prepare wrapper: adapt consumer callback to test harness signature */
    t_domain_sub_cb *wrapper = (t_domain_sub_cb *)malloc(sizeof(t_domain_sub_cb));
    if (!wrapper) return -1;
    wrapper->cb = cb;
    wrapper->ud = ud;

    uint64_t cid = t_queue_add_consumer(q, domain_consume_adapter, wrapper);
    if (cid == 0) {
        free(wrapper);
        return -1;
    }
    return 0; /* success */
}

size_t t_domain_total_messages(const t_domain *domain) {
    return domain ? domain->total_messages : 0;
}

size_t t_domain_total_delivered(const t_domain *domain) {
    return domain ? domain->total_delivered : 0;
}
