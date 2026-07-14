#include "t_broker.h"
#include "t_domain.h"
#include "t_map.h"
#include <stdlib.h>
#include <string.h>

typedef struct t_broker {
    char *broker_id;
    t_map domains; /* map domain_name -> t_domain* */
    int running;
} t_broker;

static t_domain *broker_find_queue_domain(t_broker *broker, const char *queue_name) {
    t_domain *def = t_broker_get_domain(broker, "default");
    if (def && t_domain_get_queue(def, queue_name)) return def;
    t_map_iter it = t_map_iter_begin(&broker->domains);
    const char *k; void *v;
    while (t_map_iter_next(&it, &k, &v)) {
        t_domain *d = (t_domain *)v;
        if (d == def) continue;
        if (t_domain_get_queue(d, queue_name)) return d;
    }
    return NULL;
}

/* Helpers */
t_broker *t_broker_create(const char *broker_id) {
    if (!broker_id) return NULL;
    t_broker *b = (t_broker *)calloc(1, sizeof(t_broker));
    if (!b) return NULL;
    b->broker_id = strdup(broker_id);
    if (!b->broker_id) {
        free(b);
        return NULL;
    }
    t_map_init(&b->domains);
    b->running = 0;
    /* Create default domain automatically */
    t_domain *def = t_domain_create("default");
    if (!def) {
        free(b->broker_id);
        free(b);
        return NULL;
    }
    if (t_map_insert(&b->domains, "default", def) != 0) {
        t_domain_destroy(def);
        free(b->broker_id);
        free(b);
        return NULL;
    }
    t_domain_set_accepting(def, 0); /* closed until t_broker_start */
    return b;
}

void t_broker_destroy(t_broker *broker) {
    if (!broker) return;
    /* destroy all domains */
    t_map_iter it = t_map_iter_begin(&broker->domains);
    const char *k; void *v;
    while (t_map_iter_next(&it, &k, &v)) {
        t_domain_destroy((t_domain *)v);
    }
    t_map_destroy(&broker->domains);
    free(broker->broker_id);
    free(broker);
}

const char *t_broker_id(const t_broker *broker) {
    return broker ? broker->broker_id : NULL;
}

int t_broker_start(t_broker *broker) {
    if (!broker) return -1;
    broker->running = 1;
    t_map_iter it = t_map_iter_begin(&broker->domains);
    const char *k; void *v;
    while (t_map_iter_next(&it, &k, &v)) {
        t_domain_set_accepting((t_domain *)v, 1);
    }
    return 0;
}

int t_broker_stop(t_broker *broker) {
    if (!broker) return -1;
    broker->running = 0;
    t_map_iter it = t_map_iter_begin(&broker->domains);
    const char *k; void *v;
    while (t_map_iter_next(&it, &k, &v)) {
        t_domain_set_accepting((t_domain *)v, 0);
    }
    return 0;
}

int t_broker_is_running(const t_broker *broker) {
    return broker ? broker->running != 0 : 0;
}

t_domain *t_broker_create_domain(t_broker *broker, const char *domain_name) {
    if (!broker || !domain_name) return NULL;
    if (t_map_contains(&broker->domains, domain_name)) {
        return (t_domain *)t_map_get(&broker->domains, domain_name);
    }
    t_domain *d = t_domain_create(domain_name);
    if (!d) return NULL;
    t_domain_set_accepting(d, broker->running);
    if (t_map_insert(&broker->domains, domain_name, d) != 0) {
        t_domain_destroy(d);
        return NULL;
    }
    return d;
}

t_domain *t_broker_get_domain(t_broker *broker, const char *domain_name) {
    if (!broker || !domain_name) return NULL;
    return (t_domain *)t_map_get(&broker->domains, domain_name);
}

int t_broker_remove_domain(t_broker *broker, const char *domain_name) {
    if (!broker || !domain_name) return -1;
    if (strcmp(domain_name, "default") == 0) return -1;
    void *v = t_map_remove(&broker->domains, domain_name);
    if (!v) return -1;
    t_domain_destroy((t_domain *)v);
    return 0;
}

size_t t_broker_domain_count(const t_broker *broker) {
    return broker ? t_map_len(&broker->domains) : 0;
}

int t_broker_create_queue(t_broker *broker, const char *domain_name,
                          const char *queue_name, int type, int flags) {
    if (!broker || !domain_name || !queue_name) return -1;
    t_domain *owner = broker_find_queue_domain(broker, queue_name);
    if (owner && strcmp(t_domain_name(owner), domain_name) != 0) {
        return -1; /* queue name already owned by another domain */
    }
    t_domain *d = t_broker_get_domain(broker, domain_name);
    if (!d) {
        d = t_broker_create_domain(broker, domain_name);
        if (!d) return -1;
    }
    int r = t_domain_create_queue(d, queue_name, type, flags);
    return r;
}

int t_broker_delete_queue(t_broker *broker, const char *domain_name,
                          const char *queue_name) {
    if (!broker || !domain_name || !queue_name) return -1;
    t_domain *d = t_broker_get_domain(broker, domain_name);
    if (!d) return -1;
    return t_domain_delete_queue(d, queue_name);
}

int t_broker_publish(t_broker *broker, const char *queue_name,
                     const uint8_t *data, size_t len, int priority) {
    if (!broker || !broker->running || !queue_name || (len > 0 && !data)) return -1;
    t_domain *d = broker_find_queue_domain(broker, queue_name);
    if (!d) return -1;
    return t_domain_publish(d, queue_name, data, len, priority);
}

int t_broker_subscribe(t_broker *broker, const char *queue_name,
                        t_broker_msg_cb cb, void *ud) {
    if (!broker || !broker->running || !queue_name || !cb) return -1;
    t_domain *d = broker_find_queue_domain(broker, queue_name);
    if (!d) return -1;
    return t_domain_subscribe(d, queue_name, cb, ud);
}

int t_broker_unsubscribe(t_broker *broker, const char *queue_name,
                         t_broker_msg_cb cb, void *ud) {
    if (!broker || !queue_name || !cb) return -1;
    t_domain *d = broker_find_queue_domain(broker, queue_name);
    if (!d) return -1;
    return t_domain_unsubscribe(d, queue_name, cb, ud);
}

int t_broker_has_subscription(t_broker *broker, const char *queue_name,
                              t_broker_msg_cb cb, void *ud) {
    if (!broker || !queue_name || !cb) return 0;
    t_domain *d = broker_find_queue_domain(broker, queue_name);
    if (!d) return 0;
    return t_domain_has_subscription(d, queue_name, cb, ud);
}

size_t t_broker_total_queues(const t_broker *broker) {
    if (!broker) return 0;
    size_t sum = 0;
    t_map_iter it = t_map_iter_begin(&broker->domains);
    const char *k; void *v;
    while (t_map_iter_next(&it, &k, &v)) {
        sum += t_domain_queue_count((t_domain *)v);
    }
    return sum;
}

size_t t_broker_total_messages(const t_broker *broker) {
    if (!broker) return 0;
    size_t sum = 0;
    t_map_iter it = t_map_iter_begin(&broker->domains);
    const char *k; void *v;
    while (t_map_iter_next(&it, &k, &v)) {
        sum += t_domain_total_messages((t_domain *)v);
    }
    return sum;
}

size_t t_broker_total_delivered(const t_broker *broker) {
    if (!broker) return 0;
    size_t sum = 0;
    t_map_iter it = t_map_iter_begin(&broker->domains);
    const char *k; void *v;
    while (t_map_iter_next(&it, &k, &v)) {
        sum += t_domain_total_delivered((t_domain *)v);
    }
    return sum;
}
