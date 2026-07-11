#include "t_cgroup.h"
#include <stdlib.h>
#include <string.h>
#include <stdint.h>

typedef struct {
    char                *id;
    t_cgroup_deliver_cb  cb;
    void                *ud;
} t_consumer;

struct t_cgroup {
    char        *group_id;
    t_consumer  *consumers;
    size_t       consumer_count;
    size_t       consumer_cap;
    size_t       next_idx;
    uint64_t     total_dispatched;
};

t_cgroup *t_cgroup_create(const char *group_id) {
    t_cgroup *cg = (t_cgroup *)calloc(1, sizeof(*cg));
    if (!cg) return NULL;
    cg->group_id = group_id ? strdup(group_id) : strdup("default");
    if (!cg->group_id) {
        free(cg);
        return NULL;
    }
    cg->consumer_cap = 8;
    cg->consumers = (t_consumer *)calloc(cg->consumer_cap, sizeof(t_consumer));
    if (!cg->consumers) {
        free(cg->group_id);
        free(cg);
        return NULL;
    }
    return cg;
}

void t_cgroup_destroy(t_cgroup *cg) {
    if (!cg) return;
    for (size_t i = 0; i < cg->consumer_count; i++) {
        free(cg->consumers[i].id);
    }
    free(cg->consumers);
    free(cg->group_id);
    free(cg);
}

int t_cgroup_add_consumer(t_cgroup *cg, const char *consumer_id, t_cgroup_deliver_cb cb, void *ud) {
    if (!cg || !consumer_id || !cb) return -1;
    for (size_t i = 0; i < cg->consumer_count; i++) {
        if (strcmp(cg->consumers[i].id, consumer_id) == 0) return -1;
    }
    if (cg->consumer_count >= cg->consumer_cap) {
        if (cg->consumer_cap > SIZE_MAX / 2) return -1;
        size_t new_cap = cg->consumer_cap * 2;
        if (new_cap > SIZE_MAX / sizeof(t_consumer)) return -1;
        t_consumer *tmp = (t_consumer *)realloc(cg->consumers, new_cap * sizeof(t_consumer));
        if (!tmp) return -1;
        cg->consumers = tmp;
        cg->consumer_cap = new_cap;
    }
    t_consumer *c = &cg->consumers[cg->consumer_count];
    c->id = strdup(consumer_id);
    if (!c->id) return -1;
    c->cb = cb;
    c->ud = ud;
    cg->consumer_count++;
    return 0;
}

int t_cgroup_remove_consumer(t_cgroup *cg, const char *consumer_id) {
    if (!cg || !consumer_id) return -1;
    for (size_t i = 0; i < cg->consumer_count; i++) {
        if (strcmp(cg->consumers[i].id, consumer_id) == 0) {
            free(cg->consumers[i].id);
            cg->consumers[i] = cg->consumers[cg->consumer_count - 1];
            if (i < cg->next_idx) cg->next_idx--;
            cg->consumer_count--;
            if (cg->consumer_count == 0 || cg->next_idx >= cg->consumer_count)
                cg->next_idx = 0;
            return 0;
        }
    }
    return -1;
}

int t_cgroup_dispatch(t_cgroup *cg, const char *topic, const uint8_t *payload, size_t len) {
    if (!cg || cg->consumer_count == 0) return -1;
    t_consumer *c = &cg->consumers[cg->next_idx];
    if (c->cb) {
        c->cb(topic, payload, len, c->ud);
    }
    cg->next_idx = (cg->next_idx + 1) % cg->consumer_count;
    cg->total_dispatched++;
    return 0;
}

size_t t_cgroup_consumer_count(const t_cgroup *cg) {
    return cg ? cg->consumer_count : 0;
}

uint64_t t_cgroup_total_dispatched(const t_cgroup *cg) {
    return cg ? cg->total_dispatched : 0;
}

const char *t_cgroup_id(const t_cgroup *cg) {
    return cg ? cg->group_id : NULL;
}
