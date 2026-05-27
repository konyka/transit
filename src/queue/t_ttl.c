#include "t_ttl.h"
#include "t_map.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

struct t_ttl {
    t_map           entries;
    t_ttl_expire_cb cb;
    void           *ud;
};

static void ttl_entry_free(t_ttl_entry *e) {
    if (e) {
        free(e->topic);
        free(e->payload);
        free(e);
    }
}

t_ttl *t_ttl_create(t_ttl_expire_cb cb, void *ud) {
    t_ttl *ttl = (t_ttl *)calloc(1, sizeof(*ttl));
    if (!ttl) return NULL;
    t_map_init(&ttl->entries);
    ttl->cb = cb;
    ttl->ud = ud;
    return ttl;
}

void t_ttl_destroy(t_ttl *ttl) {
    if (!ttl) return;
    t_map_iter it = t_map_iter_begin(&ttl->entries);
    const char *key;
    void *val;
    while (t_map_iter_next(&it, &key, &val)) {
        ttl_entry_free((t_ttl_entry *)val);
    }
    t_map_destroy(&ttl->entries);
    free(ttl);
}

int t_ttl_add(t_ttl *ttl, uint64_t msg_id, const char *topic,
              const uint8_t *payload, size_t len, uint64_t expire_at) {
    if (!ttl) return -1;
    t_ttl_entry *e = (t_ttl_entry *)calloc(1, sizeof(*e));
    if (!e) return -1;
    e->msg_id = msg_id;
    e->expire_at = expire_at;
    e->topic = topic ? strdup(topic) : NULL;
    if (payload && len > 0) {
        e->payload = (uint8_t *)malloc(len);
        if (e->payload) {
            memcpy(e->payload, payload, len);
        }
        e->payload_len = len;
    }
    char key[32];
    snprintf(key, sizeof(key), "%llu", (unsigned long long)msg_id);
    return t_map_insert(&ttl->entries, key, e);
}

int t_ttl_remove(t_ttl *ttl, uint64_t msg_id) {
    if (!ttl) return -1;
    char key[32];
    snprintf(key, sizeof(key), "%llu", (unsigned long long)msg_id);
    void *old = t_map_remove(&ttl->entries, key);
    if (old) {
        ttl_entry_free((t_ttl_entry *)old);
        return 0;
    }
    return -1;
}

size_t t_ttl_expire(t_ttl *ttl, uint64_t now) {
    if (!ttl) return 0;
    size_t expired = 0;
    uint64_t ids[256];
    size_t nids = 0;

    t_map_iter it = t_map_iter_begin(&ttl->entries);
    const char *key;
    void *val;
    while (t_map_iter_next(&it, &key, &val)) {
        t_ttl_entry *e = (t_ttl_entry *)val;
        if (e && e->expire_at <= now) {
            if (nids < 256) {
                ids[nids++] = e->msg_id;
            }
            if (ttl->cb) {
                ttl->cb(e, ttl->ud);
            }
            expired++;
        }
    }

    for (size_t i = 0; i < nids; i++) {
        t_ttl_remove(ttl, ids[i]);
    }
    return expired;
}

size_t t_ttl_count(const t_ttl *ttl) {
    return ttl ? t_map_len(&ttl->entries) : 0;
}

int t_ttl_is_expired(const t_ttl *ttl, uint64_t msg_id, uint64_t now) {
    if (!ttl) return 1;
    char key[32];
    snprintf(key, sizeof(key), "%llu", (unsigned long long)msg_id);
    t_ttl_entry *e = (t_ttl_entry *)t_map_get(&ttl->entries, key);
    if (!e) return 1;
    return e->expire_at <= now ? 1 : 0;
}
