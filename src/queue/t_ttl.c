#include "t_ttl.h"
#include "t_map.h"
#include "t_pqueue.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>

/*
 * TTL tracker: hashmap for O(1) lookup + min-heap by expire_at.
 * Heap nodes store msg_id (not entry pointers). Removals/replacements leave
 * stale heap nodes; expire() skips them when map miss or expire_at mismatch.
 * Complexity: add/remove O(log n), expire O(k log n) for k due entries.
 */

struct t_ttl {
    t_map           entries;
    t_pqueue        heap;
    t_ttl_expire_cb cb;
    void           *ud;
};

static void ttl_key(char *buf, size_t buflen, uint64_t msg_id) {
    snprintf(buf, buflen, "%llu", (unsigned long long)msg_id);
}

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
    if (t_pqueue_init(&ttl->heap, 16) != 0) {
        free(ttl);
        return NULL;
    }
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
    t_pqueue_destroy(&ttl->heap);
    free(ttl);
}

int t_ttl_add(t_ttl *ttl, uint64_t msg_id, const char *topic,
              const uint8_t *payload, size_t len, uint64_t expire_at) {
    if (!ttl) return -1;
    /* Heap stores expire_at as int64_t priority; reject values that wrap. */
    if (expire_at > (uint64_t)INT64_MAX) return -1;

    char key[32];
    ttl_key(key, sizeof(key), msg_id);

    t_ttl_entry *old = (t_ttl_entry *)t_map_get(&ttl->entries, key);

    t_ttl_entry *e = (t_ttl_entry *)calloc(1, sizeof(*e));
    if (!e) return -1;
    e->msg_id = msg_id;
    e->expire_at = expire_at;
    e->topic = topic ? strdup(topic) : NULL;
    if (topic && !e->topic) {
        ttl_entry_free(e);
        return -1;
    }
    if (payload && len > 0) {
        e->payload = (uint8_t *)malloc(len);
        if (!e->payload) {
            ttl_entry_free(e);
            return -1;
        }
        memcpy(e->payload, payload, len);
        e->payload_len = len;
    }

    if (t_map_insert(&ttl->entries, key, e) != 0) {
        ttl_entry_free(e);
        return -1;
    }
    /* Heap stores msg_id; stale nodes detected via expire_at mismatch. */
    if (t_pqueue_push(&ttl->heap, (int64_t)expire_at, (void *)(uintptr_t)msg_id) != 0) {
        t_map_remove(&ttl->entries, key);
        ttl_entry_free(e);
        if (old && t_map_insert(&ttl->entries, key, old) != 0) {
            ttl_entry_free(old);
        }
        return -1;
    }
    if (old) ttl_entry_free(old);
    return 0;
}

int t_ttl_remove(t_ttl *ttl, uint64_t msg_id) {
    if (!ttl) return -1;
    char key[32];
    ttl_key(key, sizeof(key), msg_id);
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
    t_pq_entry top;

    while (t_pqueue_peek(&ttl->heap, &top) == 0) {
        if ((uint64_t)top.priority > now) break;
        t_pqueue_pop(&ttl->heap, &top);

        uint64_t msg_id = (uint64_t)(uintptr_t)top.data;
        char key[32];
        ttl_key(key, sizeof(key), msg_id);
        t_ttl_entry *e = (t_ttl_entry *)t_map_get(&ttl->entries, key);
        /* Stale if removed, or replaced with a different expire_at. */
        if (!e || e->expire_at != (uint64_t)top.priority) continue;

        /* Remove before callback so reentrant t_ttl_remove cannot double-free. */
        t_map_remove(&ttl->entries, key);
        if (ttl->cb) ttl->cb(e, ttl->ud);
        ttl_entry_free(e);
        expired++;
    }

    /* Compact: rebuild heap when stale nodes dominate (heap > 4x live). */
    size_t live = t_map_len(&ttl->entries);
    size_t hlen = t_pqueue_len(&ttl->heap);
    if (hlen > 64 && live > 0 && hlen > live * 4) {
        t_pqueue fresh;
        if (t_pqueue_init(&fresh, live) == 0) {
            int ok = 1;
            t_map_iter it = t_map_iter_begin(&ttl->entries);
            const char *k;
            void *val;
            while (t_map_iter_next(&it, &k, &val)) {
                t_ttl_entry *e = (t_ttl_entry *)val;
                if (t_pqueue_push(&fresh, (int64_t)e->expire_at, (void *)(uintptr_t)e->msg_id) != 0) {
                    ok = 0;
                    break;
                }
            }
            if (ok) {
                t_pqueue_destroy(&ttl->heap);
                ttl->heap = fresh;
            } else {
                t_pqueue_destroy(&fresh);
            }
        }
    }
    return expired;
}

size_t t_ttl_count(const t_ttl *ttl) {
    return ttl ? t_map_len(&ttl->entries) : 0;
}

int t_ttl_is_expired(const t_ttl *ttl, uint64_t msg_id, uint64_t now) {
    if (!ttl) return 1;
    char key[32];
    ttl_key(key, sizeof(key), msg_id);
    t_ttl_entry *e = (t_ttl_entry *)t_map_get(&ttl->entries, key);
    if (!e) return 1;
    return e->expire_at <= now ? 1 : 0;
}
