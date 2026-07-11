#include <stdlib.h>
#include <string.h>
#include "t_map.h"

/* 64-bit FNV-1a hash */
static uint64_t t_hash_str(const char *s) {
    const uint64_t fnv_basis = 0xcbf29ce484222325ULL;
    const uint64_t fnv_prime = 0x100000001b3ULL;
    uint64_t hash = fnv_basis;
    while (*s) {
        hash ^= (unsigned char)(*s++);
        hash *= fnv_prime;
    }
    return hash;
}

static int t_map_resize(t_map *m, size_t new_cap);

void t_map_init(t_map *m) {
    if (!m) return;
    m->entries = NULL;
    m->cap = 0;
    m->len = 0;
}

void t_map_destroy(t_map *m) {
    if (!m) return;
    if (m->entries) {
        for (size_t i = 0; i < m->cap; ++i) {
            if (m->entries[i].key) {
                free(m->entries[i].key);
            }
        }
        free(m->entries);
    }
    m->entries = NULL;
    m->cap = 0;
    m->len = 0;
}

static int t_map_resize(t_map *m, size_t new_cap) {
    t_map_entry *old = m->entries;
    size_t old_cap = m->cap;
    t_map_entry *new_entries = (t_map_entry *)calloc(new_cap, sizeof(t_map_entry));
    if (!new_entries) return -1;
    size_t new_len = 0;
    for (size_t i = 0; i < old_cap; ++i) {
        t_map_entry *e = &old[i];
        if (e->occupied == 1 && e->key) {
            size_t idx = (size_t)(t_hash_str(e->key) % new_cap);
            int placed = 0;
            for (size_t j = 0; j < new_cap; ++j) {
                size_t probe = (idx + j) % new_cap;
                t_map_entry *ne = &new_entries[probe];
                if (ne->occupied == 0 || ne->occupied == 2) {
                    ne->key = e->key; /* move ownership after full success */
                    ne->val = e->val;
                    ne->occupied = 1;
                    new_len++;
                    placed = 1;
                    break;
                }
            }
            if (!placed) {
                /* Keys still owned by old table; drop new table without freeing keys. */
                free(new_entries);
                return -1;
            }
        }
    }
    for (size_t i = 0; i < old_cap; ++i) {
        if (old[i].occupied != 1 && old[i].key) {
            free(old[i].key);
        }
    }
    free(old);
    m->entries = new_entries;
    m->cap = new_cap;
    m->len = new_len;
    return 0;
}

static int t_map_grow_if_needed(t_map *m) {
    if (m->cap == 0) {
        return t_map_resize(m, 8);
    }
    if (m->len * 10 >= m->cap * 7) {
        return t_map_resize(m, m->cap * 2);
    }
    return 0;
}

int t_map_insert(t_map *m, const char *key, void *val) {
    if (!m || !key) return -1;
    if (t_map_grow_if_needed(m) != 0) return -1;
    if (m->cap == 0) return -1;
    size_t cap = m->cap;
    uint64_t h = t_hash_str(key);
    size_t idx = (size_t)(h % (uint64_t)cap);
    size_t first_tomb = (size_t)-1;
    for (size_t i = 0; i < cap; ++i) {
        size_t probe = (idx + i) % cap;
        t_map_entry *e = &m->entries[probe];
        if (e->occupied == 0) {
            if (first_tomb != (size_t)-1) {
                e = &m->entries[first_tomb];
            }
            if (e->key) free(e->key);
            e->key = strdup(key);
            if (!e->key) return -1;
            e->val = val;
            e->occupied = 1;
            m->len++;
            return 0;
        } else if (e->occupied == 2) {
            if (first_tomb == (size_t)-1) first_tomb = probe;
            continue;
        } else if (e->occupied == 1 && e->key && strcmp(e->key, key) == 0) {
            e->val = val;
            return 0;
        }
    }
    /* if we reach here, need to resize and retry */
    if (t_map_resize(m, m->cap * 2) == 0) {
        return t_map_insert(m, key, val);
    }
    return -1;
}

void *t_map_get(const t_map *m, const char *key) {
    if (!m || m->cap == 0 || !key) return NULL;
    uint64_t h = t_hash_str(key);
    size_t cap = m->cap;
    size_t idx = (size_t)(h % (uint64_t)cap);
    for (size_t i = 0; i < cap; ++i) {
        size_t probe = (idx + i) % cap;
        t_map_entry *e = &m->entries[probe];
        if (e->occupied == 0) return NULL;
        if (e->occupied == 1 && e->key && strcmp(e->key, key) == 0) {
            return e->val;
        }
    }
    return NULL;
}

void *t_map_remove(t_map *m, const char *key) {
    if (!m || m->cap == 0 || !key) return NULL;
    uint64_t h = t_hash_str(key);
    size_t cap = m->cap;
    size_t idx = (size_t)(h % (uint64_t)cap);
    for (size_t i = 0; i < cap; ++i) {
        size_t probe = (idx + i) % cap;
        t_map_entry *e = &m->entries[probe];
        if (e->occupied == 0) return NULL;
        if (e->occupied == 1 && e->key && strcmp(e->key, key) == 0) {
            void *ret = e->val;
            e->occupied = 2; /* tombstone */
            // do not free key to keep tombstone semantics consistent
            m->len--;
            return ret;
        }
    }
    return NULL;
}

int t_map_contains(const t_map *m, const char *key) {
    return t_map_get(m, key) != NULL;
}

size_t t_map_len(const t_map *m) {
    return m ? m->len : 0;
}

void t_map_clear(t_map *m) {
    if (!m) return;
    if (m->entries) {
        for (size_t i = 0; i < m->cap; ++i) {
            if (m->entries[i].occupied == 1 && m->entries[i].key) {
                free(m->entries[i].key);
            }
            m->entries[i].key = NULL;
            m->entries[i].val = NULL;
            m->entries[i].occupied = 0;
        }
        m->len = 0;
    }
}

/* Iteration */
t_map_iter t_map_iter_begin(const t_map *m) {
    t_map_iter it;
    it.map = m;
    it.index = 0;
    return it;
}

int t_map_iter_next(t_map_iter *it, const char **key, void **val) {
    if (!it || !it->map) return 0;
    while (it->index < it->map->cap) {
        t_map_entry *e = &it->map->entries[it->index++];
        if (e->occupied == 1 && e->key) {
            if (key) *key = e->key;
            if (val) *val = e->val;
            return 1;
        }
    }
    return 0;
}
