#include "t_storage.h"
#include "t_map.h"
#include "t_file.h"
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <stdio.h>

/* Align with wire payload limit; reject oversized entries on put/load. */
#define T_STORAGE_MAX_VALUE (16 * 1024 * 1024)
/* Whole-file load is in-memory; refuse pathological dumps. */
#define T_STORAGE_MAX_FILE  (256 * 1024 * 1024)

/* Private storage implementation */
/* We forward-declare the internal structure to be compatible with the
   header which defines 'typedef struct t_storage t_storage;'. */
struct t_storage {
    t_storage_type type;
    t_map        map;      /* key (as string) -> t_storage_entry* */
    int          dirty;    /* for file-backed storage */
    char        *path;     /* file path for persistence (optional) */
    int          iterating;   /* foreach callback nest guard */
    int          free_pending; /* destroy deferred until foreach returns */
};

/* Helpers: convert 64-bit key to string for map */
static void key_to_str(uint64_t key, char *buf, size_t bufsz) {
    snprintf(buf, bufsz, "%016llu", (unsigned long long)key);
}

/* Helpers: determine if a string is a decimal representation of uint64 */
static uint64_t str_to_key(const char *s) {
    if (!s) return 0ULL;
    unsigned long long v = 0;
    sscanf(s, "%llu", &v);
    return (uint64_t)v;
}

static uint64_t read_le64(const uint8_t *p) {
    return (uint64_t)p[0]
         | ((uint64_t)p[1] << 8)
         | ((uint64_t)p[2] << 16)
         | ((uint64_t)p[3] << 24)
         | ((uint64_t)p[4] << 32)
         | ((uint64_t)p[5] << 40)
         | ((uint64_t)p[6] << 48)
         | ((uint64_t)p[7] << 56);
}

static void write_le64(uint8_t *p, uint64_t v) {
    for (int i = 0; i < 8; i++)
        p[i] = (uint8_t)(v >> (8 * i));
}

/* Load file format: [key(8 bytes little-endian)] [len(8 bytes little-endian)] [data(len)] ...
   This is a simple binary dump to support basic persistence. */
static int t_storage_fs_load(t_storage *st, const char *path) {
    if (!st || !path) return -1;
    t_file f;
    t_file_init(&f);
    if (t_file_open(&f, path, T_FILE_READ) != 0) {
        /* Missing file = empty store (created on first flush/destroy). */
        return t_file_not_found() ? 0 : -1;
    }
    uint64_t fsz = 0;
    if (t_file_size(&f, &fsz) != 0) { t_file_close(&f); return -1; }
    if (fsz > (uint64_t)SIZE_MAX || fsz > (uint64_t)T_STORAGE_MAX_FILE) {
        t_file_close(&f);
        return -1;
    }
    size_t total = (size_t)fsz;
    if (total == 0) { t_file_close(&f); return 0; }
    uint8_t *buf = (uint8_t*)malloc(total);
    if (!buf) { t_file_close(&f); return -1; }
    if (t_file_read(&f, buf, total) != 0) {
        t_file_close(&f); free(buf); return -1;
    }
    t_file_close(&f);
    size_t pos = 0;
    char keybuf[32];
    while (pos + 16 <= total) {
        uint64_t key = read_le64(buf + pos); pos += 8;
        uint64_t len = read_le64(buf + pos); pos += 8;
        if (len > (uint64_t)(total - pos)) { free(buf); return -1; }
        if (len > T_STORAGE_MAX_VALUE) { free(buf); return -1; }
        if (len > 0) {
            t_storage_entry *entry = (t_storage_entry*)malloc(sizeof(t_storage_entry));
            if (!entry) { free(buf); return -1; }
            entry->data = (uint8_t*)malloc((size_t)len);
            if (!(entry->data)) { free(entry); free(buf); return -1; }
            memcpy(entry->data, buf + pos, (size_t)len);
            entry->data_len = (size_t)len;
            entry->key = key;
            pos += (size_t)len;
            key_to_str(key, keybuf, sizeof(keybuf));
            t_storage_entry *old = (t_storage_entry*)t_map_get(&st->map, keybuf);
            if (t_map_insert(&st->map, keybuf, entry) != 0) {
                free(entry->data);
                free(entry);
                free(buf);
                return -1;
            }
            if (old) {
                free(old->data);
                free(old);
            }
        } else {
            t_storage_entry *entry = (t_storage_entry*)calloc(1, sizeof(t_storage_entry));
            if (!entry) { free(buf); return -1; }
            entry->key = key;
            entry->data = NULL;
            entry->data_len = 0;
            key_to_str(key, keybuf, sizeof(keybuf));
            t_storage_entry *old = (t_storage_entry*)t_map_get(&st->map, keybuf);
            if (t_map_insert(&st->map, keybuf, entry) != 0) {
                free(entry);
                free(buf);
                return -1;
            }
            if (old) {
                free(old->data);
                free(old);
            }
        }
    }
    if (pos != total) { free(buf); return -1; }
    free(buf);
    return 0;
}


/* Public API implementations (mem and file variants) */

t_storage *t_storage_create(t_storage_type type, const char *path) {
    (void)path; /* path is optional for mem */
    t_storage *s = (t_storage*)calloc(1, sizeof(t_storage));
    if (!s) return NULL;
    s->type = type;
    s->dirty = 0;
    s->path = NULL;
    t_map_init(&s->map);
    if (path && path[0] != '\0') {
        s->path = strdup(path);
        if (!s->path) {
            t_map_destroy(&s->map);
            free(s);
            return NULL;
        }
        /* If file-backed, try to load existing data into memoryMap */
        if (type == T_STORAGE_FILE) {
            if (t_storage_fs_load(s, path) != 0) {
                t_storage_destroy(s);
                return NULL;
            }
        }
    }
    return s;
}

/* force=1: best-effort flush then always free (deferred destroy must not leak). */
static int storage_destroy_impl(t_storage *storage, int force) {
    if (!storage) return 0;
    if (storage->iterating) {
        storage->free_pending = 1;
        return -1; /* deferred; caller must not assume freed */
    }
    storage->free_pending = 0;
    /* Persist dirty file-backed state before tearing down the map. */
    if (storage->type == T_STORAGE_FILE && storage->dirty) {
        if (t_storage_flush(storage) != 0 && !force) return -1;
    }
    /* Free stored entries */
    t_map_iter it = t_map_iter_begin(&storage->map);
    const char *k;
    void *v;
    while (t_map_iter_next(&it, &k, &v)) {
        t_storage_entry *e = (t_storage_entry*)v;
        if (e) {
            free(e->data);
            free(e);
        }
    }
    t_map_destroy(&storage->map);
    if (storage->path) free(storage->path);
    free(storage);
    return 0;
}

int t_storage_destroy(t_storage *storage) {
    return storage_destroy_impl(storage, 0);
}

int t_storage_put(t_storage *storage, uint64_t key, const void *data, size_t len) {
    if (!storage || storage->iterating || storage->free_pending || (len > 0 && !data))
        return -1;
    if (len > T_STORAGE_MAX_VALUE) return -1;
    char keybuf[32]; key_to_str(key, keybuf, sizeof(keybuf));
    t_storage_entry *entry = (t_storage_entry*)malloc(sizeof(t_storage_entry));
    if (!entry) return -1;
    entry->data = NULL;
    entry->data_len = 0;
    entry->key = key;
    if (len > 0) {
        entry->data = (uint8_t*)malloc(len);
        if (!entry->data) { free(entry); return -1; }
        memcpy(entry->data, data, len);
        entry->data_len = len;
    }

    /* Preserve old value until insert succeeds (avoid data loss on OOM). */
    t_storage_entry *old = (t_storage_entry*)t_map_get(&storage->map, keybuf);
    /* Reject put that aliases the live buffer from t_storage_get (would UAF on free). */
    if (old && data && old->data == data) {
        free(entry->data);
        free(entry);
        return -1;
    }
    if (t_map_insert(&storage->map, keybuf, entry) != 0) {
        free(entry->data);
        free(entry);
        return -1;
    }
    if (old) {
        free(old->data);
        free(old);
    }
    if (storage->type == T_STORAGE_FILE) storage->dirty = 1;
    return 0;
}

int t_storage_get(t_storage *storage, uint64_t key, const void **data, size_t *len) {
    if (!storage || storage->free_pending || !data || !len) return -1;
    char keybuf[32]; key_to_str(key, keybuf, sizeof(keybuf));
    t_storage_entry *entry = (t_storage_entry*)t_map_get(&storage->map, keybuf);
    if (!entry) return -1;
    *data = entry->data;
    *len  = entry->data_len;
    return 0;
}

int t_storage_delete(t_storage *storage, uint64_t key) {
    if (!storage || storage->iterating || storage->free_pending) return -1;
    char keybuf[32]; key_to_str(key, keybuf, sizeof(keybuf));
    t_storage_entry *entry = (t_storage_entry*)t_map_remove(&storage->map, keybuf);
    if (!entry) return -1;
    free(entry->data);
    free(entry);
    if (storage->type == T_STORAGE_FILE) storage->dirty = 1;
    return 0;
}

int t_storage_contains(t_storage *storage, uint64_t key) {
    if (!storage) return 0;
    char keybuf[32]; key_to_str(key, keybuf, sizeof(keybuf));
    return t_map_contains(&storage->map, keybuf);
}

size_t t_storage_count(const t_storage *storage) {
    if (!storage) return 0;
    return t_map_len(&storage->map);
}

int t_storage_foreach(t_storage *storage, t_storage_iter_fn fn, void *ud) {
    if (!storage || !fn) return -1;
    if (storage->iterating || storage->free_pending) return -1;
    size_t n = t_map_len(&storage->map);
    if (n == 0) return 0;
    if (n > SIZE_MAX / sizeof(uint64_t)) return -1;
    uint64_t *keys = (uint64_t *)malloc(n * sizeof(uint64_t));
    if (!keys) return -1;
    size_t count = 0;
    t_map_iter it = t_map_iter_begin(&storage->map);
    const char *k;
    void *v;
    while (count < n && t_map_iter_next(&it, &k, &v)) {
        keys[count++] = str_to_key(k);
    }
    storage->iterating = 1;
    for (size_t i = 0; i < count && !storage->free_pending; ++i) {
        char keybuf[32];
        key_to_str(keys[i], keybuf, sizeof(keybuf));
        t_storage_entry *e = (t_storage_entry *)t_map_get(&storage->map, keybuf);
        if (e) fn(keys[i], e->data, e->data_len, ud);
    }
    free(keys);
    storage->iterating = 0;
    if (storage->free_pending) {
        (void)storage_destroy_impl(storage, 1);
        return -1; /* storage freed; caller must not touch it */
    }
    return 0;
}

int t_storage_flush(t_storage *storage) {
    if (!storage || storage->iterating || storage->free_pending) return -1;
    if (storage->type != T_STORAGE_FILE) return 0;
    if (!storage->path) return 0;
    /* Write to a temp file then rename for crash-safe replace. */
    size_t plen = strlen(storage->path);
    if (plen > SIZE_MAX - 5) return -1;
    char *tmp = (char *)malloc(plen + 5);
    if (!tmp) return -1;
    memcpy(tmp, storage->path, plen);
    memcpy(tmp + plen, ".tmp", 5);
    t_file f;
    t_file_init(&f);
    if (t_file_open(&f, tmp, T_FILE_WRITE | T_FILE_CREAT | T_FILE_TRUNC) != 0) {
        free(tmp);
        return -1;
    }
    t_map_iter it = t_map_iter_begin(&storage->map);
    const char *k;
    void *v;
    uint64_t wrote = 0;
    while (t_map_iter_next(&it, &k, &v)) {
        t_storage_entry *e = (t_storage_entry*)v;
        if (!e) continue;
        uint64_t key = str_to_key(k);
        uint64_t len64 = (uint64_t)e->data_len;
        if (wrote > T_STORAGE_MAX_FILE ||
            (uint64_t)(16 + e->data_len) > T_STORAGE_MAX_FILE - wrote) {
            t_file_close(&f); (void)t_file_unlink(tmp); free(tmp); return -1;
        }
        uint8_t header[16];
        write_le64(header, key);
        write_le64(header + 8, len64);
        if (t_file_write(&f, header, 16) != 0) {
            t_file_close(&f); (void)t_file_unlink(tmp); free(tmp); return -1;
        }
        if (e->data_len > 0) {
            if (t_file_write(&f, e->data, e->data_len) != 0) {
                t_file_close(&f); (void)t_file_unlink(tmp); free(tmp); return -1;
            }
        }
        wrote += 16 + (uint64_t)e->data_len;
    }
    if (t_file_sync(&f) != 0) { t_file_close(&f); (void)t_file_unlink(tmp); free(tmp); return -1; }
    t_file_close(&f);
    if (t_file_rename(tmp, storage->path) != 0) { (void)t_file_unlink(tmp); free(tmp); return -1; }
    free(tmp);
    storage->dirty = 0;
    return 0;
}

void t_storage_destroy_no_free(t_storage *storage) {
    /* helper if needed in future */ (void)storage;
}
