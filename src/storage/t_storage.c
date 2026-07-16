#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <stdio.h>
#include <fcntl.h>
#include <unistd.h>
#include <errno.h>
#include <sys/stat.h>

#include "t_storage.h"
#include "t_map.h"

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

/* Load file format: [key(8 bytes little-endian)] [len(8 bytes little-endian)] [data(len)] ...
   This is a simple binary dump to support basic persistence. */
static int t_storage_fs_load(t_storage *st, const char *path) {
    if (!st || !path) return -1;
    int fd = open(path, O_RDONLY);
    if (fd < 0) {
        /* Missing file = empty store (created on first flush/destroy). */
        return (errno == ENOENT) ? 0 : -1;
    }
    struct stat stf;
    if (fstat(fd, &stf) != 0) { close(fd); return -1; }
    size_t total = (size_t)stf.st_size;
    if (total == 0) { close(fd); return 0; }
    if (total > T_STORAGE_MAX_FILE) { close(fd); return -1; }
    uint8_t *buf = (uint8_t*)malloc(total);
    if (!buf) { close(fd); return -1; }
    size_t off = 0;
    while (off < total) {
        ssize_t r = read(fd, buf + off, total - off);
        if (r < 0) {
            if (errno == EINTR) continue;
            close(fd); free(buf); return -1;
        }
        if (r == 0) { close(fd); free(buf); return -1; }
        off += (size_t)r;
    }
    close(fd);
    size_t pos = 0;
    char keybuf[32];
    while (pos + 16 <= total) {
        uint64_t key;
        memcpy(&key, buf + pos, 8); pos += 8;
        uint64_t len;
        memcpy(&len, buf + pos, 8); pos += 8;
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

void t_storage_destroy(t_storage *storage) {
    if (!storage) return;
    if (storage->iterating) {
        storage->free_pending = 1;
        return;
    }
    storage->free_pending = 0;
    /* Persist dirty file-backed state before tearing down the map. */
    if (storage->type == T_STORAGE_FILE && storage->dirty) {
        (void)t_storage_flush(storage);
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
}

int t_storage_put(t_storage *storage, uint64_t key, const void *data, size_t len) {
    if (!storage || (len > 0 && !data)) return -1;
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
    if (!storage || !data || !len) return -1;
    char keybuf[32]; key_to_str(key, keybuf, sizeof(keybuf));
    t_storage_entry *entry = (t_storage_entry*)t_map_get(&storage->map, keybuf);
    if (!entry) return -1;
    *data = entry->data;
    *len  = entry->data_len;
    return 0;
}

int t_storage_delete(t_storage *storage, uint64_t key) {
    if (!storage) return -1;
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
        t_storage_destroy(storage);
        return 0;
    }
    return 0;
}

int t_storage_flush(t_storage *storage) {
    if (!storage) return -1;
    if (storage->type != T_STORAGE_FILE) return 0;
    if (!storage->path) return 0;
    /* Write to a temp file then rename for crash-safe replace. */
    size_t plen = strlen(storage->path);
    char *tmp = (char *)malloc(plen + 5);
    if (!tmp) return -1;
    memcpy(tmp, storage->path, plen);
    memcpy(tmp + plen, ".tmp", 5);
    int fd = open(tmp, O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (fd < 0) { free(tmp); return -1; }
    t_map_iter it = t_map_iter_begin(&storage->map);
    const char *k;
    void *v;
    off_t wrote = 0;
    while (t_map_iter_next(&it, &k, &v)) {
        t_storage_entry *e = (t_storage_entry*)v;
        if (!e) continue;
        uint64_t key = str_to_key(k);
        uint64_t len64 = (uint64_t)e->data_len;
        if ((uint64_t)wrote > T_STORAGE_MAX_FILE ||
            (uint64_t)(16 + e->data_len) > T_STORAGE_MAX_FILE - (uint64_t)wrote) {
            close(fd); unlink(tmp); free(tmp); return -1;
        }
        uint8_t header[16];
        memcpy(header, &key, 8);
        memcpy(header + 8, &len64, 8);
        size_t hoff = 0;
        while (hoff < 16) {
            ssize_t w = write(fd, header + hoff, 16 - hoff);
            if (w < 0) {
                if (errno == EINTR) continue;
                close(fd); unlink(tmp); free(tmp); return -1;
            }
            if (w == 0) { close(fd); unlink(tmp); free(tmp); return -1; }
            hoff += (size_t)w;
        }
        if (e->data_len > 0) {
            size_t doff = 0;
            while (doff < e->data_len) {
                ssize_t w = write(fd, e->data + doff, e->data_len - doff);
                if (w < 0) {
                    if (errno == EINTR) continue;
                    close(fd); unlink(tmp); free(tmp); return -1;
                }
                if (w == 0) { close(fd); unlink(tmp); free(tmp); return -1; }
                doff += (size_t)w;
            }
        }
        wrote += (16 + (off_t)e->data_len);
    }
    if (fsync(fd) != 0) { close(fd); unlink(tmp); free(tmp); return -1; }
    close(fd);
    if (rename(tmp, storage->path) != 0) { unlink(tmp); free(tmp); return -1; }
    free(tmp);
    storage->dirty = 0;
    return 0;
}

void t_storage_destroy_no_free(t_storage *storage) {
    /* helper if needed in future */ (void)storage;
}
