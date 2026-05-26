#ifndef T_STORAGE_H
#define T_STORAGE_H

#include "t_compiler.h"
#include <stdint.h>
#include <stddef.h>

typedef enum t_storage_type {
    T_STORAGE_MEM,
    T_STORAGE_FILE
} t_storage_type;

typedef struct t_storage_entry {
    uint8_t *data;
    size_t   data_len;
    uint64_t key;
} t_storage_entry;

typedef struct t_storage t_storage;

typedef void (*t_storage_iter_fn)(uint64_t key, const uint8_t *data, size_t len, void *ud);

t_storage *t_storage_create(t_storage_type type, const char *path);
void       t_storage_destroy(t_storage *storage);
int        t_storage_put(t_storage *storage, uint64_t key, const void *data, size_t len);
int        t_storage_get(t_storage *storage, uint64_t key, void **data, size_t *len);
int        t_storage_delete(t_storage *storage, uint64_t key);
int        t_storage_contains(t_storage *storage, uint64_t key);
size_t     t_storage_count(const t_storage *storage);
void       t_storage_foreach(t_storage *storage, t_storage_iter_fn fn, void *ud);
int        t_storage_flush(t_storage *storage);

#endif /* T_STORAGE_H */
