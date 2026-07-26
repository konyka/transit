#ifndef T_MAP_H
#define T_MAP_H

#include <stddef.h>
#include <stdint.h>

/* Hash map with open addressing and linear probing.
   Keys are null-terminated strings, values are void*. */

typedef struct t_map_entry {
    char  *key;
    void  *val;
    int    occupied; /* 0 = empty, 1 = occupied, 2 = tombstone */
} t_map_entry;

typedef struct t_map {
    t_map_entry *entries;
    size_t       cap;
    size_t       len;
    size_t       tombstones;
} t_map;

#define T_MAP_INIT { NULL, 0, 0, 0 }

void    t_map_init(t_map *m);
void    t_map_destroy(t_map *m);
int     t_map_insert(t_map *m, const char *key, void *val);
void   *t_map_get(const t_map *m, const char *key);
void   *t_map_remove(t_map *m, const char *key);
int     t_map_compact(t_map *m);
int     t_map_contains(const t_map *m, const char *key);
size_t  t_map_len(const t_map *m);
void    t_map_clear(t_map *m);

/* Iteration */
typedef struct t_map_iter {
    const t_map *map;
    size_t       index;
} t_map_iter;

t_map_iter t_map_iter_begin(const t_map *m);
int        t_map_iter_next(t_map_iter *it, const char **key, void **val);

#endif /* T_MAP_H */
