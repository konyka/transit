#ifndef T_VEC_H
#define T_VEC_H

#include <stddef.h>
#include <stdint.h>

/* Generic dynamic array. Stores void* elements.
   For type-safe wrappers, use the T_VEC_DECLARE macro below. */

typedef struct t_vec {
    void   **items;
    size_t   len;
    size_t   cap;
} t_vec;

#define T_VEC_INIT { NULL, 0, 0 }

void   t_vec_init(t_vec *v);
void   t_vec_destroy(t_vec *v);
size_t t_vec_len(const t_vec *v);
size_t t_vec_cap(const t_vec *v);
int    t_vec_push(t_vec *v, void *item);
void  *t_vec_pop(t_vec *v);
void  *t_vec_get(const t_vec *v, size_t index);
int    t_vec_set(t_vec *v, size_t index, void *item);
int    t_vec_insert(t_vec *v, size_t index, void *item);
void  *t_vec_remove(t_vec *v, size_t index);
void   t_vec_clear(t_vec *v);
int    t_vec_reserve(t_vec *v, size_t capacity);
int    t_vec_find(const t_vec *v, const void *item);

#endif /* T_VEC_H */
