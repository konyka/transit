#ifndef T_POOL_H
#define T_POOL_H

#include "t_compiler.h"
#include <stdint.h>
#include <stddef.h>

#define T_POOL_SIZE_CLASSES 16
#define T_POOL_MIN_ALIGN    16
#define T_POOL_MAX_CLASS    8192

typedef struct t_pool t_pool;

t_pool *t_pool_create(size_t chunk_size);
void    t_pool_destroy(t_pool *pool);
void   *t_pool_alloc(t_pool *pool, size_t size);
void   *t_pool_alloc_zero(t_pool *pool, size_t size);
void    t_pool_free(t_pool *pool, void *ptr, size_t size);
void   *t_pool_alloc_array(t_pool *pool, size_t elem_size, size_t count);
size_t  t_pool_used_bytes(t_pool *pool);
size_t  t_pool_total_bytes(t_pool *pool);
size_t  t_pool_chunk_count(t_pool *pool);
int     t_pool_size_class(size_t size);

#endif /* T_POOL_H */
