#ifndef T_SLAB_H
#define T_SLAB_H

#include "t_compiler.h"
#include <stdint.h>
#include <stddef.h>

typedef struct t_slab t_slab;

t_slab *t_slab_create(size_t object_size, size_t alignment);
void    t_slab_destroy(t_slab *slab);
void   *t_slab_alloc(t_slab *slab);
void   *t_slab_alloc_zero(t_slab *slab);
void    t_slab_free(t_slab *slab, void *obj);
size_t  t_slab_object_size(t_slab *slab);
size_t  t_slab_used_count(t_slab *slab);
size_t  t_slab_total_count(t_slab *slab);
size_t  t_slab_slab_count(t_slab *slab);

#endif /* T_SLAB_H */
