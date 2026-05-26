#ifndef T_ARENA_H
#define T_ARENA_H

#include "t_compiler.h"
#include <stdint.h>
#include <stddef.h>

typedef struct t_arena t_arena;

/* Create an arena with the given block size (0 = default 4096) */
t_arena *t_arena_create(size_t block_size);

/* Destroy arena and free all memory */
void t_arena_destroy(t_arena *arena);

/* Allocate from arena. Memory is freed all at once on destroy. */
void   *t_arena_alloc(t_arena *arena, size_t size);

/* Allocate aligned memory from arena */
void   *t_arena_alloc_aligned(t_arena *arena, size_t size, size_t alignment);

/* Allocate and zero-fill */
void   *t_arena_alloc_zero(t_arena *arena, size_t size);

/* Duplicate a string into arena */
char   *t_arena_strdup(t_arena *arena, const char *str);

/* Duplicate memory into arena */
void   *t_arena_memdup(t_arena *arena, const void *src, size_t len);

/* Reset arena (free all allocations but keep the blocks for reuse) */
void    t_arena_reset(t_arena *arena);

/* Statistics */
size_t  t_arena_used(t_arena *arena);
size_t  t_arena_capacity(t_arena *arena);
size_t  t_arena_block_count(t_arena *arena);

#endif /* T_ARENA_H */
