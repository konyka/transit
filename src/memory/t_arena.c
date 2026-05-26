#include "t_arena.h"
#include <stdlib.h>
#include <string.h>

typedef struct t_arena_block {
    void *data;
    size_t size;
    size_t used;
    struct t_arena_block *next;
} t_arena_block;

struct t_arena {
    size_t block_size;
    t_arena_block *head;
    t_arena_block *current;
};

static t_arena_block *arena_block_create(size_t size) {
    t_arena_block *b = (t_arena_block *)malloc(sizeof(t_arena_block));
    if (!b) return NULL;
    b->data = malloc(size);
    if (!b->data) {
        free(b);
        return NULL;
    }
    b->size = size;
    b->used = 0;
    b->next = NULL;
    return b;
}

t_arena *t_arena_create(size_t block_size) {
    if (block_size == 0) block_size = 4096; /* default */
    t_arena *arena = (t_arena *)malloc(sizeof(t_arena));
    if (!arena) return NULL;
    arena->block_size = block_size;
    arena->head = arena_block_create(block_size);
    if (!arena->head) {
        free(arena);
        return NULL;
    }
    arena->current = arena->head;
    return arena;
}

void t_arena_destroy(t_arena *arena) {
    if (!arena) return;
    t_arena_block *b = arena->head;
    while (b) {
        t_arena_block *n = b->next;
        free(b->data);
        free(b);
        b = n;
    }
    free(arena);
}

void *t_arena_alloc(t_arena *arena, size_t size) {
    if (!arena || size == 0) return NULL;
    t_arena_block *cur = arena->current;
    size_t avail = cur->size - cur->used;
    if (size <= avail) {
        void *ptr = (void *)((unsigned char *)cur->data + cur->used);
        cur->used += size;
        return ptr;
    }
    /* Not enough space, allocate a new block */
    size_t new_size = arena->block_size;
    if (size > new_size) new_size = size;
    t_arena_block *nb = arena_block_create(new_size);
    if (!nb) return NULL;
    cur->next = nb;
    arena->current = nb;
    nb->used = size;
    return nb->data;
}

void *t_arena_alloc_aligned(t_arena *arena, size_t size, size_t alignment) {
    if (!arena) return NULL;
    if (alignment <= 1) return t_arena_alloc(arena, size);
    t_arena_block *cur = arena->current;
    size_t base_addr = (size_t)cur->data + cur->used;
    size_t mis = base_addr % alignment;
    size_t pad = mis ? (alignment - mis) : 0;
    if (size + pad <= cur->size - cur->used) {
        unsigned char *start = (unsigned char *)cur->data + cur->used + pad;
        cur->used += pad + size;
        return (void *)start;
    }
    /* allocate new block and align there */
    size_t new_size = arena->block_size;
    if (size + alignment > new_size) new_size = size + alignment;
    t_arena_block *nb = arena_block_create(new_size);
    if (!nb) return NULL;
    cur->next = nb;
    arena->current = nb;
    size_t mis2 = (size_t)nb->data % alignment;
    size_t pad2 = mis2 ? (alignment - mis2) : 0;
    nb->used = pad2 + size;
    return (void *)((unsigned char *)nb->data + pad2);
}

void *t_arena_alloc_zero(t_arena *arena, size_t size) {
    void *p = t_arena_alloc(arena, size);
    if (p && size > 0) memset(p, 0, size);
    return p;
}

char *t_arena_strdup(t_arena *arena, const char *str) {
    if (!str) return NULL;
    size_t len = strlen(str) + 1; /* include terminator */
    char *dst = (char *)t_arena_alloc(arena, len);
    if (!dst) return NULL;
    memcpy(dst, str, len);
    return dst;
}

void *t_arena_memdup(t_arena *arena, const void *src, size_t len) {
    void *dst = t_arena_alloc(arena, len);
    if (!dst) return NULL;
    memcpy(dst, src, len);
    return dst;
}

void t_arena_reset(t_arena *arena) {
    if (!arena) return;
    t_arena_block *b = arena->head;
    while (b) {
        b->used = 0;
        b = b->next;
    }
    arena->current = arena->head;
}

size_t t_arena_used(t_arena *arena) {
    if (!arena) return 0;
    size_t total = 0;
    t_arena_block *b = arena->head;
    while (b) {
        total += b->used;
        b = b->next;
    }
    return total;
}

size_t t_arena_capacity(t_arena *arena) {
    if (!arena) return 0;
    size_t cap = 0;
    t_arena_block *b = arena->head;
    while (b) {
        cap += b->size;
        b = b->next;
    }
    return cap;
}

size_t t_arena_block_count(t_arena *arena) {
    if (!arena) return 0;
    size_t n = 0;
    t_arena_block *b = arena->head;
    while (b) {
        n++;
        b = b->next;
    }
    return n;
}
