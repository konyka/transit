/* Minimal high-performance tiered memory pool implementation
 * for Transit project. This is a simplified, single-threaded
 * variant intended for unit tests in this kata.
 */

#include "t_pool.h"
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
/* no extra system headers required beyond stdlib and string */

typedef struct t_pool_chunk {
    void *mem;
    size_t chunk_size;
    struct t_pool_chunk *next;
} t_pool_chunk;

typedef struct t_pool_class {
    size_t block_size;      /* size per block in this class */
    void *free_head;          /* head of free list for this class */
    t_pool_chunk *chunks;     /* list of chunks backing this class */
    size_t used_bytes;          /* total used bytes in this class */
} t_pool_class;

struct t_pool {
    size_t chunk_size;           /* bytes per chunk when allocating new blocks */
    t_pool_class classes[T_POOL_SIZE_CLASSES];
};

/* Fixed, ascending size class table (power-of-two-ish with extras) */
static const size_t g_pool_sizes[T_POOL_SIZE_CLASSES] = {
    16, 32, 48, 64, 96, 128, 192, 256, 384, 512, 768, 1024, 1536, 2048, 4096, 8192
};

static int t_pool_class_index_for_size(size_t size) {
    for (int i = 0; i < T_POOL_SIZE_CLASSES; ++i) {
        if (size <= g_pool_sizes[i]) {
            return i;
        }
    }
    return -1; /* too large for pool */
}

/* Resolve class from pointer ownership so a wrong size cannot corrupt freelists. */
static int t_pool_class_index_for_ptr(t_pool *pool, void *ptr) {
    uintptr_t p = (uintptr_t)ptr;
    for (int i = 0; i < T_POOL_SIZE_CLASSES; ++i) {
        t_pool_class *cls = &pool->classes[i];
        size_t block = cls->block_size;
        if (block == 0) continue;
        for (t_pool_chunk *c = cls->chunks; c; c = c->next) {
            if (!c->mem || c->chunk_size < block) continue;
            uintptr_t base = (uintptr_t)c->mem;
            uintptr_t end = base + c->chunk_size;
            if (p < base || p >= end) continue;
            if ((p - base) % block != 0) continue;
            return i;
        }
    }
    return -1;
}



/* Create a new memory pool. If chunk_size == 0, use 64KB. */
t_pool *t_pool_create(size_t chunk_size) {
    (void)chunk_size; /* chunk_size is important for behavior, store in pool */
    t_pool *pool = (t_pool *)calloc(1, sizeof(t_pool));
    if (!pool) return NULL;
    pool->chunk_size = (chunk_size == 0 ? 65536 : chunk_size);
    /* Initialize size classes */
    for (int i = 0; i < T_POOL_SIZE_CLASSES; ++i) {
        pool->classes[i].block_size = g_pool_sizes[i];
        pool->classes[i].free_head = NULL;
        pool->classes[i].chunks = NULL;
        pool->classes[i].used_bytes = 0;
    }
    return pool;
}

void t_pool_destroy(t_pool *pool) {
    if (!pool) return;
    /* Free all chunks for all classes */
    for (int i = 0; i < T_POOL_SIZE_CLASSES; ++i) {
        t_pool_class *cls = &pool->classes[i];
        t_pool_chunk *c = cls->chunks;
        while (c) {
            t_pool_chunk *next = c->next;
            if (c->mem) {
                free(c->mem);
            }
            free(c);
            c = next;
        }
        cls->chunks = NULL;
        cls->free_head = NULL;
        cls->used_bytes = 0;
    }
    free(pool);
}

static void t_pool_allocate_chunk_for_class(t_pool_class *cls, size_t pool_chunk_size) {
    /* Determine number of blocks we can fit */
    size_t block = cls->block_size;
    size_t chunk_bytes = pool_chunk_size;
    if (chunk_bytes < block) chunk_bytes = block;
    size_t blocks_per_chunk = chunk_bytes / block;
    if (blocks_per_chunk == 0) blocks_per_chunk = 1;
    chunk_bytes = blocks_per_chunk * block;

    void *mem = NULL;
    /* Fail closed: malloc may break T_POOL_MIN_ALIGN (16). */
    if (posix_memalign(&mem, T_POOL_MIN_ALIGN, chunk_bytes) != 0 || !mem)
        return;

    /* Create chunk record */
    t_pool_chunk *chunk = (t_pool_chunk *)malloc(sizeof(t_pool_chunk));
    if (!chunk) {
        free(mem);
        return;
    }
    chunk->mem = mem;
    chunk->chunk_size = chunk_bytes;
    chunk->next = cls->chunks;
    cls->chunks = chunk;

    /* Initialize free list within this chunk */
    unsigned char *base = (unsigned char *)mem;
    for (size_t i = 0; i < blocks_per_chunk; ++i) {
        void *blk = (void *)(base + i * block);
        void *next = cls->free_head;
        /* Store next pointer inside the block's first word */
        *((void **)blk) = next;
        cls->free_head = blk;
    }
}

void *t_pool_alloc(t_pool *pool, size_t size) {
    if (!pool) return NULL;
    int idx = t_pool_class_index_for_size(size);
    if (idx < 0) {
        /* Large allocation: require pool alignment; no unaligned malloc. */
        void *ptr = NULL;
        if (posix_memalign(&ptr, T_POOL_MIN_ALIGN, size) != 0)
            return NULL;
        return ptr;
    }
    t_pool_class *cls = &pool->classes[idx];
    if (cls->free_head == NULL) {
        t_pool_allocate_chunk_for_class(cls, pool->chunk_size);
        if (cls->free_head == NULL) {
            return NULL; /* allocation failed */
        }
    }
    void *blk = cls->free_head;
    cls->free_head = *((void **)blk);
    cls->used_bytes += cls->block_size;
    return blk;
}

void *t_pool_alloc_zero(t_pool *pool, size_t size) {
    void *p = t_pool_alloc(pool, size);
    if (!p) return NULL;
    int idx = t_pool_class_index_for_size(size);
    size_t clear = (idx < 0) ? size : g_pool_sizes[idx];
    memset(p, 0, clear);
    return p;
}

void t_pool_free(t_pool *pool, void *ptr, size_t size) {
    if (!pool || !ptr) return;
    int idx = t_pool_class_index_for_ptr(pool, ptr);
    if (idx < 0) {
        /* Large allocation (or unknown) – free directly.
         * Ignore caller size so a mismatched size cannot poison a freelist. */
        (void)size;
        free(ptr);
        return;
    }
    t_pool_class *cls = &pool->classes[idx];
    /* Reject double-free: already on the freelist (would self-alias). */
    for (void *p = cls->free_head; p; p = *((void **)p)) {
        if (p == ptr) return;
    }
    *((void **)ptr) = cls->free_head;
    cls->free_head = ptr;
    if (cls->used_bytes >= cls->block_size) {
        cls->used_bytes -= cls->block_size;
    } else {
        cls->used_bytes = 0;
    }
}

void *t_pool_alloc_array(t_pool *pool, size_t elem_size, size_t count) {
    if (!pool || elem_size == 0 || count == 0) return NULL;
    if (count > SIZE_MAX / elem_size) return NULL;
    size_t total = elem_size * count;
    return t_pool_alloc(pool, total);
}

size_t t_pool_used_bytes(t_pool *pool) {
    if (!pool) return 0;
    size_t sum = 0;
    for (int i = 0; i < T_POOL_SIZE_CLASSES; ++i) {
        sum += pool->classes[i].used_bytes;
    }
    return sum;
}

size_t t_pool_total_bytes(t_pool *pool) {
    if (!pool) return 0;
    size_t total = 0;
    for (int i = 0; i < T_POOL_SIZE_CLASSES; ++i) {
        total += pool->classes[i].block_size * (size_t)0; /* unused: compute via chunks if needed */
    }
    /* Practical approximation: sum chunk sizes across all classes */
    for (int i = 0; i < T_POOL_SIZE_CLASSES; ++i) {
        t_pool_class *cls = &pool->classes[i];
        t_pool_chunk *c = cls->chunks;
        while (c) {
            total += c->chunk_size; /* this counts all chunks once */
            c = c->next;
        }
    }
    /* Also count any large allocations? We don't track them here. */
    return total;
}

size_t t_pool_chunk_count(t_pool *pool) {
    if (!pool) return 0;
    size_t cnt = 0;
    for (int i = 0; i < T_POOL_SIZE_CLASSES; ++i) {
        t_pool_chunk *c = pool->classes[i].chunks;
        while (c) {
            cnt++;
            c = c->next;
        }
    }
    return cnt;
}

int t_pool_size_class(size_t size) {
    return t_pool_class_index_for_size(size);
}
