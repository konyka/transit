/* Simple per-thread slab allocator for fixed-size objects.
 * NOT thread-safe by design (as required in the task).
 */

#include "t_slab.h"
#include <stdlib.h>
#include <string.h>
#include <stddef.h>

typedef struct t_slab_page {
    void *mem;           /* raw memory for objects */
    size_t capacity;     /* number of objects this page can hold */
    size_t object_size;  /* size of each object */
    int free_head;         /* index of first free slot, -1 if full */
    struct t_slab_page *next;
} t_slab_page;

struct t_slab {
    size_t object_size;
    size_t alignment;
    t_slab_page *pages;
    size_t used; /* total allocated objects currently in use */
};

/* Create a new page for the slab */
static t_slab_page *t_slab_new_page(t_slab *slab) {
    if (!slab) return NULL;
    size_t obj = slab->object_size;
    /* Simple page size: 4KB chunks, full utilization */
    size_t page_bytes = 4096;
    size_t cap = (page_bytes) / obj;
    if (cap == 0) cap = 1;
    void *mem = NULL;
    if (posix_memalign(&mem, (slab->alignment ? slab->alignment : slab->object_size), cap * obj) != 0) {
        mem = NULL;
    }
    if (!mem) mem = malloc(cap * obj);
    if (!mem) return NULL;
    t_slab_page *page = (t_slab_page *)malloc(sizeof(t_slab_page));
    if (!page) {
        free(mem);
        return NULL;
    }
    page->mem = mem;
    page->capacity = cap;
    page->object_size = obj;
    page->free_head = 0;
    page->next = slab->pages;
    /* Initialize free list within page: each slot stores next index */
    for (size_t i = 0; i < cap; ++i) {
        int next = (int)(i + 1);
        if (i + 1 >= cap) next = -1;
        *((int *)((char *)mem + i * obj)) = next;
    }
    return page;
}

t_slab *t_slab_create(size_t object_size, size_t alignment) {
    if (object_size == 0) return NULL;
    /* Free-list stores an int index in each free slot. */
    if (object_size < sizeof(int)) object_size = sizeof(int);
    t_slab *slab = (t_slab *)calloc(1, sizeof(t_slab));
    if (!slab) return NULL;
    slab->object_size = object_size;
    slab->alignment = (alignment == 0) ? object_size : alignment;
    slab->pages = NULL;
    slab->used = 0;
    return slab;
}

void t_slab_destroy(t_slab *slab) {
    if (!slab) return;
    t_slab_page *p = slab->pages;
    while (p) {
        t_slab_page *next = p->next;
        if (p->mem) free(p->mem);
        free(p);
        p = next;
    }
    free(slab);
}

void *t_slab_alloc(t_slab *slab) {
    if (!slab) return NULL;
    t_slab_page *page = slab->pages;
    if (!page) {
        page = t_slab_new_page(slab);
        if (!page) return NULL;
        slab->pages = page;
    }
    if (page->free_head == -1) {
        /* page is full, create a new one */
        t_slab_page *np = t_slab_new_page(slab);
        if (!np) return NULL;
        np->next = slab->pages;
        slab->pages = np;
        page = np;
    }
    int idx = page->free_head;
    void *obj = (char *)page->mem + idx * page->object_size;
    page->free_head = *((int *)obj);
    slab->used++;
    return obj;
}

void *t_slab_alloc_zero(t_slab *slab) {
    void *p = t_slab_alloc(slab);
    if (p) memset(p, 0, slab->object_size);
    return p;
}

static t_slab_page *t_slab_find_page_for_ptr(t_slab *slab, void *ptr) {
    for (t_slab_page *p = slab->pages; p; p = p->next) {
        void *start = p->mem;
        void *end = (char *)p->mem + p->capacity * p->object_size;
        if (ptr >= start && ptr < end) return p;
    }
    return NULL;
}

void t_slab_free(t_slab *slab, void *obj) {
    if (!slab || !obj) return;
    t_slab_page *page = t_slab_find_page_for_ptr(slab, obj);
    if (!page) return; /* invalid pointer */
    ptrdiff_t off = (char *)obj - (char *)page->mem;
    if (off < 0 || (size_t)off % page->object_size != 0) return;
    int idx = (int)((size_t)off / page->object_size);
    if (idx < 0 || (size_t)idx >= page->capacity) return;
    /* Reject double-free: object already on the free list. */
    for (int i = page->free_head; i >= 0; ) {
        if (i == idx) return;
        void *slot = (char *)page->mem + (size_t)i * page->object_size;
        i = *((int *)slot);
    }
    *((int *)obj) = page->free_head;
    page->free_head = idx;
    if (slab->used > 0) slab->used--;
}

size_t t_slab_object_size(t_slab *slab) {
    return slab ? slab->object_size : 0;
}

size_t t_slab_used_count(t_slab *slab) {
    return slab ? slab->used : 0;
}

size_t t_slab_total_count(t_slab *slab) {
    if (!slab) return 0;
    size_t total = 0;
    for (t_slab_page *p = slab->pages; p; p = p->next) {
        total += p->capacity;
    }
    return total;
}

size_t t_slab_slab_count(t_slab *slab) {
    if (!slab) return 0;
    size_t cnt = 0;
    for (t_slab_page *p = slab->pages; p; p = p->next) cnt++;
    return cnt;
}
