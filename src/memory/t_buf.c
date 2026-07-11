#include "t_buf.h"
#include <stdlib.h>
#include <string.h>

/* Create a refcounted buffer with given capacity */
// Create a refcounted buffer with given capacity
t_rcbuf *t_rcbuf_create(size_t capacity) {
    // Allocate header + data block in one chunk for locality
    t_rcbuf *rcb = (t_rcbuf *)malloc(sizeof(t_rcbuf) + capacity);
    if (!rcb) return NULL;
    rcb->base = (unsigned char *)(rcb + 1);
    rcb->cap  = capacity;
    rcb->buf.data = rcb->base;
    rcb->buf.len  = 0;
    rcb->buf.cap  = capacity;
    // Simple non-atomic initialization for test purposes
    rcb->refcount = 1;
    rcb->owns_base = 0; /* memory is part of this allocation, freed with rcbuf */
    return rcb;
}

/* Create a refcounted buffer that wraps existing data (takes ownership) */
t_rcbuf *t_rcbuf_wrap(void *data, size_t len, size_t cap) {
    (void)cap; // cap is informational when wrapping existing data
    t_rcbuf *rcb = (t_rcbuf *)malloc(sizeof(t_rcbuf));
    if (!rcb) return NULL;
    rcb->base = (unsigned char *)data;
    rcb->cap  = cap ? cap : len;
    rcb->buf.data = rcb->base;
    rcb->buf.len  = len;
    rcb->buf.cap  = rcb->cap;
    rcb->refcount = 1;
    rcb->owns_base = 1; /* we own the provided base memory in wrap */
    return rcb;
}

/* Increment refcount, return the same pointer */
t_rcbuf *t_rcbuf_ref(t_rcbuf *rcb) {
    if (rcb) {
        rcb->refcount++;
    }
    return rcb;
}

/* Decrement refcount, free if reaches 0 */
void t_rcbuf_unref(t_rcbuf *rcb) {
    if (!rcb) return;
    rcb->refcount--;
    if (rcb->refcount == 0) {
        // Free owned base buffer if present
        if (rcb->base && rcb->owns_base) free(rcb->base);
        free(rcb);
    }
}

/* Get refcount */
int t_rcbuf_refcount(const t_rcbuf *rcb) {
    if (!rcb) return 0;
    return rcb->refcount;
}

/* Simple buffer operations */
t_buf t_buf_from(const void *data, size_t len) {
    t_buf b;
    b.data = (unsigned char *)data;
    b.len  = len;
    b.cap  = len;
    return b;
}

t_buf t_buf_empty(void) {
    t_buf b;
    b.data = NULL;
    b.len  = 0;
    b.cap  = 0;
    return b;
}

size_t t_buf_copy(t_buf *dst, const t_buf *src) {
    if (!dst || !src) return 0;
    size_t n = dst->cap < src->len ? dst->cap : src->len;
    if (n > 0 && dst->data && src->data) {
        memcpy(dst->data, src->data, n);
    }
    dst->len = n;
    return n;
}

int t_buf_eq(const t_buf *a, const t_buf *b) {
    if (!a || !b) return 0;
    if (a->len != b->len) return 0;
    if (a->len == 0) return 1;
    return memcmp(a->data, b->data, a->len) == 0;
}
