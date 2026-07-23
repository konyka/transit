#include "t_buf.h"
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <limits.h>

/* Create a refcounted buffer with given capacity */
// Create a refcounted buffer with given capacity
t_rcbuf *t_rcbuf_create(size_t capacity) {
    /* Allocate header + data block in one chunk for locality */
    if (capacity > SIZE_MAX - sizeof(t_rcbuf)) return NULL;
    t_rcbuf *rcb = (t_rcbuf *)malloc(sizeof(t_rcbuf) + capacity);
    if (!rcb) return NULL;
    rcb->base = (unsigned char *)(rcb + 1);
    rcb->cap  = capacity;
    rcb->buf.data = rcb->base;
    rcb->buf.len  = 0;
    rcb->buf.cap  = capacity;
    t_atomic_store_int(&rcb->refcount, 1);
    rcb->owns_base = 0; /* memory is part of this allocation, freed with rcbuf */
    return rcb;
}

/* Create a refcounted buffer that wraps existing data (takes ownership) */
t_rcbuf *t_rcbuf_wrap(void *data, size_t len, size_t cap) {
    if (len > 0 && !data) return NULL;
    if (cap && len > cap) return NULL;
    t_rcbuf *rcb = (t_rcbuf *)malloc(sizeof(t_rcbuf));
    if (!rcb) return NULL;
    rcb->base = (unsigned char *)data;
    rcb->cap  = cap ? cap : len;
    rcb->buf.data = rcb->base;
    rcb->buf.len  = len;
    rcb->buf.cap  = rcb->cap;
    t_atomic_store_int(&rcb->refcount, 1);
    rcb->owns_base = 1; /* we own the provided base memory in wrap */
    return rcb;
}

/* Increment refcount, return the same pointer */
t_rcbuf *t_rcbuf_ref(t_rcbuf *rcb) {
    if (!rcb) return NULL;
    for (;;) {
        int n = t_atomic_load_int(&rcb->refcount);
        if (n <= 0 || n == INT_MAX) return NULL;
        if (t_atomic_cas_int(&rcb->refcount, n, n + 1)) return rcb;
    }
}

/* Decrement refcount, free if reaches 0 */
void t_rcbuf_unref(t_rcbuf *rcb) {
    if (!rcb) return;
    int n = t_atomic_sub_fetch_int(&rcb->refcount, 1);
    if (n == 0) {
        if (rcb->base && rcb->owns_base) free(rcb->base);
        free(rcb);
    } else if (n < 0) {
        t_atomic_store_int(&rcb->refcount, 0); /* clamp over-unref */
    }
}

/* Get refcount */
int t_rcbuf_refcount(const t_rcbuf *rcb) {
    if (!rcb) return 0;
    return t_atomic_load_int((t_atomic_int *)&rcb->refcount);
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
    if (n > 0) {
        if (!dst->data || !src->data) return 0;
        memcpy(dst->data, src->data, n);
    }
    dst->len = n;
    return n;
}

int t_buf_eq(const t_buf *a, const t_buf *b) {
    if (!a || !b) return 0;
    if (a->len != b->len) return 0;
    if (a->len == 0) return 1;
    if (!a->data || !b->data) return 0;
    return memcmp(a->data, b->data, a->len) == 0;
}
