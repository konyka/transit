#ifndef T_BUF_H
#define T_BUF_H

#include "t_compiler.h"
#include "t_atomic.h"
#include <stdint.h>
#include <stddef.h>

/* A single buffer segment */
typedef struct t_buf {
    unsigned char *data;      /* Pointer to data */
    size_t         len;       /* Length of data */
    size_t         cap;       /* Capacity */
} t_buf;

/* A buffer with reference counting for zero-copy */
typedef struct t_rcbuf {
    t_atomic_int  refcount;
    unsigned char *base;      /* Base allocation pointer (may be freed) */
    size_t         cap;       /* Total allocated capacity */
    t_buf          buf;       /* The usable buffer region */
    int            owns_base; /* 1 if this rcbuf should free base on destruction */
} t_rcbuf;

/* Create a refcounted buffer with given capacity */
t_rcbuf *t_rcbuf_create(size_t capacity);

/* Create a refcounted buffer that wraps existing data (takes ownership) */
t_rcbuf *t_rcbuf_wrap(void *data, size_t len, size_t cap);

/* Increment refcount, return the same pointer */
t_rcbuf *t_rcbuf_ref(t_rcbuf *rcb);

/* Decrement refcount, free if reaches 0 */
void t_rcbuf_unref(t_rcbuf *rcb);

/* Get refcount */
int t_rcbuf_refcount(const t_rcbuf *rcb);

/* Simple buffer operations */
t_buf  t_buf_from(const void *data, size_t len);
t_buf  t_buf_empty(void);
size_t t_buf_copy(t_buf *dst, const t_buf *src);
int    t_buf_eq(const t_buf *a, const t_buf *b);

#endif /* T_BUF_H */
