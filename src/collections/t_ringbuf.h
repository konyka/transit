#ifndef T_RINGBUF_H
#define T_RINGBUF_H

#include "t_compiler.h"
#include "t_atomic.h"
#include <stdint.h>
#include <stddef.h>

/* SPSC (Single-Producer Single-Consumer) lock-free ring buffer.
   Stores bytes. One thread writes, one thread reads, no locking needed. */

typedef struct t_ringbuf {
    unsigned char *buf;
    size_t         cap;
    size_t         mask;
    t_atomic_int   head; /* write position (producer) */
    t_atomic_int   tail; /* read position (consumer)  */
} t_ringbuf;

int     t_ringbuf_init(t_ringbuf *rb, size_t capacity);
void    t_ringbuf_destroy(t_ringbuf *rb);
size_t  t_ringbuf_write(t_ringbuf *rb, const void *data, size_t len);
size_t  t_ringbuf_read(t_ringbuf *rb, void *data, size_t len);
size_t  t_ringbuf_available(const t_ringbuf *rb);
size_t  t_ringbuf_used(const t_ringbuf *rb);
void    t_ringbuf_reset(t_ringbuf *rb);

#endif /* T_RINGBUF_H */
