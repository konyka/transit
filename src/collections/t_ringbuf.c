#include "t_ringbuf.h"
#include <stdlib.h>
#include <string.h>
#include <stdint.h>

/* Internal helpers */
static size_t next_power_of_two(size_t v) {
    if (v == 0) return 1;
    v -= 1;
    v |= v >> 1;
    v |= v >> 2;
    v |= v >> 4;
    v |= v >> 8;
    v |= v >> 16;
#if SIZE_MAX > 0xFFFFFFFFULL
    v |= v >> 32;
#endif
    return v + 1;
}

int t_ringbuf_init(t_ringbuf *rb, size_t capacity) {
    if (!rb) return -1;
    size_t cap = next_power_of_two(capacity);
    rb->buf = (unsigned char*)malloc(cap);
    if (!rb->buf) return -1;
    rb->cap = cap;
    rb->mask = cap - 1;
    rb->head = 0;
    rb->tail = 0;
    return 0;
}

void t_ringbuf_destroy(t_ringbuf *rb) {
    if (!rb) return;
    free(rb->buf);
    rb->buf = NULL;
    rb->cap = 0;
    rb->mask = 0;
    t_atomic_store_int(&rb->head, 0);
    t_atomic_store_int(&rb->tail, 0);
}

size_t t_ringbuf_write(t_ringbuf *rb, const void *data, size_t len) {
    if (!rb || !data || len == 0) return 0;
    size_t written = 0;
    const unsigned char *src = (const unsigned char*)data;
    while (written < len) {
        /* Unsigned wrap avoids signed overflow on long-lived counters. */
        uint32_t head = (uint32_t)t_atomic_load_int(&rb->head);
        uint32_t tail = (uint32_t)t_atomic_load_int(&rb->tail);
        uint32_t used = head - tail;
        if ((size_t)used >= rb->cap) break; /* full */
        size_t available = rb->cap - (size_t)used;
        size_t to_copy = (len - written) < available ? (len - written) : available;
        size_t idx = (size_t)(head & (uint32_t)rb->mask);
        size_t first = rb->cap - idx;
        if (first > to_copy) first = to_copy;
        memcpy(rb->buf + idx, src + written, first);
        if (to_copy > first) {
            memcpy(rb->buf, src + written + first, to_copy - first);
        }
        written += to_copy;
        t_atomic_store_int(&rb->head, (int)(head + (uint32_t)to_copy));
    }
    return written;
}

size_t t_ringbuf_read(t_ringbuf *rb, void *data, size_t len) {
    if (!rb || !data || len == 0) return 0;
    size_t read = 0;
    unsigned char *dst = (unsigned char*)data;
    uint32_t head = (uint32_t)t_atomic_load_int(&rb->head);
    uint32_t tail = (uint32_t)t_atomic_load_int(&rb->tail);
    uint32_t available = head - tail;
    size_t to_read = (len < (size_t)available) ? len : (size_t)available;
    while (read < to_read) {
        size_t idx = (size_t)(tail & (uint32_t)rb->mask);
        size_t first = rb->cap - idx;
        size_t want = to_read - read;
        if (first > want) first = want;
        memcpy(dst + read, rb->buf + idx, first);
        if (want > first) {
            memcpy(dst + read + first, rb->buf, want - first);
        }
        read += want;
        t_atomic_store_int(&rb->tail, (int)(tail + (uint32_t)to_read));
        break; /* single read loop suffices since we computed to_read above */
    }
    return read;
}

size_t t_ringbuf_available(const t_ringbuf *rb) {
    if (!rb) return 0;
    uint32_t head = (uint32_t)t_atomic_load_int((t_atomic_int*)&rb->head);
    uint32_t tail = (uint32_t)t_atomic_load_int((t_atomic_int*)&rb->tail);
    uint32_t used = head - tail;
    if ((size_t)used >= rb->cap) return 0;
    return rb->cap - (size_t)used;
}

size_t t_ringbuf_used(const t_ringbuf *rb) {
    if (!rb) return 0;
    uint32_t head = (uint32_t)t_atomic_load_int((t_atomic_int*)&rb->head);
    uint32_t tail = (uint32_t)t_atomic_load_int((t_atomic_int*)&rb->tail);
    uint32_t used = head - tail;
    return ((size_t)used > rb->cap) ? rb->cap : (size_t)used;
}

void t_ringbuf_reset(t_ringbuf *rb) {
    if (!rb) return;
    t_atomic_store_int(&rb->head, 0);
    t_atomic_store_int(&rb->tail, 0);
}
