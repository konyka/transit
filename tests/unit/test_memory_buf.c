#include "t_test.h"
#include "t_buf.h"
#include "t_arena.h"
#include <stdlib.h>
#include <string.h>

/* --- t_rcbuf tests --- */

T_TEST(rcbuf_create_unref) {
    t_rcbuf *rcb = t_rcbuf_create(64);
    T_ASSERT_NOT_NULL(rcb);
    T_ASSERT_EQ(t_rcbuf_refcount(rcb), 1);
    t_rcbuf_unref(rcb);
}

T_TEST(rcbuf_ref_unref) {
    t_rcbuf *rcb = t_rcbuf_create(64);
    T_ASSERT_EQ(t_rcbuf_refcount(rcb), 1);
    t_rcbuf *rcb2 = t_rcbuf_ref(rcb);
    T_ASSERT_EQ(t_rcbuf_refcount(rcb), 2);
    T_ASSERT(rcb2 == rcb);
    t_rcbuf_unref(rcb2);
    T_ASSERT_EQ(t_rcbuf_refcount(rcb), 1);
    t_rcbuf_unref(rcb);
}

T_TEST(rcbuf_write_data) {
    t_rcbuf *rcb = t_rcbuf_create(64);
    T_ASSERT(rcb != NULL);
    const char *msg = "hello world";
    size_t len = strlen(msg);
    memcpy(rcb->buf.data, msg, len);
    rcb->buf.len = len;
    T_ASSERT(memcmp(rcb->buf.data, "hello world", len) == 0);
    t_rcbuf_unref(rcb);
}

T_TEST(rcbuf_wrap) {
    char *data = (char *)malloc(32);
    memcpy(data, "test data", 9);
    t_rcbuf *rcb = t_rcbuf_wrap(data, 9, 32);
    T_ASSERT_NOT_NULL(rcb);
    T_ASSERT(rcb->buf.data == (unsigned char*)data);
    T_ASSERT_EQ((int)rcb->buf.len, 9);
    t_rcbuf_unref(rcb);
}

/* --- t_buf tests --- */

T_TEST(buf_from) {
    const char *s = "hello";
    t_buf b = t_buf_from(s, 5);
    T_ASSERT(b.data == (const unsigned char*)s);
    T_ASSERT_EQ((int)b.len, 5);
}

T_TEST(buf_empty) {
    t_buf b = t_buf_empty();
    T_ASSERT(b.data == NULL);
    T_ASSERT_EQ((int)b.len, 0);
}

T_TEST(buf_copy) {
    char src[] = "source";
    char dst[16] = {0};
    t_buf sb = t_buf_from(src, 6);
    t_buf db = t_buf_from(dst, 16);
    size_t n = t_buf_copy(&db, &sb);
    T_ASSERT_EQ((int)n, 6);
    T_ASSERT(memcmp(dst, "source", 6) == 0);
}

T_TEST(buf_copy_into_rcbuf) {
    t_rcbuf *rcb = t_rcbuf_create(16);
    T_ASSERT_NOT_NULL(rcb);
    t_buf sb = t_buf_from("hello", 5);
    size_t n = t_buf_copy(&rcb->buf, &sb);
    T_ASSERT_EQ((int)n, 5);
    T_ASSERT_EQ((int)rcb->buf.len, 5);
    T_ASSERT(memcmp(rcb->buf.data, "hello", 5) == 0);
    t_rcbuf_unref(rcb);
}

T_TEST(buf_eq) {
    t_buf a = t_buf_from("hello", 5);
    t_buf b = t_buf_from("hello", 5);
    t_buf c = t_buf_from("world", 5);
    T_ASSERT(t_buf_eq(&a, &b));
    T_ASSERT(!t_buf_eq(&a, &c));
}

/* --- t_arena tests --- */

T_TEST(arena_create_destroy) {
    t_arena *a = t_arena_create(0);
    T_ASSERT_NOT_NULL(a);
    t_arena_destroy(a);
}

T_TEST(arena_alloc_basic) {
    t_arena *a = t_arena_create(256);
    int *p = (int *)t_arena_alloc(a, sizeof(int));
    T_ASSERT_NOT_NULL(p);
    *p = 42;
    T_ASSERT_EQ(*p, 42);
    t_arena_destroy(a);
}

T_TEST(arena_alloc_many) {
    t_arena *a = t_arena_create(128);
    for (int i = 0; i < 1000; i++) {
        int *p = (int *)t_arena_alloc(a, sizeof(int));
        T_ASSERT_NOT_NULL(p);
        *p = i;
    }
    T_ASSERT(t_arena_used(a) > 0);
    t_arena_destroy(a);
}

T_TEST(arena_alloc_zero) {
    t_arena *a = t_arena_create(64);
    unsigned char *p = (unsigned char *)t_arena_alloc_zero(a, 32);
    T_ASSERT_NOT_NULL(p);
    for (int i = 0; i < 32; i++)
        T_ASSERT_EQ((int)p[i], 0);
    t_arena_destroy(a);
}

T_TEST(arena_strdup) {
    t_arena *a = t_arena_create(64);
    char *s = t_arena_strdup(a, "hello world");
    T_ASSERT_NOT_NULL(s);
    T_ASSERT_STR_EQ(s, "hello world");
    t_arena_destroy(a);
}

T_TEST(arena_memdup) {
    t_arena *a = t_arena_create(64);
    int data[] = {1, 2, 3, 4, 5};
    int *copy = (int *)t_arena_memdup(a, data, sizeof(data));
    T_ASSERT_NOT_NULL(copy);
    T_ASSERT_MEM_EQ(copy, data, sizeof(data));
    t_arena_destroy(a);
}

T_TEST(arena_reset) {
    t_arena *a = t_arena_create(128);
    t_arena_alloc(a, 64);
    T_ASSERT(t_arena_used(a) > 0);
    size_t blocks_before = t_arena_block_count(a);
    t_arena_reset(a);
    T_ASSERT_EQ((int)t_arena_used(a), 0);
    T_ASSERT_EQ((int)t_arena_block_count(a), (int)blocks_before);
    t_arena_alloc(a, 32);
    T_ASSERT(t_arena_used(a) > 0);
    t_arena_destroy(a);
}

T_TEST(arena_large_alloc) {
    t_arena *a = t_arena_create(128);
    void *big = t_arena_alloc(a, 1024);
    T_ASSERT_NOT_NULL(big);
    memset(big, 0xFF, 1024);
    T_ASSERT(t_arena_used(a) >= 1024);
    t_arena_destroy(a);
}

int main(void) {
    return t_run_all_tests();
}
