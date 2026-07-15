#include "t_test.h"
#include "t_pool.h"
#include "t_slab.h"
#include <string.h>

T_TEST(pool_create_destroy) {
    t_pool *p = t_pool_create(0);
    T_ASSERT_NOT_NULL(p);
    t_pool_destroy(p);
}

T_TEST(pool_alloc_basic) {
    t_pool *p = t_pool_create(0);
    void *ptr = t_pool_alloc(p, 32);
    T_ASSERT_NOT_NULL(ptr);
    memset(ptr, 0xAB, 32);
    t_pool_free(p, ptr, 32);
    t_pool_destroy(p);
}

T_TEST(pool_alloc_zero_filled) {
    t_pool *p = t_pool_create(0);
    unsigned char *ptr = (unsigned char *)t_pool_alloc_zero(p, 128);
    T_ASSERT_NOT_NULL(ptr);
    for (int i = 0; i < 128; i++)
        T_ASSERT_EQ((int)ptr[i], 0);
    t_pool_free(p, ptr, 128);
    t_pool_destroy(p);
}

T_TEST(pool_alloc_various_sizes) {
    t_pool *p = t_pool_create(0);
    size_t sizes[] = {16, 32, 48, 64, 96, 128, 256, 512, 1024, 2048, 4096};
    void *ptrs[11];
    for (int i = 0; i < 11; i++) {
        ptrs[i] = t_pool_alloc(p, sizes[i]);
        T_ASSERT_NOT_NULL(ptrs[i]);
        memset(ptrs[i], i, sizes[i]);
    }
    for (int i = 0; i < 11; i++)
        t_pool_free(p, ptrs[i], sizes[i]);
    t_pool_destroy(p);
}

T_TEST(pool_size_class) {
    T_ASSERT(t_pool_size_class(1) >= 0);
    T_ASSERT(t_pool_size_class(16) >= 0);
    T_ASSERT(t_pool_size_class(100) >= 0);
    T_ASSERT(t_pool_size_class(8192) >= 0);
}

T_TEST(pool_alloc_alignment) {
    t_pool *p = t_pool_create(0);
    for (int i = 0; i < 100; i++) {
        void *ptr = t_pool_alloc(p, 1);
        T_ASSERT_EQ(((uintptr_t)ptr) % T_POOL_MIN_ALIGN, (uintptr_t)0);
    }
    t_pool_destroy(p);
}

T_TEST(pool_statistics) {
    t_pool *p = t_pool_create(0);
    T_ASSERT_EQ((int)t_pool_used_bytes(p), 0);
    void *ptr = t_pool_alloc(p, 64);
    T_ASSERT(t_pool_used_bytes(p) > 0);
    T_ASSERT(t_pool_total_bytes(p) > 0);
    T_ASSERT(t_pool_chunk_count(p) >= 1);
    t_pool_free(p, ptr, 64);
    t_pool_destroy(p);
}

T_TEST(pool_large_allocation) {
    t_pool *p = t_pool_create(0);
    void *ptr = t_pool_alloc(p, 10000);
    T_ASSERT_NOT_NULL(ptr);
    memset(ptr, 0xFF, 10000);
    t_pool_free(p, ptr, 10000);
    t_pool_destroy(p);
}

T_TEST(pool_free_ignores_wrong_size) {
    t_pool *p = t_pool_create(0);
    void *a = t_pool_alloc(p, 32);
    void *b = t_pool_alloc(p, 32);
    T_ASSERT_NOT_NULL(a);
    T_ASSERT_NOT_NULL(b);
    /* Mismatched size must not place the 32-byte block on the 64-byte freelist. */
    t_pool_free(p, a, 64);
    void *c = t_pool_alloc(p, 32);
    T_ASSERT(c == a);
    t_pool_free(p, b, 32);
    t_pool_free(p, c, 32);
    t_pool_destroy(p);
}

T_TEST(pool_alloc_array) {
    t_pool *p = t_pool_create(0);
    int *arr = (int *)t_pool_alloc_array(p, sizeof(int), 100);
    T_ASSERT_NOT_NULL(arr);
    for (int i = 0; i < 100; i++)
        arr[i] = i;
    for (int i = 0; i < 100; i++)
        T_ASSERT_EQ(arr[i], i);
    t_pool_free(p, arr, sizeof(int) * 100);
    t_pool_destroy(p);
}

T_TEST(slab_create_destroy) {
    t_slab *s = t_slab_create(64, 0);
    T_ASSERT_NOT_NULL(s);
    T_ASSERT_EQ((int)t_slab_object_size(s), (int)64);
    t_slab_destroy(s);
}

T_TEST(slab_alloc_free) {
    t_slab *s = t_slab_create(32, 0);
    void *obj = t_slab_alloc(s);
    T_ASSERT_NOT_NULL(obj);
    T_ASSERT_EQ((int)t_slab_used_count(s), 1);
    memset(obj, 0xAA, 32);
    t_slab_free(s, obj);
    T_ASSERT_EQ((int)t_slab_used_count(s), 0);
    t_slab_destroy(s);
}

T_TEST(slab_alloc_many) {
    t_slab *s = t_slab_create(16, 0);
    #define OBJ_COUNT 1000
    void *objs[OBJ_COUNT];
    for (int i = 0; i < OBJ_COUNT; i++) {
        objs[i] = t_slab_alloc(s);
        T_ASSERT_NOT_NULL(objs[i]);
    }
    T_ASSERT_EQ((int)t_slab_used_count(s), OBJ_COUNT);
    for (int i = 0; i < OBJ_COUNT; i++)
        t_slab_free(s, objs[i]);
    T_ASSERT_EQ((int)t_slab_used_count(s), 0);
    t_slab_destroy(s);
}

T_TEST(slab_alloc_zero) {
    t_slab *s = t_slab_create(32, 0);
    unsigned char *obj = (unsigned char *)t_slab_alloc_zero(s);
    T_ASSERT_NOT_NULL(obj);
    for (int i = 0; i < 32; i++)
        T_ASSERT_EQ((int)obj[i], 0);
    t_slab_free(s, obj);
    t_slab_destroy(s);
}

T_TEST(slab_reuse_freed) {
    t_slab *s = t_slab_create(8, 0);
    void *a = t_slab_alloc(s);
    t_slab_free(s, a);
    void *b = t_slab_alloc(s);
    T_ASSERT(b == a);
    t_slab_free(s, b);
    t_slab_destroy(s);
}

int main(void) {
    return t_run_all_tests();
}
