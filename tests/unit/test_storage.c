#include "t_test.h"
#include "t_storage.h"
#include "t_mmap.h"
#include <string.h>
#include <unistd.h>

/* Basic in-memory storage tests */
T_TEST(storage_mem_create_destroy) {
    t_storage *s = t_storage_create(T_STORAGE_MEM, NULL);
    T_ASSERT_NOT_NULL(s);
    t_storage_destroy(s);
}

T_TEST(storage_mem_put_get) {
    t_storage *s = t_storage_create(T_STORAGE_MEM, NULL);
    T_ASSERT_EQ(t_storage_put(s, 1, "hello", 5), 0);
    const void *data = NULL; size_t len = 0;
    T_ASSERT_EQ(t_storage_get(s, 1, &data, &len), 0);
    T_ASSERT_EQ((int)len, 5);
    T_ASSERT(memcmp(data, "hello", 5) == 0);
    t_storage_destroy(s);
}

T_TEST(storage_mem_delete) {
    t_storage *s = t_storage_create(T_STORAGE_MEM, NULL);
    t_storage_put(s, 1, "hello", 5);
    T_ASSERT_EQ(t_storage_delete(s, 1), 0);
    const void *data = NULL; size_t len = 0;
    T_ASSERT(t_storage_get(s, 1, &data, &len) != 0);
    t_storage_destroy(s);
}

T_TEST(storage_mem_count) {
    t_storage *s = t_storage_create(T_STORAGE_MEM, NULL);
    T_ASSERT_EQ((int)t_storage_count(s), 0);
    t_storage_put(s, 1, "a", 1);
    t_storage_put(s, 2, "b", 1);
    T_ASSERT_EQ((int)t_storage_count(s), 2);
    t_storage_destroy(s);
}

T_TEST(storage_mem_contains) {
    t_storage *s = t_storage_create(T_STORAGE_MEM, NULL);
    T_ASSERT_FALSE(t_storage_contains(s, 1));
    t_storage_put(s, 1, "x", 1);
    T_ASSERT_TRUE(t_storage_contains(s, 1));
    t_storage_destroy(s);
}

T_TEST(storage_mem_overwrite) {
    t_storage *s = t_storage_create(T_STORAGE_MEM, NULL);
    t_storage_put(s, 1, "hello", 5);
    t_storage_put(s, 1, "world", 5);
    const void *data = NULL; size_t len = 0;
    t_storage_get(s, 1, &data, &len);
    T_ASSERT(memcmp(data, "world", 5) == 0);
    t_storage_destroy(s);
}

T_TEST(storage_mem_empty_value) {
    t_storage *s = t_storage_create(T_STORAGE_MEM, NULL);
    T_ASSERT_EQ(t_storage_put(s, 42, NULL, 0), 0);
    T_ASSERT_TRUE(t_storage_contains(s, 42));
    const void *data = (const void *)0x1; size_t len = 99;
    T_ASSERT_EQ(t_storage_get(s, 42, &data, &len), 0);
    T_ASSERT_EQ((int)len, 0);
    T_ASSERT(data == NULL);
    t_storage_destroy(s);
}

/* Simple mmap lifecyle tests */
T_TEST(mmap_create_close) {
    const char *path = "/tmp/test_transit_mmap.bin";
    unlink(path);
    t_mmap mm;
    T_ASSERT_EQ(t_mmap_create(&mm, path, 4096), 0);
    T_ASSERT_NOT_NULL(t_mmap_data(&mm));
    T_ASSERT_EQ(t_mmap_size(&mm), (size_t)4096);
    memset(t_mmap_data(&mm), 0xAA, 4096);
    t_mmap_sync(&mm);
    t_mmap_close(&mm);
    unlink(path);
}

T_TEST(mmap_persistence) {
    const char *path = "/tmp/test_transit_mmap2.bin";
    unlink(path);
    t_mmap mm;
    t_mmap_create(&mm, path, 4096);
    uint32_t *vals = (uint32_t *)t_mmap_data(&mm);
    vals[0] = 0xDEADBEEF;
    t_mmap_sync(&mm);
    t_mmap_close(&mm);

    t_mmap mm2;
    T_ASSERT_EQ(t_mmap_open(&mm2, path), 0);
    uint32_t *vals2 = (uint32_t *)t_mmap_data(&mm2);
    T_ASSERT_EQ((int)vals2[0], (int)0xDEADBEEF);
    t_mmap_close(&mm2);
    unlink(path);
}

int main(void) {
    return t_run_all_tests();
}
