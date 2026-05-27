#include "t_test.h"
#include "t_ttl.h"
#include <string.h>

static size_t g_expired_count;
static uint64_t g_last_expired_id;

static void on_expire(const t_ttl_entry *entry, void *ud) {
    (void)ud;
    if (entry) {
        g_expired_count++;
        g_last_expired_id = entry->msg_id;
    }
}

T_TEST(ttl_create_destroy) {
    t_ttl *ttl = t_ttl_create(on_expire, NULL);
    T_ASSERT_NOT_NULL(ttl);
    T_ASSERT_EQ((int)t_ttl_count(ttl), 0);
    t_ttl_destroy(ttl);
}

T_TEST(ttl_add_and_count) {
    t_ttl *ttl = t_ttl_create(on_expire, NULL);
    T_ASSERT_EQ(t_ttl_add(ttl, 1, "topic", (const uint8_t *)"hello", 5, 1000), 0);
    T_ASSERT_EQ(t_ttl_add(ttl, 2, "topic", (const uint8_t *)"world", 5, 2000), 0);
    T_ASSERT_EQ((int)t_ttl_count(ttl), 2);
    t_ttl_destroy(ttl);
}

T_TEST(ttl_remove) {
    t_ttl *ttl = t_ttl_create(on_expire, NULL);
    t_ttl_add(ttl, 1, "t", NULL, 0, 1000);
    T_ASSERT_EQ(t_ttl_remove(ttl, 1), 0);
    T_ASSERT_EQ((int)t_ttl_count(ttl), 0);
    t_ttl_destroy(ttl);
}

T_TEST(ttl_expire_removes_old) {
    t_ttl *ttl = t_ttl_create(on_expire, NULL);
    g_expired_count = 0;
    t_ttl_add(ttl, 10, "a", (const uint8_t *)"old", 3, 100);
    t_ttl_add(ttl, 20, "b", (const uint8_t *)"new", 3, 9999);

    size_t n = t_ttl_expire(ttl, 500);
    T_ASSERT_EQ((int)n, 1);
    T_ASSERT_EQ((int)g_expired_count, 1);
    T_ASSERT_EQ((int)g_last_expired_id, 10);
    T_ASSERT_EQ((int)t_ttl_count(ttl), 1);
    t_ttl_destroy(ttl);
}

T_TEST(ttl_is_expired_check) {
    t_ttl *ttl = t_ttl_create(on_expire, NULL);
    t_ttl_add(ttl, 5, "t", NULL, 0, 100);
    T_ASSERT_EQ(t_ttl_is_expired(ttl, 5, 50), 0);
    T_ASSERT_EQ(t_ttl_is_expired(ttl, 5, 200), 1);
    t_ttl_destroy(ttl);
}

T_TEST(ttl_expire_all) {
    t_ttl *ttl = t_ttl_create(on_expire, NULL);
    g_expired_count = 0;
    t_ttl_add(ttl, 1, "t", NULL, 0, 10);
    t_ttl_add(ttl, 2, "t", NULL, 0, 20);
    t_ttl_add(ttl, 3, "t", NULL, 0, 30);

    size_t n = t_ttl_expire(ttl, 100);
    T_ASSERT_EQ((int)n, 3);
    T_ASSERT_EQ((int)t_ttl_count(ttl), 0);
    t_ttl_destroy(ttl);
}

int main(void) {
    return t_run_all_tests();
}
