#include "t_test.h"
#include "t_map.h"

#include <stdint.h>
#include <stdio.h>

T_TEST(map_insert_get_remove) {
    t_map m;
    int a = 10;
    int b = 20;

    t_map_init(&m);
    T_ASSERT_EQ(t_map_insert(&m, "alpha", &a), 0);
    T_ASSERT_EQ(t_map_insert(&m, "beta", &b), 0);
    T_ASSERT_EQ((int)t_map_len(&m), 2);
    T_ASSERT(t_map_get(&m, "alpha") == &a);
    T_ASSERT(t_map_contains(&m, "beta"));
    T_ASSERT(t_map_remove(&m, "alpha") == &a);
    T_ASSERT_NULL(t_map_get(&m, "alpha"));
    T_ASSERT_EQ((int)t_map_len(&m), 1);
    t_map_destroy(&m);
}

T_TEST(map_replace_keeps_length) {
    t_map m;
    int old_value = 1;
    int new_value = 2;

    t_map_init(&m);
    T_ASSERT_EQ(t_map_insert(&m, "k", &old_value), 0);
    T_ASSERT_EQ(t_map_insert(&m, "k", &new_value), 0);
    T_ASSERT_EQ((int)t_map_len(&m), 1);
    T_ASSERT(t_map_get(&m, "k") == &new_value);
    t_map_destroy(&m);
}

T_TEST(map_tombstone_reuse_and_compact) {
    enum { N = 96 };
    t_map m;
    int values[N];
    char key[32];

    t_map_init(&m);
    for (int i = 0; i < N; i++) {
        values[i] = i;
        snprintf(key, sizeof(key), "k-%03d", i);
        T_ASSERT_EQ(t_map_insert(&m, key, &values[i]), 0);
    }
    for (int i = 0; i < N; i += 2) {
        snprintf(key, sizeof(key), "k-%03d", i);
        T_ASSERT_NOT_NULL(t_map_remove(&m, key));
    }
    T_ASSERT_EQ((int)t_map_len(&m), N / 2);
    T_ASSERT_EQ(t_map_compact(&m), 0);
    for (int i = 1; i < N; i += 2) {
        snprintf(key, sizeof(key), "k-%03d", i);
        T_ASSERT(t_map_get(&m, key) == &values[i]);
    }
    for (int i = 0; i < N; i += 2) {
        values[i] = i + N;
        snprintf(key, sizeof(key), "r-%03d", i);
        T_ASSERT_EQ(t_map_insert(&m, key, &values[i]), 0);
        T_ASSERT(t_map_get(&m, key) == &values[i]);
    }
    T_ASSERT_EQ((int)t_map_len(&m), N);
    t_map_destroy(&m);
}

int main(void) {
    return t_run_all_tests();
}
