#include "t_test.h"
#include "t_flowcontrol.h"
#include <stdlib.h>

T_TEST(fc_create_destroy) {
    t_flowcontrol *fc = t_fc_create(100, 1000);
    T_ASSERT_NOT_NULL(fc);
    T_ASSERT_EQ((int)t_fc_available(fc), 100);
    T_ASSERT_EQ((int)t_fc_max(fc), 100);
    T_ASSERT(!t_fc_is_blocked(fc));
    t_fc_destroy(fc);
}

T_TEST(fc_acquire_release) {
    t_flowcontrol *fc = t_fc_create(10, 0);
    T_ASSERT_EQ(t_fc_acquire(fc, 3), 0);
    T_ASSERT_EQ((int)t_fc_available(fc), 7);
    T_ASSERT(!t_fc_is_blocked(fc));
    t_fc_release(fc, 2);
    T_ASSERT_EQ((int)t_fc_available(fc), 9);
    t_fc_destroy(fc);
}

T_TEST(fc_block_when_exhausted) {
    t_flowcontrol *fc = t_fc_create(5, 0);
    T_ASSERT_EQ(t_fc_acquire(fc, 5), 0);
    T_ASSERT_EQ((int)t_fc_available(fc), 0);
    T_ASSERT(t_fc_acquire(fc, 1) != 0);
    T_ASSERT(t_fc_is_blocked(fc));
    t_fc_release(fc, 1);
    T_ASSERT(!t_fc_is_blocked(fc));
    t_fc_destroy(fc);
}

T_TEST(fc_refill) {
    t_flowcontrol *fc = t_fc_create(10, 0);
    t_fc_acquire(fc, 10);
    T_ASSERT_EQ((int)t_fc_available(fc), 0);
    t_fc_refill(fc);
    T_ASSERT_EQ((int)t_fc_available(fc), 10);
    T_ASSERT(!t_fc_is_blocked(fc));
    t_fc_destroy(fc);
}

T_TEST(fc_stats) {
    t_flowcontrol *fc = t_fc_create(100, 0);
    t_fc_acquire(fc, 10);
    t_fc_acquire(fc, 5);
    t_fc_acquire(fc, 200);
    t_fc_release(fc, 3);
    T_ASSERT_EQ((int)t_fc_total_acquired(fc), 15);
    T_ASSERT_EQ((int)t_fc_total_released(fc), 3);
    T_ASSERT_EQ((int)t_fc_total_rejected(fc), 200);
    t_fc_destroy(fc);
}

T_TEST(fc_oversized_acquire_does_not_block) {
    t_flowcontrol *fc = t_fc_create(10, 0);
    T_ASSERT(t_fc_acquire(fc, 100) != 0);
    T_ASSERT(!t_fc_is_blocked(fc));
    T_ASSERT_EQ((int)t_fc_available(fc), 10);
    T_ASSERT_EQ(t_fc_acquire(fc, 3), 0);
    t_fc_destroy(fc);
}

T_TEST(fc_release_caps_at_max) {
    t_flowcontrol *fc = t_fc_create(10, 0);
    t_fc_acquire(fc, 5);
    t_fc_release(fc, 20);
    T_ASSERT_EQ((int)t_fc_available(fc), 10);
    t_fc_destroy(fc);
}

int main(void) {
    return t_run_all_tests();
}
