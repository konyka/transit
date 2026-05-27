#include "t_test.h"
#include "t_ratelimit.h"

T_TEST(rl_create_destroy) {
    t_ratelimit *rl = t_ratelimit_create(10, 1.0);
    T_ASSERT_NOT_NULL(rl);
    t_ratelimit_destroy(rl);
}

T_TEST(rl_allow_within_budget) {
    t_ratelimit *rl = t_ratelimit_create(5, 0.0);
    for (int i = 0; i < 5; i++) {
        T_ASSERT_EQ(t_ratelimit_allow(rl, 1000), 1);
    }
    T_ASSERT_EQ((int)t_ratelimit_total_allowed(rl), 5);
    t_ratelimit_destroy(rl);
}

T_TEST(rl_reject_over_budget) {
    t_ratelimit *rl = t_ratelimit_create(3, 0.0);
    T_ASSERT_EQ(t_ratelimit_allow(rl, 0), 1);
    T_ASSERT_EQ(t_ratelimit_allow(rl, 0), 1);
    T_ASSERT_EQ(t_ratelimit_allow(rl, 0), 1);
    T_ASSERT_EQ(t_ratelimit_allow(rl, 0), 0);
    T_ASSERT_EQ(t_ratelimit_allow(rl, 0), 0);
    T_ASSERT_EQ((int)t_ratelimit_total_rejected(rl), 2);
    t_ratelimit_destroy(rl);
}

T_TEST(rl_refill_over_time) {
    t_ratelimit *rl = t_ratelimit_create(2, 1.0);
    T_ASSERT_EQ(t_ratelimit_allow(rl, 0), 1);
    T_ASSERT_EQ(t_ratelimit_allow(rl, 0), 1);
    T_ASSERT_EQ(t_ratelimit_allow(rl, 0), 0);
    T_ASSERT_EQ(t_ratelimit_allow(rl, 1000), 1);
    T_ASSERT_EQ((int)t_ratelimit_total_allowed(rl), 3);
    t_ratelimit_destroy(rl);
}

T_TEST(rl_available_count) {
    t_ratelimit *rl = t_ratelimit_create(10, 0.0);
    T_ASSERT_EQ((int)t_ratelimit_available(rl, 0), 10);
    t_ratelimit_allow(rl, 0);
    T_ASSERT_EQ((int)t_ratelimit_available(rl, 0), 9);
    t_ratelimit_destroy(rl);
}

T_TEST(rl_reset) {
    t_ratelimit *rl = t_ratelimit_create(5, 0.0);
    for (int i = 0; i < 5; i++) t_ratelimit_allow(rl, 0);
    t_ratelimit_reset(rl);
    T_ASSERT_EQ((int)t_ratelimit_total_allowed(rl), 0);
    T_ASSERT_EQ((int)t_ratelimit_total_rejected(rl), 0);
    T_ASSERT_EQ(t_ratelimit_allow(rl, 0), 1);
    t_ratelimit_destroy(rl);
}

int main(void) {
    return t_run_all_tests();
}
