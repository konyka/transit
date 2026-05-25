#include "t_test.h"

T_TEST(basic_assertion) {
    T_ASSERT(1 == 1);
    T_ASSERT_TRUE(1);
    T_ASSERT_FALSE(0);
}

T_TEST(equality_assertions) {
    T_ASSERT_EQ(42, 42);
    T_ASSERT_NE(1, 2);
}

T_TEST(string_assertions) {
    T_ASSERT_STR_EQ("hello", "hello");
    T_ASSERT_NULL(NULL);
    T_ASSERT_NOT_NULL((void*)0x1);
}

T_TEST(memory_assertions) {
    int a[] = {1, 2, 3};
    int b[] = {1, 2, 3};
    T_ASSERT_MEM_EQ(a, b, sizeof(a));
}

int main(void) {
    return t_run_all_tests();
}
