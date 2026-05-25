/* Include core headers via project-relative paths to avoid include-path setup issues */
#include "../../src/core/t_compiler.h"
#include "../../src/core/t_atomic.h"
#include "../../src/core/t_time.h"
#include "t_test.h"

T_TEST(compiler_detection) {
    T_ASSERT(T_COMPILER_CLANG || T_COMPILER_GCC || T_COMPILER_MSVC || 
             (!T_COMPILER_CLANG && !T_COMPILER_GCC && !T_COMPILER_MSVC));
}

T_TEST(static_assertion) {
    T_STATIC_ASSERT(1, "should_compile");
    T_ASSERT(1);
}

T_TEST(container_of_macro) {
    struct foo { int a; int b; };
    struct foo f = {.a = 1, .b = 2};
    int *bptr = &f.b;
    struct foo *fptr = T_CONTAINER_OF(bptr, struct foo, b);
    T_ASSERT_EQ(fptr->a, 1);
    T_ASSERT_EQ(fptr->b, 2);
}

T_TEST(array_size_macro) {
    int arr[] = {1, 2, 3, 4, 5};
    T_ASSERT_EQ((int)T_ARRAY_SIZE(arr), 5);
}

T_TEST(min_max_macros) {
    T_ASSERT_EQ(T_MIN(3, 5), 3);
    T_ASSERT_EQ(T_MAX(3, 5), 5);
}

T_TEST(bit_operations) {
    int x = 0;
    x = T_BITS_SET(x, 0x3);
    T_ASSERT_EQ(x, 0x3);
    T_ASSERT(T_BITS_TEST(x, 0x1));
    T_ASSERT(T_BITS_TEST(x, 0x2));
    x = T_BITS_CLEAR(x, 0x1);
    T_ASSERT_EQ(x, 0x2);
    x = T_BITS_TOGGLE(x, 0x3);
    T_ASSERT_EQ(x, 0x1);
}

T_TEST(rounding_macros) {
    T_ASSERT_EQ(T_ROUND_UP(17, 8), 24);
    T_ASSERT_EQ(T_ROUND_UP(16, 8), 16);
    T_ASSERT_EQ(T_ROUND_UP(0, 8), 0);
    T_ASSERT_EQ(T_ROUND_DOWN(17, 8), 16);
    T_ASSERT_EQ(T_ROUND_DOWN(16, 8), 16);
}

T_TEST(atomic_int_operations) {
    t_atomic_int val = T_ATOMIC_INIT(0);
    T_ASSERT_EQ(t_atomic_load_int(&val), 0);
    t_atomic_store_int(&val, 42);
    T_ASSERT_EQ(t_atomic_load_int(&val), 42);
    T_ASSERT_EQ(t_atomic_fetch_add_int(&val, 8), 42);
    T_ASSERT_EQ(t_atomic_load_int(&val), 50);
    T_ASSERT_EQ(t_atomic_add_fetch_int(&val, 10), 60);
    T_ASSERT_EQ(t_atomic_sub_fetch_int(&val, 30), 30);
}

T_TEST(atomic_cas_operation) {
    t_atomic_int val = T_ATOMIC_INIT(10);
    T_ASSERT(t_atomic_cas_int(&val, 10, 20));
    T_ASSERT_EQ(t_atomic_load_int(&val), 20);
    T_ASSERT(!t_atomic_cas_int(&val, 10, 30));
    T_ASSERT_EQ(t_atomic_load_int(&val), 20);
}

T_TEST(atomic_ptr_operations) {
    void *a = (void*)0xDEAD;
    void *b = (void*)0xBEEF;
    t_atomic_ptr ptr = T_ATOMIC_INIT(a);
    T_ASSERT(t_atomic_load_ptr(&ptr) == a);
    t_atomic_store_ptr(&ptr, b);
    T_ASSERT(t_atomic_load_ptr(&ptr) == b);
    T_ASSERT(t_atomic_cas_ptr(&ptr, b, a));
    T_ASSERT(t_atomic_load_ptr(&ptr) == a);
}

T_TEST(time_monotonic) {
    t_timespec t1 = t_time_now();
    t_time_sleep_ms(1);
    t_timespec t2 = t_time_now();
    int64_t diff = t_time_diff_ns(t1, t2);
    T_ASSERT(diff > 0);
}

T_TEST(time_now_functions) {
    int64_t ns = t_time_now_ns();
    int64_t us = t_time_now_us();
    int64_t ms = t_time_now_ms();
    double sec = t_time_now_sec();
    T_ASSERT(ns > 0);
    T_ASSERT(us > 0);
    T_ASSERT(ms > 0);
    T_ASSERT(sec > 0.0);
    T_ASSERT(us <= ns / 1000 + 1);
}

int main(void) {
    return t_run_all_tests();
}
