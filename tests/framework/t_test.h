#ifndef T_TEST_H
#define T_TEST_H

#include <stddef.h>

/* Cross-compiler constructor support for automatic registration. */
#if defined(_MSC_VER) && !defined(__clang__)
#define T_CONSTRUCTOR
#define T_TEST(name) \
    static void test_##name(void); \
    static void register_test_##name(void) { \
        t_register_test(#name, test_##name, __FILE__, __LINE__); \
    } \
    static int register_test_##name##_crt(void) { \
        register_test_##name(); \
        return 0; \
    } \
    __pragma(section(".CRT$XCU", read)) \
    __declspec(allocate(".CRT$XCU")) static int (*register_test_##name##_ptr)(void) = register_test_##name##_crt; \
    static void test_##name(void)
#else
#define T_CONSTRUCTOR __attribute__((constructor))
#define T_TEST(name) \
    static void test_##name(void); \
    static void T_CONSTRUCTOR register_test_##name(void) { \
        t_register_test(#name, test_##name, __FILE__, __LINE__); \
    } \
    static void test_##name(void)
#endif

/* Test function signature */
typedef void (*t_test_fn)(void);

/* Test registration structure */
typedef struct t_test_case {
    const char *name;       /* Test name */
    t_test_fn   fn;         /* Test function */
    const char *file;       /* Source file */
    int         line;       /* Line number */
} t_test_case;

/* Test suite */
typedef struct t_test_suite {
    const char    *name;
    t_test_case   *cases;
    size_t         count;
    size_t         capacity;
} t_test_suite;

/* Core API */
t_test_suite *t_suite_create(const char *name);
void          t_suite_destroy(t_test_suite *suite);
void          t_suite_add(t_test_suite *suite, const char *name, t_test_fn fn, const char *file, int line);
int           t_suite_run(t_test_suite *suite);  /* Returns 0 if all pass */

/* Assertion macros - these are the primary user-facing API */
#define T_ASSERT(expr) \
    t_assert_impl((expr), #expr, __FILE__, __LINE__)

#define T_ASSERT_EQ(a, b) \
    t_assert_eq_impl((long long)(a), (long long)(b), #a, #b, __FILE__, __LINE__)

#define T_ASSERT_NE(a, b) \
    t_assert_ne_impl((long long)(a), (long long)(b), #a, #b, __FILE__, __LINE__)

#define T_ASSERT_NULL(ptr) \
    t_assert_null_impl((const void*)(ptr), #ptr, __FILE__, __LINE__)

#define T_ASSERT_NOT_NULL(ptr) \
    t_assert_not_null_impl((const void*)(ptr), #ptr, __FILE__, __LINE__)

#define T_ASSERT_STR_EQ(a, b) \
    t_assert_str_eq_impl((a), (b), #a, #b, __FILE__, __LINE__)

#define T_ASSERT_MEM_EQ(a, b, n) \
    t_assert_mem_eq_impl((a), (b), (n), #a, #b, __FILE__, __LINE__)

#define T_ASSERT_TRUE(expr)  T_ASSERT(expr)
#define T_ASSERT_FALSE(expr) T_ASSERT(!(expr))

/* For MSVC compatibility, also provide a manual registration approach */
#define T_TEST_REGISTER(suite, name) \
    t_suite_add(suite, #name, test_##name, __FILE__, __LINE__)

/* Implementation functions (called by macros) */
void t_assert_impl(int expr, const char *expr_str, const char *file, int line);
void t_assert_eq_impl(long long a, long long b, const char *a_str, const char *b_str, const char *file, int line);
void t_assert_ne_impl(long long a, long long b, const char *a_str, const char *b_str, const char *file, int line);
void t_assert_null_impl(const void *ptr, const char *ptr_str, const char *file, int line);
void t_assert_not_null_impl(const void *ptr, const char *ptr_str, const char *file, int line);
void t_assert_str_eq_impl(const char *a, const char *b, const char *a_str, const char *b_str, const char *file, int line);
void t_assert_mem_eq_impl(const void *a, const void *b, size_t n, const char *a_str, const char *b_str, const char *file, int line);

/* Global registration (for constructor-based auto-registration) */
void t_register_test(const char *name, t_test_fn fn, const char *file, int line);

/* Main runner - runs all auto-registered tests */
int t_run_all_tests(void);

/* Timing utilities */
double t_test_elapsed_sec(void);  /* Time since test started */

#endif /* T_TEST_H */
