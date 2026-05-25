#include "t_test.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

/* Forward declaration for the public API used before its definition in this file */
t_test_suite *t_suite_create(const char *name);

#if defined(_WIN32)
#include <windows.h>
#endif

/* Forward declarations for internal helpers (none needed here) */

/* Global state for tests */
static t_test_suite *g_all_suite = NULL;
static int g_current_test_failed = 0;
static const t_test_case *g_current_case = NULL;
static double g_suite_start_time = 0.0;

/* Time utilities (monotonic) */
static double t_now_sec(void)
{
#if defined(_WIN32)
    static int g_inited = 0;
    static LARGE_INTEGER freq;
    static LARGE_INTEGER start;
    if(!g_inited){
        QueryPerformanceFrequency(&freq);
        QueryPerformanceCounter(&start);
        g_inited = 1;
    }
    LARGE_INTEGER now;
    QueryPerformanceCounter(&now);
    return (double)(now.QuadPart - start.QuadPart) / (double)freq.QuadPart;
#else
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (double)ts.tv_sec + (double)ts.tv_nsec / 1e9;
#endif
}

/* Simple colored output helpers */
static void t_color_set(int color_code)
{
#if defined(_WIN32)
    HANDLE h = GetStdHandle(STD_OUTPUT_HANDLE);
    WORD w = FOREGROUND_RED | FOREGROUND_GREEN | FOREGROUND_BLUE; // default
    switch(color_code){
        case 2: w = FOREGROUND_GREEN | FOREGROUND_INTENSITY; break; // GREEN
        case 4: w = FOREGROUND_RED | FOREGROUND_INTENSITY; break;   // RED
        case 6: w = FOREGROUND_GREEN | FOREGROUND_RED | FOREGROUND_INTENSITY; break; // YELLOW
        default: w = FOREGROUND_RED | FOREGROUND_GREEN | FOREGROUND_BLUE; break;
    }
    SetConsoleTextAttribute(h, w);
#else
    const char *seq = "";
    switch(color_code){
        case 2: seq = "\033[1;32m"; break; // GREEN
        case 4: seq = "\033[1;31m"; break; // RED
        case 6: seq = "\033[1;33m"; break; // YELLOW
        default: seq = ""; break;
    }
    if(*seq) printf("%s", seq);
#endif
}

static void t_color_reset(void)
{
#if defined(_WIN32)
    HANDLE h = GetStdHandle(STD_OUTPUT_HANDLE);
    SetConsoleTextAttribute(h, FOREGROUND_RED | FOREGROUND_GREEN | FOREGROUND_BLUE);
#else
    printf("\033[0m");
#endif
}

/* Ensure a global suite exists (auto-registration) */
static void ensure_global_suite(void)
{
    if(!g_all_suite){
        g_all_suite = t_suite_create("Auto-registered tests");
    }
}

/* Public API implementations (as declared in header) */
t_test_suite *t_suite_create(const char *name)
{
    t_test_suite *s = (t_test_suite*)malloc(sizeof(*s));
    if(!s) return NULL;
    s->name = name;
    s->cases = NULL;
    s->count = 0;
    s->capacity = 0;
    return s;
}

void t_suite_destroy(t_test_suite *suite)
{
    if(!suite) return;
    if(suite->cases) free(suite->cases);
    free(suite);
}

void t_suite_add(t_test_suite *suite, const char *name, t_test_fn fn, const char *file, int line)
{
    if(!suite) return;
    if(suite->count == suite->capacity){
        size_t newcap = suite->capacity ? suite->capacity * 2 : 4;
        t_test_case *nc = (t_test_case*)realloc(suite->cases, newcap * sizeof(t_test_case));
        if(!nc) return; // allocation failed
        suite->cases = nc;
        suite->capacity = newcap;
    }
    t_test_case *c = &suite->cases[suite->count++];
    c->name = name;
    c->fn = fn;
    c->file = file;
    c->line = line;
}

int t_suite_run(t_test_suite *suite)
{
    if(!suite) return 0;
    int total = (int)suite->count;
    int passed = 0;
    g_suite_start_time = t_now_sec();
    t_color_reset(); printf("\n");
    t_color_set(6); printf("Running test suite: %s\n", suite->name); t_color_reset();
    for(size_t i = 0; i < suite->count; ++i){
        g_current_case = &suite->cases[i];
        g_current_test_failed = 0;
        t_color_set(6); printf("[TEST] %s\n", suite->cases[i].name); t_color_reset();
        if(suite->cases[i].fn){
            suite->cases[i].fn();
        }
        if(!g_current_test_failed){
            t_color_set(2); printf("[PASS] %s\n", suite->cases[i].name); t_color_reset();
            ++passed;
        } else {
            t_color_set(4); printf("[FAIL] %s\n", suite->cases[i].name); t_color_reset();
        }
    }
    double elapsed = t_now_sec() - g_suite_start_time;
    int failed = total - passed;
    t_color_set(6); printf("\nSummary: Total=%d, Passed=%d, Failed=%d, Time=%.3fs\n", total, passed, failed, elapsed); t_color_reset();
    return failed == 0 ? 0 : 1;
}

int t_run_all_tests(void)
{
    if(!g_all_suite){
        return 0;
    }
    return t_suite_run(g_all_suite);
}

double t_test_elapsed_sec(void)
{
    return t_now_sec() - g_suite_start_time;
}

/* Assertion implementations (provide detailed failure messages) */
void t_assert_impl(int expr, const char *expr_str, const char *file, int line)
{
    if(!expr){
        t_color_set(4);
        fprintf(stderr, "%s:%d: Assertion failed: %s\n", file, line, expr_str);
        t_color_reset();
        g_current_test_failed = 1;
    }
}

void t_assert_eq_impl(long long a, long long b, const char *a_str, const char *b_str, const char *file, int line)
{
    if(a != b){
        t_color_set(4);
        fprintf(stderr, "%s:%d: Assertion failed: %s == %s (actual=%lld, expected=%lld)\n", file, line, a_str, b_str, (long long)a, (long long)b);
        t_color_reset();
        g_current_test_failed = 1;
    }
}

void t_assert_ne_impl(long long a, long long b, const char *a_str, const char *b_str, const char *file, int line)
{
    if(a == b){
        t_color_set(4);
        fprintf(stderr, "%s:%d: Assertion failed: %s != %s (both are %lld)\n", file, line, a_str, b_str, (long long)a);
        t_color_reset();
        g_current_test_failed = 1;
    }
}

void t_assert_null_impl(const void *ptr, const char *ptr_str, const char *file, int line)
{
    if(ptr != NULL){
        t_color_set(4);
        fprintf(stderr, "%s:%d: Assertion failed: %s is NULL (non-NULL)\n", file, line, ptr_str);
        t_color_reset();
        g_current_test_failed = 1;
    }
}

void t_assert_not_null_impl(const void *ptr, const char *ptr_str, const char *file, int line)
{
    if(ptr == NULL){
        t_color_set(4);
        fprintf(stderr, "%s:%d: Assertion failed: %s is not NULL (NULL)\n", file, line, ptr_str);
        t_color_reset();
        g_current_test_failed = 1;
    }
}

void t_assert_str_eq_impl(const char *a, const char *b, const char *a_str, const char *b_str, const char *file, int line)
{
    int ok = 0;
    if(a == NULL && b == NULL) ok = 1;
    else if(a != NULL && b != NULL && strcmp(a, b) == 0) ok = 1;
    if(!ok){
        const char *ra = (a != NULL) ? a : "NULL";
        const char *rb = (b != NULL) ? b : "NULL";
        t_color_set(4);
        fprintf(stderr, "%s:%d: Assertion failed: %s == %s (\"%s\" vs \"%s\")\n", file, line, a_str, b_str, ra, rb);
        t_color_reset();
        g_current_test_failed = 1;
    }
}

void t_assert_mem_eq_impl(const void *a, const void *b, size_t n, const char *a_str, const char *b_str, const char *file, int line)
{
    if(memcmp(a, b, n) != 0){
        t_color_set(4);
        fprintf(stderr, "%s:%d: Assertion failed: %s == %s (memory differs in first %zu bytes)\n", file, line, a_str, b_str, n);
        t_color_reset();
        g_current_test_failed = 1;
    }
}

/* Global registration hook (auto) */
void t_register_test(const char *name, t_test_fn fn, const char *file, int line)
{
    ensure_global_suite();
    t_suite_add(g_all_suite, name, fn, file, line);
}
