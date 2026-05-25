#ifndef T_TIME_H
#define T_TIME_H

#include "t_compiler.h"
#include <stdint.h>

#if T_PLATFORM_WINDOWS
    #include <windows.h>
#else
    #include <time.h>
    #include <sys/time.h>
#endif

typedef struct t_timespec {
    int64_t sec;
    int64_t nsec;
} t_timespec;

T_ALWAYS_INLINE t_timespec t_time_now(void) {
    t_timespec ts;
#if T_PLATFORM_WINDOWS
    LARGE_INTEGER freq, count;
    QueryPerformanceFrequency(&freq);
    QueryPerformanceCounter(&count);
    ts.sec = count.QuadPart / freq.QuadPart;
    ts.nsec = (int64_t)((double)(count.QuadPart % freq.QuadPart) / (double)freq.QuadPart * 1e9);
#else
    struct timespec cts;
    clock_gettime(CLOCK_MONOTONIC, &cts);
    ts.sec = (int64_t)cts.tv_sec;
    ts.nsec = (int64_t)cts.tv_nsec;
#endif
    return ts;
}

T_ALWAYS_INLINE int64_t t_time_now_ns(void) {
    t_timespec ts = t_time_now();
    return ts.sec * 1000000000LL + ts.nsec;
}

T_ALWAYS_INLINE int64_t t_time_now_us(void) {
    return t_time_now_ns() / 1000;
}

T_ALWAYS_INLINE int64_t t_time_now_ms(void) {
    return t_time_now_ns() / 1000000;
}

T_ALWAYS_INLINE double t_time_now_sec(void) {
    t_timespec ts = t_time_now();
    return (double)ts.sec + (double)ts.nsec / 1e9;
}

T_ALWAYS_INLINE int64_t t_time_diff_ns(t_timespec start, t_timespec end) {
    return (end.sec - start.sec) * 1000000000LL + (end.nsec - start.nsec);
}

T_ALWAYS_INLINE void t_time_sleep_ms(int64_t ms) {
#if T_PLATFORM_WINDOWS
    Sleep((DWORD)ms);
#else
    struct timespec ts;
    ts.tv_sec = ms / 1000;
    ts.tv_nsec = (ms % 1000) * 1000000L;
    nanosleep(&ts, NULL);
#endif
}

T_ALWAYS_INLINE void t_time_sleep_us(int64_t us) {
#if T_PLATFORM_WINDOWS
    Sleep((DWORD)(us / 1000));
#else
    struct timespec ts;
    ts.tv_sec = us / 1000000;
    ts.tv_nsec = (us % 1000000) * 1000L;
    nanosleep(&ts, NULL);
#endif
}

#endif /* T_TIME_H */
