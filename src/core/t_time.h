#ifndef T_TIME_H
#define T_TIME_H

#include "t_compiler.h"
#include <stdint.h>

#if T_PLATFORM_WINDOWS
    #include <windows.h>
#else
    #include <errno.h>
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
    if (ts.sec > INT64_MAX / 1000000000LL) return INT64_MAX;
    int64_t ns = ts.sec * 1000000000LL;
    if (ts.nsec > 0 && ns > INT64_MAX - ts.nsec) return INT64_MAX;
    return ns + ts.nsec;
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
    int64_t dsec = end.sec - start.sec;
    if (dsec > INT64_MAX / 1000000000LL) return INT64_MAX;
    if (dsec < INT64_MIN / 1000000000LL) return INT64_MIN;
    int64_t ns = dsec * 1000000000LL;
    int64_t dnsec = end.nsec - start.nsec;
    if (dnsec > 0 && ns > INT64_MAX - dnsec) return INT64_MAX;
    if (dnsec < 0 && ns < INT64_MIN - dnsec) return INT64_MIN;
    return ns + dnsec;
}

T_ALWAYS_INLINE void t_time_sleep_ms(int64_t ms) {
    if (ms <= 0) return;
#if T_PLATFORM_WINDOWS
    Sleep((DWORD)ms);
#else
    struct timespec req, rem;
    req.tv_sec = ms / 1000;
    req.tv_nsec = (ms % 1000) * 1000000L;
    while (nanosleep(&req, &rem) != 0) {
        if (errno != EINTR) break;
        req = rem;
    }
#endif
}

T_ALWAYS_INLINE void t_time_sleep_us(int64_t us) {
    if (us <= 0) return;
#if T_PLATFORM_WINDOWS
    Sleep((DWORD)(us / 1000));
#else
    struct timespec req, rem;
    req.tv_sec = us / 1000000;
    req.tv_nsec = (us % 1000000) * 1000L;
    while (nanosleep(&req, &rem) != 0) {
        if (errno != EINTR) break;
        req = rem;
    }
#endif
}

#endif /* T_TIME_H */
