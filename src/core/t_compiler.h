#ifndef T_COMPILER_H
#define T_COMPILER_H

/* Include basic helpers for offsetof/size constants */
#include <stddef.h>

/* Compiler detection */
#if defined(__clang__)
    #define T_COMPILER_CLANG 1
    #define T_COMPILER_GCC 0
    #define T_COMPILER_MSVC 0
#elif defined(__GNUC__)
    #define T_COMPILER_CLANG 0
    #define T_COMPILER_GCC 1
    #define T_COMPILER_MSVC 0
#elif defined(_MSC_VER)
    #define T_COMPILER_CLANG 0
    #define T_COMPILER_GCC 0
    #define T_COMPILER_MSVC 1
#else
    #define T_COMPILER_CLANG 0
    #define T_COMPILER_GCC 0
    #define T_COMPILER_MSVC 0
#endif

/* Platform detection */
#if defined(__linux__)
    #define T_PLATFORM_LINUX 1
#else
    #define T_PLATFORM_LINUX 0
#endif
#if defined(__APPLE__) && defined(__MACH__)
    #define T_PLATFORM_MACOS 1
#else
    #define T_PLATFORM_MACOS 0
#endif
#if defined(_WIN32)
    #define T_PLATFORM_WINDOWS 1
    /* Must precede windows.h so it does not pull winsock.h before winsock2.h. */
#ifndef WIN32_LEAN_AND_MEAN
    #define WIN32_LEAN_AND_MEAN
#endif
#else
    #define T_PLATFORM_WINDOWS 0
#endif

/* Architecture detection */
#if defined(__x86_64__) || defined(_M_X64)
    #define T_ARCH_X64 1
#elif defined(__aarch64__) || defined(_M_ARM64)
    #define T_ARCH_ARM64 1
#else
    #define T_ARCH_UNKNOWN 1
#endif

/* C standard version */
#if defined(__STDC_VERSION__)
    #if __STDC_VERSION__ >= 202311L
        #define T_C23 1
        #define T_C11 1
        #define T_C99 1
    #elif __STDC_VERSION__ >= 201112L
        #define T_C23 0
        #define T_C11 1
        #define T_C99 1
    #elif __STDC_VERSION__ >= 199901L
        #define T_C23 0
        #define T_C11 0
        #define T_C99 1
    #else
        #define T_C23 0
        #define T_C11 0
        #define T_C99 0
    #endif
#else
    #define T_C23 0
    #define T_C11 0
    #define T_C99 0
#endif

/* Inline keyword (C99 compatible) */
#if T_C99
    #define T_INLINE static inline
#else
    #define T_INLINE static
#endif

/* Always inline */
#if T_COMPILER_GCC || T_COMPILER_CLANG
    #define T_ALWAYS_INLINE __attribute__((always_inline)) static inline
    #define T_NEVER_INLINE __attribute__((noinline))
    #define T_UNUSED __attribute__((unused))
    #define T_NORETURN __attribute__((noreturn))
    #define T_LIKELY(x) __builtin_expect(!!(x), 1)
    #define T_UNLIKELY(x) __builtin_expect(!!(x), 0)
    #define T_ALIGNED(n) __attribute__((aligned(n)))
    #define T_PACKED __attribute__((packed))
    #define T_WEAK __attribute__((weak))
    #define T_FORCE_INLINE __attribute__((always_inline)) inline
#elif T_COMPILER_MSVC
    #define T_ALWAYS_INLINE __forceinline static
    #define T_NEVER_INLINE __declspec(noinline)
    #define T_UNUSED
    #define T_NORETURN __declspec(noreturn)
    #define T_LIKELY(x) (x)
    #define T_UNLIKELY(x) (x)
    #define T_ALIGNED(n) __declspec(align(n))
    #define T_PACKED
    #define T_WEAK
    #define T_FORCE_INLINE __forceinline
#else
    #define T_ALWAYS_INLINE static inline
    #define T_NEVER_INLINE
    #define T_UNUSED
    #define T_NORETURN
    #define T_LIKELY(x) (x)
    #define T_UNLIKELY(x) (x)
    #define T_ALIGNED(n)
    #define T_PACKED
    #define T_WEAK
    #define T_FORCE_INLINE inline
#endif

/* Branch prediction hints */
#define T_PREFETCH(addr) __builtin_prefetch(addr)

/* Compile-time assertion */
#if T_C11
    #define T_STATIC_ASSERT(cond, msg) _Static_assert(cond, msg)
#else
    #define T_STATIC_ASSERT(cond, msg) typedef char t_static_assert_##msg[(cond) ? 1 : -1]
#endif

/* Container of macro */
#define T_CONTAINER_OF(ptr, type, member) \
    ((type *)((char *)(ptr) - offsetof(type, member)))

/* Array size */
#define T_ARRAY_SIZE(arr) (sizeof(arr) / sizeof((arr)[0]))

/* Min/Max */
#define T_MIN(a, b) ((a) < (b) ? (a) : (b))
#define T_MAX(a, b) ((a) > (b) ? (a) : (b))

/* Bit manipulation helpers */
#define T_BITS_SET(x, mask)    ((x) | (mask))
#define T_BITS_CLEAR(x, mask)  ((x) & ~(mask))
#define T_BITS_TOGGLE(x, mask) ((x) ^ (mask))
#define T_BITS_TEST(x, mask)   (((x) & (mask)) != 0)
#define T_ROUND_UP(x, align)   (((x) + (align) - 1) & ~((align) - 1))
#define T_ROUND_DOWN(x, align) ((x) & ~((align) - 1))

#endif /* T_COMPILER_H */
