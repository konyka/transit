#ifndef T_ATOMIC_H
#define T_ATOMIC_H

#include "t_compiler.h"

/* Memory order enumeration */
typedef enum t_memory_order {
    T_MEMORY_RELAXED,
    T_MEMORY_ACQUIRE,
    T_MEMORY_RELEASE,
    T_MEMORY_ACQ_REL,
    T_MEMORY_SEQ_CST
} t_memory_order;

/* Real cl.exe: Interlocked first. /std:c11 sets T_C11, and VS 2026 may
 * omit __STDC_NO_ATOMICS__ while stdatomic still needs /experimental:c11atomics.
 * clang-cl has __clang__ and keeps the C11 path below. */
#if defined(_MSC_VER) && !defined(__clang__)
    #include <intrin.h>

    typedef volatile int t_atomic_int;
    typedef volatile unsigned int t_atomic_uint;
    typedef volatile long t_atomic_long;
    typedef volatile unsigned long t_atomic_ulong;
    typedef void *volatile t_atomic_ptr;

    #define T_ATOMIC_INIT(val) (val)

    T_ALWAYS_INLINE int t_atomic_load_int(t_atomic_int *a) {
        return (int)_InterlockedCompareExchange((volatile long *)a, 0, 0);
    }
    T_ALWAYS_INLINE void t_atomic_store_int(t_atomic_int *a, int val) {
        (void)_InterlockedExchange((volatile long *)a, (long)val);
    }
    T_ALWAYS_INLINE int t_atomic_add_fetch_int(t_atomic_int *a, int val) {
        return (int)_InterlockedExchangeAdd((volatile long *)a, (long)val) + val;
    }
    T_ALWAYS_INLINE int t_atomic_fetch_add_int(t_atomic_int *a, int val) {
        return (int)_InterlockedExchangeAdd((volatile long *)a, (long)val);
    }
    T_ALWAYS_INLINE int t_atomic_sub_fetch_int(t_atomic_int *a, int val) {
        return (int)_InterlockedExchangeAdd((volatile long *)a, -(long)val) - val;
    }
    T_ALWAYS_INLINE int t_atomic_cas_int(t_atomic_int *a, int expected, int desired) {
        return _InterlockedCompareExchange((volatile long *)a, (long)desired,
                                           (long)expected) == (long)expected;
    }
    T_ALWAYS_INLINE void *t_atomic_load_ptr(t_atomic_ptr *a) {
        return _InterlockedCompareExchangePointer((void *volatile *)a, NULL, NULL);
    }
    T_ALWAYS_INLINE void t_atomic_store_ptr(t_atomic_ptr *a, void *val) {
        (void)_InterlockedExchangePointer((void *volatile *)a, val);
    }
    T_ALWAYS_INLINE int t_atomic_cas_ptr(t_atomic_ptr *a, void *expected, void *desired) {
        return _InterlockedCompareExchangePointer((void *volatile *)a, desired,
                                                  expected) == expected;
    }
    T_ALWAYS_INLINE int t_atomic_compare_exchange_int(t_atomic_int *a, int *expected,
                                                      int desired) {
        long want = (long)*expected;
        long cur = _InterlockedCompareExchange((volatile long *)a, (long)desired, want);
        if (cur == want) return 1;
        *expected = (int)cur;
        return 0;
    }

#elif T_C11 && !defined(__STDC_NO_ATOMICS__)
    /* Use C11 stdatomic */
    #include <stdatomic.h>
    
    typedef _Atomic int t_atomic_int;
    typedef _Atomic unsigned int t_atomic_uint;
    typedef _Atomic long t_atomic_long;
    typedef _Atomic unsigned long t_atomic_ulong;
    typedef _Atomic(void *) t_atomic_ptr;
    
    #define T_ATOMIC_INIT(val) (val)
    
    T_ALWAYS_INLINE int t_atomic_load_int(t_atomic_int *a) {
        return atomic_load_explicit(a, memory_order_seq_cst);
    }
    T_ALWAYS_INLINE int t_atomic_load_explicit_int(t_atomic_int *a, t_memory_order mo) {
        memory_order c_mo = (mo == T_MEMORY_RELAXED) ? memory_order_relaxed :
                            (mo == T_MEMORY_ACQUIRE) ? memory_order_acquire :
                            (mo == T_MEMORY_RELEASE) ? memory_order_release :
                            (mo == T_MEMORY_ACQ_REL) ? memory_order_acq_rel :
                            memory_order_seq_cst;
        return atomic_load_explicit(a, c_mo);
    }
    T_ALWAYS_INLINE void t_atomic_store_int(t_atomic_int *a, int val) {
        atomic_store_explicit(a, val, memory_order_seq_cst);
    }
    T_ALWAYS_INLINE int t_atomic_add_fetch_int(t_atomic_int *a, int val) {
        return atomic_fetch_add_explicit(a, val, memory_order_seq_cst) + val;
    }
    T_ALWAYS_INLINE int t_atomic_fetch_add_int(t_atomic_int *a, int val) {
        return atomic_fetch_add_explicit(a, val, memory_order_seq_cst);
    }
    T_ALWAYS_INLINE int t_atomic_sub_fetch_int(t_atomic_int *a, int val) {
        return atomic_fetch_sub_explicit(a, val, memory_order_seq_cst) - val;
    }
    T_ALWAYS_INLINE int t_atomic_cas_int(t_atomic_int *a, int expected, int desired) {
        int exp = expected;
        return atomic_compare_exchange_strong_explicit(a, &exp, desired, memory_order_seq_cst, memory_order_seq_cst);
    }
    /* Compatibility helper expected by spinlocks: update *expected on failure */
    T_ALWAYS_INLINE int t_atomic_compare_exchange_int(t_atomic_int *a, int *expected, int desired) {
        return atomic_compare_exchange_strong_explicit(a, expected, desired, memory_order_seq_cst, memory_order_seq_cst);
    }
    
    T_ALWAYS_INLINE void* t_atomic_load_ptr(t_atomic_ptr *a) {
        return atomic_load_explicit(a, memory_order_seq_cst);
    }
    T_ALWAYS_INLINE void t_atomic_store_ptr(t_atomic_ptr *a, void *val) {
        atomic_store_explicit(a, val, memory_order_seq_cst);
    }
    T_ALWAYS_INLINE int t_atomic_cas_ptr(t_atomic_ptr *a, void *expected, void *desired) {
        void *exp = expected;
        return atomic_compare_exchange_strong_explicit(a, &exp, desired, memory_order_seq_cst, memory_order_seq_cst);
    }

/* GCC __sync builtin fallback */
#else
    typedef volatile int t_atomic_int;
    typedef volatile unsigned int t_atomic_uint;
    typedef volatile long t_atomic_long;
    typedef volatile unsigned long t_atomic_ulong;
    typedef void* volatile t_atomic_ptr;

    #define T_ATOMIC_INIT(val) (val)

    T_ALWAYS_INLINE int t_atomic_load_int(t_atomic_int *a) {
        return __sync_fetch_and_add(a, 0);
    }
    T_ALWAYS_INLINE void t_atomic_store_int(t_atomic_int *a, int val) {
        __sync_lock_test_and_set(a, val);
    }
    T_ALWAYS_INLINE int t_atomic_add_fetch_int(t_atomic_int *a, int val) {
        return __sync_add_and_fetch(a, val);
    }
    T_ALWAYS_INLINE int t_atomic_fetch_add_int(t_atomic_int *a, int val) {
        return __sync_fetch_and_add(a, val);
    }
    T_ALWAYS_INLINE int t_atomic_sub_fetch_int(t_atomic_int *a, int val) {
        return __sync_sub_and_fetch(a, val);
    }
    T_ALWAYS_INLINE int t_atomic_cas_int(t_atomic_int *a, int expected, int desired) {
        return __sync_bool_compare_and_swap(a, expected, desired);
    }

    T_ALWAYS_INLINE void* t_atomic_load_ptr(t_atomic_ptr *a) {
        return __atomic_load_n(a, __ATOMIC_SEQ_CST);
    }
    T_ALWAYS_INLINE void t_atomic_store_ptr(t_atomic_ptr *a, void *val) {
        __atomic_store_n(a, val, __ATOMIC_SEQ_CST);
    }
    T_ALWAYS_INLINE int t_atomic_cas_ptr(t_atomic_ptr *a, void *expected, void *desired) {
        return __sync_bool_compare_and_swap(a, expected, desired);
    }
    T_ALWAYS_INLINE int t_atomic_compare_exchange_int(t_atomic_int *a, int *expected, int desired) {
        int cur = __sync_val_compare_and_swap(a, *expected, desired);
        if (cur == *expected) return 1;
        *expected = cur;
        return 0;
    }
#endif

#endif /* T_ATOMIC_H */
