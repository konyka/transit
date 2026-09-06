#include "t_spinlock.h"
#include "t_atomic.h"

#if T_PLATFORM_WINDOWS
#include <windows.h>
#else
#include <sched.h>
#endif

static inline void t_spin_pause(void) {
#if T_PLATFORM_WINDOWS
    YieldProcessor();
#elif defined(__i386__) || defined(__x86_64__)
    __builtin_ia32_pause();
#elif defined(__aarch64__)
    __asm__ __volatile__("yield");
#else
    sched_yield();
#endif
}

void t_spinlock_init(t_spinlock *s) {
    t_atomic_store_int(&s->locked, 0);
}

void t_spinlock_lock(t_spinlock *s) {
    int expected = 0;
    while (!t_atomic_compare_exchange_int(&s->locked, &expected, 1)) {
        expected = 0;
        t_spin_pause();
    }
}

int t_spinlock_trylock(t_spinlock *s) {
    int expected = 0;
    return t_atomic_compare_exchange_int(&s->locked, &expected, 1) ? 1 : 0;
}

void t_spinlock_unlock(t_spinlock *s) {
    t_atomic_store_int(&s->locked, 0);
}
