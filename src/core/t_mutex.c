#include "t_mutex.h"
#include "t_compiler.h"

#if T_PLATFORM_WINDOWS
    #include <windows.h>
    // SRWLOCK already provides init semantics via InitializeSRWLock
#else
    #include <pthread.h>
#endif

void t_mutex_init(t_mutex *m) {
#if T_PLATFORM_WINDOWS
    InitializeSRWLock(m);
#else
    pthread_mutex_init(m, NULL);
#endif
}

void t_mutex_destroy(t_mutex *m) {
#if !T_PLATFORM_WINDOWS
    pthread_mutex_destroy(m);
#else
    // No destroy needed for SRWLOCK
#endif
}

void t_mutex_lock(t_mutex *m) {
#if T_PLATFORM_WINDOWS
    AcquireSRWLockExclusive(m);
#else
    pthread_mutex_lock(m);
#endif
}

void t_mutex_unlock(t_mutex *m) {
#if T_PLATFORM_WINDOWS
    ReleaseSRWLockExclusive(m);
#else
    pthread_mutex_unlock(m);
#endif
}

int t_mutex_trylock(t_mutex *m) {
#if T_PLATFORM_WINDOWS
    return (int)TryAcquireSRWLockExclusive(m);
#else
    return pthread_mutex_trylock(m);
#endif
}
