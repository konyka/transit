#include "t_rwlock.h"
#include "t_compiler.h"

#if T_PLATFORM_WINDOWS
    #include <windows.h>
#else
    #include <pthread.h>
#endif

void t_rwlock_init(t_rwlock *rw) {
#if T_PLATFORM_WINDOWS
    InitializeSRWLock(rw);
#else
    pthread_rwlock_init(rw, NULL);
#endif
}

void t_rwlock_destroy(t_rwlock *rw) {
#if !T_PLATFORM_WINDOWS
    pthread_rwlock_destroy(rw);
#endif
}

void t_rwlock_read_lock(t_rwlock *rw) {
#if T_PLATFORM_WINDOWS
    AcquireSRWLockShared(rw);
#else
    pthread_rwlock_rdlock(rw);
#endif
}

void t_rwlock_read_unlock(t_rwlock *rw) {
#if T_PLATFORM_WINDOWS
    ReleaseSRWLockShared(rw);
#else
    pthread_rwlock_unlock(rw);
#endif
}

void t_rwlock_write_lock(t_rwlock *rw) {
#if T_PLATFORM_WINDOWS
    AcquireSRWLockExclusive(rw);
#else
    pthread_rwlock_wrlock(rw);
#endif
}

void t_rwlock_write_unlock(t_rwlock *rw) {
#if T_PLATFORM_WINDOWS
    ReleaseSRWLockExclusive(rw);
#else
    pthread_rwlock_unlock(rw);
#endif
}
