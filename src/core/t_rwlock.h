#ifndef T_RWLOCK_H
#define T_RWLOCK_H

#include "t_compiler.h"

#if T_PLATFORM_WINDOWS
    #include <windows.h>
    typedef SRWLOCK t_rwlock;
#else
    #include <pthread.h>
    typedef pthread_rwlock_t t_rwlock;
#endif

void t_rwlock_init(t_rwlock *rw);
void t_rwlock_destroy(t_rwlock *rw);
void t_rwlock_read_lock(t_rwlock *rw);
void t_rwlock_read_unlock(t_rwlock *rw);
void t_rwlock_write_lock(t_rwlock *rw);
void t_rwlock_write_unlock(t_rwlock *rw);

#endif
