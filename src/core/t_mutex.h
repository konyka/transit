#ifndef T_MUTEX_H
#define T_MUTEX_H

#include "t_compiler.h"

#if T_PLATFORM_WINDOWS
    #include <windows.h>
    typedef SRWLOCK t_mutex;
    #define T_MUTEX_INIT SRWLOCK_INIT
#else
    #include <pthread.h>
    typedef pthread_mutex_t t_mutex;
    #define T_MUTEX_INIT PTHREAD_MUTEX_INITIALIZER
#endif

void t_mutex_init(t_mutex *m);
void t_mutex_destroy(t_mutex *m);
void t_mutex_lock(t_mutex *m);
void t_mutex_unlock(t_mutex *m);
int t_mutex_trylock(t_mutex *m);

#endif
