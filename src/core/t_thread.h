#ifndef T_THREAD_H
#define T_THREAD_H

#include "t_compiler.h"

#if T_PLATFORM_WINDOWS
typedef struct t_thread {
    void     *os;
    unsigned  tid;
} t_thread;
#else
#include <pthread.h>
typedef struct t_thread {
    pthread_t pt;
} t_thread;
#endif

/* Spawn a joinable thread. Returns 0 on success, -1 on failure. */
int  t_thread_spawn(t_thread *th, void *(*fn)(void *), void *arg);
int  t_thread_join(t_thread *th);
void t_thread_yield(void);

#endif /* T_THREAD_H */
