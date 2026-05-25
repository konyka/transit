#ifndef T_SPINLOCK_H
#define T_SPINLOCK_H

#include "t_atomic.h"

typedef struct t_spinlock {
    t_atomic_int locked;
} t_spinlock;

#define T_SPINLOCK_INIT { T_ATOMIC_INIT(0) }

void t_spinlock_init(t_spinlock *s);
void t_spinlock_lock(t_spinlock *s);
void t_spinlock_unlock(t_spinlock *s);
int t_spinlock_trylock(t_spinlock *s);

#endif
