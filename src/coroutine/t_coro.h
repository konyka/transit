#ifndef T_CORO_H
#define T_CORO_H

#include "t_compiler.h"
#include <stddef.h>
#include <stdint.h>

typedef struct t_coro t_coro;
typedef void (*t_coro_fn)(void *arg);

typedef enum t_coro_state {
    T_CORO_DEAD = 0,
    T_CORO_READY,
    T_CORO_RUNNING,
    T_CORO_SUSPENDED
} t_coro_state;

#define T_CORO_DEFAULT_STACK (64 * 1024)

/* SysV assembly frames; Windows x64 ABI is not supported. */
#if T_PLATFORM_WINDOWS
#define T_HAVE_CORO_ASM 0
#elif defined(T_ARCH_X64) || defined(T_ARCH_ARM64)
#define T_HAVE_CORO_ASM 1
#else
#define T_HAVE_CORO_ASM 0
#endif

t_coro      *t_coro_create(t_coro_fn fn, void *arg, size_t stack_size);
int          t_coro_destroy(t_coro *coro);
int          t_coro_resume(t_coro *coro);
int          t_coro_yield(void);
t_coro_state t_coro_get_state(const t_coro *coro);
t_coro      *t_coro_current(void);
void        *t_coro_get_arg(const t_coro *coro);

#endif /* T_CORO_H */
