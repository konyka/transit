#include "t_coro.h"
#include <stdlib.h>
#include <string.h>
#include <stdint.h>

#if defined(T_ARCH_X64) || defined(T_ARCH_ARM64)
extern void t_coro_switch(void **from_sp, void **to_sp);
extern void t_coro_trampoline(void);
#else
static void t_coro_switch(void **from_sp, void **to_sp) {
    (void)from_sp;
    (void)to_sp;
}
#endif

static __thread t_coro *g_current_coro = NULL;

struct t_coro {
    t_coro_fn    fn;
    void        *arg;
    void        *stack;
    size_t       stack_size;
    void        *saved_sp;
    t_coro_state state;
    t_coro      *caller;
};

void t_coro_wrapper(t_coro *coro) {
    coro->state = T_CORO_RUNNING;
    coro->fn(coro->arg);
    coro->state = T_CORO_DEAD;
    t_coro_switch(&coro->saved_sp, &coro->caller->saved_sp);
}

static void *coro_init_stack(void *stack, size_t stack_size, t_coro *coro) {
    char *base = (char *)stack;
    char *top = (char *)((uintptr_t)(base + stack_size) & ~(uintptr_t)15);
#if defined(T_ARCH_X64)
    /* retaddr + 6 callee-saved GPRs (rbx, rbp, r12-r15). */
    if (top < base + 8 + 6 * 8) return NULL;
    top -= 8;
    *(void **)top = (void *)t_coro_trampoline;
    top -= 6 * 8;
    void **regs = (void **)top;
    regs[0] = NULL;
    regs[1] = NULL;
    regs[2] = NULL;
    regs[3] = NULL;
    regs[4] = NULL;
    regs[5] = (void *)coro; /* rbx */
    return top;
#elif defined(T_ARCH_ARM64)
    /* 6 stp pairs: x19-x28, x29/x30. x19 holds coro; x30 is trampoline. */
    if (top < base + 96) return NULL;
    top -= 96;
    ((void **)(top + 0))[0] = NULL;
    ((void **)(top + 0))[1] = (void *)t_coro_trampoline;
    ((void **)(top + 16))[0] = NULL;
    ((void **)(top + 16))[1] = NULL;
    ((void **)(top + 32))[0] = NULL;
    ((void **)(top + 32))[1] = NULL;
    ((void **)(top + 48))[0] = NULL;
    ((void **)(top + 48))[1] = NULL;
    ((void **)(top + 64))[0] = NULL;
    ((void **)(top + 64))[1] = NULL;
    ((void **)(top + 80))[0] = (void *)coro; /* x19 */
    ((void **)(top + 80))[1] = NULL;
    return top;
#else
    (void)coro;
    (void)base;
    (void)top;
    return NULL;
#endif
}

t_coro *t_coro_create(t_coro_fn fn, void *arg, size_t stack_size) {
    if (!fn || stack_size < 256) return NULL;
#if !defined(T_ARCH_X64) && !defined(T_ARCH_ARM64)
    return NULL;
#endif
    t_coro *coro = (t_coro *)calloc(1, sizeof(t_coro));
    if (!coro) return NULL;
    coro->fn = fn;
    coro->arg = arg;
    coro->stack_size = stack_size;
    coro->stack = calloc(1, stack_size);
    if (!coro->stack) { free(coro); return NULL; }
    coro->state = T_CORO_READY;
    coro->saved_sp = coro_init_stack(coro->stack, stack_size, coro);
    if (!coro->saved_sp) {
        free(coro->stack);
        free(coro);
        return NULL;
    }
    return coro;
}

static int coro_caller_is_stub(const t_coro *c) {
    return c && c->fn == NULL && c->stack == NULL;
}

int t_coro_destroy(t_coro *coro) {
    if (!coro) return -1;
    /* Refuse live coroutines — freeing the stack would UAF on resume/yield. */
    if (coro->state == T_CORO_RUNNING || coro->state == T_CORO_SUSPENDED) return -1;
    if (coro_caller_is_stub(coro->caller)) {
        free(coro->caller);
        coro->caller = NULL;
    }
    if (coro->stack) free(coro->stack);
    free(coro);
    return 0;
}

int t_coro_resume(t_coro *coro) {
    if (!coro || coro->state == T_CORO_DEAD || coro->state == T_CORO_RUNNING) return -1;
    t_coro *prev = g_current_coro;
    if (prev) {
        coro->caller = prev;
    } else if (!coro->caller) {
        /* Stable main stub: keep across yields so nested callers survive. */
        coro->caller = (t_coro *)calloc(1, sizeof(t_coro));
        if (!coro->caller) return -1;
        coro->caller->state = T_CORO_RUNNING;
    }
    /* else: keep existing caller (parent coro or main stub) */
    g_current_coro = coro;
    coro->state = T_CORO_RUNNING;
    t_coro_switch(&coro->caller->saved_sp, &coro->saved_sp);
    g_current_coro = prev;
    if (coro->state == T_CORO_RUNNING) coro->state = T_CORO_SUSPENDED;
    if (coro->state == T_CORO_DEAD && coro_caller_is_stub(coro->caller)) {
        free(coro->caller);
        coro->caller = NULL;
    }
    return 0;
}

int t_coro_yield(void) {
    t_coro *coro = g_current_coro;
    if (!coro) return -1;
    coro->state = T_CORO_SUSPENDED;
    t_coro_switch(&coro->saved_sp, &coro->caller->saved_sp);
    coro->state = T_CORO_RUNNING;
    return 0;
}

t_coro_state t_coro_get_state(const t_coro *coro) {
    return coro ? coro->state : T_CORO_DEAD;
}

t_coro *t_coro_current(void) {
    return g_current_coro;
}

void *t_coro_get_arg(const t_coro *coro) {
    return coro ? coro->arg : NULL;
}
