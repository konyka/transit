#include "t_coro.h"
#include <stdlib.h>
#include <string.h>
#include <stdint.h>

extern void t_coro_switch(void **from_sp, void **to_sp);
extern void t_coro_trampoline(void);

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

t_coro *t_coro_create(t_coro_fn fn, void *arg, size_t stack_size) {
    if (!fn || stack_size == 0) return NULL;
    t_coro *coro = (t_coro *)calloc(1, sizeof(t_coro));
    if (!coro) return NULL;
    coro->fn = fn;
    coro->arg = arg;
    coro->stack_size = stack_size;
    coro->stack = calloc(1, stack_size);
    if (!coro->stack) { free(coro); return NULL; }
    coro->state = T_CORO_READY;

    char *sp = (char *)coro->stack + stack_size;
    sp = (char *)((uintptr_t)sp & ~(uintptr_t)15);

    sp -= 8;
    *(void **)sp = (void *)t_coro_trampoline;

    sp -= 6 * 8;
    void **regs = (void **)sp;
    regs[0] = NULL;       /* r15 */
    regs[1] = NULL;       /* r14 */
    regs[2] = NULL;       /* r13 */
    regs[3] = NULL;       /* r12 */
    regs[4] = NULL;       /* rbp */
    regs[5] = (void *)coro; /* rbx */

    coro->saved_sp = sp;
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
