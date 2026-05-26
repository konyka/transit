    .text
    .globl t_coro_switch
    .type t_coro_switch, @function
    .align 16

t_coro_switch:
    pushq %rbx
    pushq %rbp
    pushq %r12
    pushq %r13
    pushq %r14
    pushq %r15
    movq %rsp, (%rdi)
    movq (%rsi), %rsp
    popq %r15
    popq %r14
    popq %r13
    popq %r12
    popq %rbp
    popq %rbx
    ret

    .size t_coro_switch, . - t_coro_switch

    .globl t_coro_trampoline
    .type t_coro_trampoline, @function
    .align 16

t_coro_trampoline:
    movq %rbx, %rdi
    callq t_coro_wrapper
    ud2

    .size t_coro_trampoline, . - t_coro_trampoline
