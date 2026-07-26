    .text
    .globl t_coro_switch
#if defined(__ELF__)
    .type t_coro_switch, @function
#endif
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

#if defined(__ELF__)
    .size t_coro_switch, . - t_coro_switch
#endif

    .globl t_coro_trampoline
#if defined(__ELF__)
    .type t_coro_trampoline, @function
#endif
    .align 16

t_coro_trampoline:
    movq %rbx, %rdi
    callq t_coro_wrapper
    ud2

#if defined(__ELF__)
    .size t_coro_trampoline, . - t_coro_trampoline
    .section .note.GNU-stack,"",@progbits
#endif
