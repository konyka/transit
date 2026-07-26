#if defined(__APPLE__)
#define T_ASM_SYM(name) _##name
#else
#define T_ASM_SYM(name) name
#endif

    .text
    .globl T_ASM_SYM(t_coro_switch)
#if defined(__ELF__)
    .type T_ASM_SYM(t_coro_switch), @function
#endif
    .align 16

T_ASM_SYM(t_coro_switch):
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
    .size T_ASM_SYM(t_coro_switch), . - T_ASM_SYM(t_coro_switch)
#endif

    .globl T_ASM_SYM(t_coro_trampoline)
#if defined(__ELF__)
    .type T_ASM_SYM(t_coro_trampoline), @function
#endif
    .align 16

T_ASM_SYM(t_coro_trampoline):
    movq %rbx, %rdi
    callq T_ASM_SYM(t_coro_wrapper)
    ud2

#if defined(__ELF__)
    .size T_ASM_SYM(t_coro_trampoline), . - T_ASM_SYM(t_coro_trampoline)
    .section .note.GNU-stack,"",@progbits
#endif
