#if defined(__APPLE__)
#define T_ASM_SYM(name) _##name
#else
#define T_ASM_SYM(name) name
#endif

    .text
    .globl T_ASM_SYM(t_coro_switch)
#if defined(__ELF__)
    .type T_ASM_SYM(t_coro_switch), %function
#endif
    .p2align 4

T_ASM_SYM(t_coro_switch):
    stp x19, x20, [sp, #-16]!
    stp x21, x22, [sp, #-16]!
    stp x23, x24, [sp, #-16]!
    stp x25, x26, [sp, #-16]!
    stp x27, x28, [sp, #-16]!
    stp x29, x30, [sp, #-16]!
    mov x2, sp
    str x2, [x0]
    ldr x2, [x1]
    mov sp, x2
    ldp x29, x30, [sp], #16
    ldp x27, x28, [sp], #16
    ldp x25, x26, [sp], #16
    ldp x23, x24, [sp], #16
    ldp x21, x22, [sp], #16
    ldp x19, x20, [sp], #16
    ret

#if defined(__ELF__)
    .size T_ASM_SYM(t_coro_switch), . - T_ASM_SYM(t_coro_switch)
#endif

    .globl T_ASM_SYM(t_coro_trampoline)
#if defined(__ELF__)
    .type T_ASM_SYM(t_coro_trampoline), %function
#endif
    .p2align 4

T_ASM_SYM(t_coro_trampoline):
    mov x0, x19
    bl T_ASM_SYM(t_coro_wrapper)
    brk #0

#if defined(__ELF__)
    .size T_ASM_SYM(t_coro_trampoline), . - T_ASM_SYM(t_coro_trampoline)
    .section .note.GNU-stack,"",@progbits
#endif
