#include "t_signal.h"
#include <signal.h>
#include <unistd.h>

static volatile sig_atomic_t t_shutdown_flag;
static int t_signal_installed;
static struct sigaction t_old_sigint;
static struct sigaction t_old_sigterm;
static void (*t_old_sigpipe)(int);

static void on_signal(int sig) {
    (void)sig;
    t_shutdown_flag = 1;
}

void t_signal_install(void) {
    t_shutdown_flag = 0;
    struct sigaction sa;
    sa.sa_handler = on_signal;
    sigemptyset(&sa.sa_mask);
    sa.sa_flags = 0;
    if (!t_signal_installed) {
        sigaction(SIGINT, &sa, &t_old_sigint);
        sigaction(SIGTERM, &sa, &t_old_sigterm);
        t_old_sigpipe = signal(SIGPIPE, SIG_IGN);
        t_signal_installed = 1;
    } else {
        /* Reinstall our handlers without overwriting the saved originals. */
        sigaction(SIGINT, &sa, NULL);
        sigaction(SIGTERM, &sa, NULL);
        (void)signal(SIGPIPE, SIG_IGN);
    }
}

void t_signal_uninstall(void) {
    if (!t_signal_installed) return;
    sigaction(SIGINT, &t_old_sigint, NULL);
    sigaction(SIGTERM, &t_old_sigterm, NULL);
    (void)signal(SIGPIPE, t_old_sigpipe ? t_old_sigpipe : SIG_DFL);
    t_signal_installed = 0;
    /* Keep t_shutdown_flag so a pending SIGINT/SIGTERM is not lost. */
}

int t_signal_is_shutdown(void) {
    return (int)t_shutdown_flag;
}
