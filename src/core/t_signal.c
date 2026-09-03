#include "t_signal.h"
#include <signal.h>

static volatile sig_atomic_t t_shutdown_flag;
static int t_signal_installed;

static void on_signal(int sig) {
    (void)sig;
    t_shutdown_flag = 1;
#if T_PLATFORM_WINDOWS
    /* MSVC resets the CRT handler to SIG_DFL after delivery. */
    (void)signal(sig, on_signal);
#endif
}

#if T_PLATFORM_WINDOWS

#include <windows.h>

static void (*t_old_sigint)(int);
static void (*t_old_sigterm)(int);

static BOOL WINAPI on_console(DWORD type) {
    if (type == CTRL_C_EVENT || type == CTRL_BREAK_EVENT || type == CTRL_CLOSE_EVENT) {
        t_shutdown_flag = 1;
        return TRUE;
    }
    return FALSE;
}

void t_signal_install(void) {
    t_shutdown_flag = 0;
    if (!t_signal_installed) {
        t_old_sigint = signal(SIGINT, on_signal);
        t_old_sigterm = signal(SIGTERM, on_signal);
        (void)SetConsoleCtrlHandler(on_console, TRUE);
        t_signal_installed = 1;
    } else {
        (void)signal(SIGINT, on_signal);
        (void)signal(SIGTERM, on_signal);
    }
}

void t_signal_uninstall(void) {
    if (!t_signal_installed) return;
    (void)signal(SIGINT, t_old_sigint ? t_old_sigint : SIG_DFL);
    (void)signal(SIGTERM, t_old_sigterm ? t_old_sigterm : SIG_DFL);
    (void)SetConsoleCtrlHandler(on_console, FALSE);
    t_signal_installed = 0;
}

#else /* POSIX */

#include <unistd.h>

static struct sigaction t_old_sigint;
static struct sigaction t_old_sigterm;
static void (*t_old_sigpipe)(int);

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

#endif /* T_PLATFORM_WINDOWS */

int t_signal_is_shutdown(void) {
    return (int)t_shutdown_flag;
}
