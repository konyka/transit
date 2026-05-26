#include "t_signal.h"
#include <signal.h>
#include <unistd.h>

static volatile sig_atomic_t t_shutdown_flag;

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
    sigaction(SIGINT, &sa, NULL);
    sigaction(SIGTERM, &sa, NULL);
    signal(SIGPIPE, SIG_IGN);
}

int t_signal_is_shutdown(void) {
    return (int)t_shutdown_flag;
}
