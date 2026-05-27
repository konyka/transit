#include "t_shutdown.h"
#include "t_signal.h"
#include "t_log.h"
#include <stdio.h>

static t_shutdown_ctx g_shutdown;

void t_shutdown_init(t_shutdown_ctx *ctx) {
    g_shutdown = *ctx;
}

void t_shutdown_on_signal(t_evloop *loop) {
    if (t_signal_is_shutdown()) {
        T_LOG_INFO("shutdown: signal received, stopping event loop");
        t_evloop_stop(loop);
    }
}
