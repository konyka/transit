#include "t_shutdown.h"
#include "t_signal.h"
#include "t_log.h"
#include <stdio.h>

static t_shutdown_ctx g_shutdown;

void t_shutdown_init(t_shutdown_ctx *ctx) {
    g_shutdown = *ctx;
}

void t_shutdown_on_signal(t_evloop *loop) {
    if (!t_signal_is_shutdown()) return;
    T_LOG_INFO("shutdown: signal received, stopping event loop");
    if (g_shutdown.admin) t_admin_stop(g_shutdown.admin);
    t_evloop_stop(loop ? loop : g_shutdown.loop);
}
