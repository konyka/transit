#ifndef T_SHUTDOWN_H
#define T_SHUTDOWN_H

#include "t_evloop.h"
#include "t_admin.h"
#include "t_config.h"

typedef struct {
    t_evloop *loop;
    t_admin  *admin;
    t_config *config;
} t_shutdown_ctx;

void t_shutdown_init(t_shutdown_ctx *ctx);
void t_shutdown_on_signal(t_evloop *loop);

#endif
