#include "t_test.h"
#include "t_shutdown.h"
#include "t_signal.h"
#include "t_evloop.h"
#include <signal.h>
#include <unistd.h>
#include <sys/types.h>

T_TEST(shutdown_ctx_init) {
    t_evloop *loop = t_evloop_create();
    t_shutdown_ctx ctx;
    ctx.loop = loop;
    ctx.admin = NULL;
    ctx.config = NULL;
    t_shutdown_init(&ctx);
    t_evloop_destroy(loop);
}

T_TEST(shutdown_signal_stops_loop) {
    t_signal_install();
    t_evloop *loop = t_evloop_create();
    kill(getpid(), SIGINT);
    T_ASSERT(t_signal_is_shutdown());
    t_evloop_destroy(loop);
}

int main(void) {
    return t_run_all_tests();
}
