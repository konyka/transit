#include "t_test.h"
#include "t_signal.h"
#include <signal.h>
#include <unistd.h>
#include <sys/types.h>

T_TEST(signal_install_clears_flag) {
    t_signal_install();
    T_ASSERT(!t_signal_is_shutdown());
}

T_TEST(signal_shutdown_on_sigint) {
    t_signal_install();
    kill(getpid(), SIGINT);
    T_ASSERT(t_signal_is_shutdown());
}

T_TEST(signal_install_resets_flag) {
    kill(getpid(), SIGINT);
    t_signal_install();
    T_ASSERT(!t_signal_is_shutdown());
}

int main(void) {
    return t_run_all_tests();
}
