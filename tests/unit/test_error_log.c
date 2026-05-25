#include "t_test.h"
#include "t_error.h"
#include "t_log.h"

T_TEST(error_code_ok) {
    /* Ensure the macro reports OK for a null-like error */
    t_error e = T_ERROR_NULL;
    T_ASSERT(T_OK(e));
}

T_TEST(error_code_strings) {
    T_ASSERT_STR_EQ(t_error_code_str(T_OK_CODE), "OK");
    T_ASSERT_STR_EQ(t_error_code_str(T_ERR_NOMEM), "ENOMEM");
    T_ASSERT_STR_EQ(t_error_code_str(T_ERR_TIMEOUT), "ETIMOUT");
    T_ASSERT_STR_EQ(t_error_code_str(T_ERR_IO), "EIO");
    T_ASSERT_STR_EQ(t_error_code_str(-999), "EUNKNOWN");
}

T_TEST(error_macro) {
    t_error err = T_ERROR(T_ERR_NOMEM, "out of memory");
    T_ASSERT(T_FAIL(err));
    T_ASSERT_EQ(err.code, T_ERR_NOMEM);
    T_ASSERT_STR_EQ(err.message, "out of memory");
    T_ASSERT(err.file != NULL);
    T_ASSERT(err.line > 0);
    T_ASSERT(err.func != NULL);
}

T_TEST(error_ok_check) {
    t_error ok = { T_OK_CODE, "success", __FILE__, __LINE__, __func__ };
    T_ASSERT(T_OK(ok));
    T_ASSERT(!T_FAIL(ok));
}

T_TEST(log_init_shutdown) {
    t_log_init(T_LOG_TRACE);
    T_ASSERT_EQ((int)t_log_get_level(), (int)T_LOG_TRACE);
    t_log_set_level(T_LOG_WARN);
    T_ASSERT_EQ((int)t_log_get_level(), (int)T_LOG_WARN);
    t_log_shutdown();
}

T_TEST(log_level_output) {
    t_log_init(T_LOG_TRACE);
    /* These should all output without crashing */
    T_LOG_TRACE("trace message %d", 1);
    T_LOG_DEBUG("debug message %d", 2);
    T_LOG_INFO("info message %d", 3);
    T_LOG_WARN("warn message %d", 4);
    T_LOG_ERROR("error message %d", 5);
    t_log_set_level(T_LOG_ERROR);
    /* These should be suppressed */
    T_LOG_TRACE("should not appear");
    T_LOG_DEBUG("should not appear");
    T_LOG_INFO("should not appear");
    T_LOG_WARN("should not appear");
    /* This should appear */
    T_LOG_ERROR("should appear");
    t_log_shutdown();
}

int main(void) {
    return t_run_all_tests();
}
