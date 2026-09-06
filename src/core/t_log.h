#ifndef T_LOG_H
#define T_LOG_H

#include "t_compiler.h"
#include <stdarg.h>

/* Log levels */
typedef enum t_log_level {
    T_LOG_TRACE = 0,
    T_LOG_DEBUG = 1,
    T_LOG_INFO  = 2,
    T_LOG_WARN  = 3,
    T_LOG_ERROR = 4,
    T_LOG_FATAL = 5
} t_log_level;

/* Initialize/shutdown */
void t_log_init(t_log_level min_level);
void t_log_shutdown(void);

/* Set minimum log level */
void t_log_set_level(t_log_level level);

/* Get current level */
t_log_level t_log_get_level(void);

/* Core log function */
void t_log_write(t_log_level level, const char *file, int line,
                 const char *func, const char *fmt, ...)
                 T_PRINTF(5, 6);

/* Convenience macros */
#define T_LOG_TRACE(fmt, ...) \
    t_log_write(T_LOG_TRACE, __FILE__, __LINE__, __func__, fmt, ##__VA_ARGS__)
#define T_LOG_DEBUG(fmt, ...) \
    t_log_write(T_LOG_DEBUG, __FILE__, __LINE__, __func__, fmt, ##__VA_ARGS__)
#define T_LOG_INFO(fmt, ...) \
    t_log_write(T_LOG_INFO, __FILE__, __LINE__, __func__, fmt, ##__VA_ARGS__)
#define T_LOG_WARN(fmt, ...) \
    t_log_write(T_LOG_WARN, __FILE__, __LINE__, __func__, fmt, ##__VA_ARGS__)
#define T_LOG_ERROR(fmt, ...) \
    t_log_write(T_LOG_ERROR, __FILE__, __LINE__, __func__, fmt, ##__VA_ARGS__)
#define T_LOG_FATAL(fmt, ...) \
    t_log_write(T_LOG_FATAL, __FILE__, __LINE__, __func__, fmt, ##__VA_ARGS__)

#endif /* T_LOG_H */
