#include "t_log.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <pthread.h>

/* Internal state */
static t_log_level g_min_level = T_LOG_INFO;
static pthread_mutex_t g_log_mutex = PTHREAD_MUTEX_INITIALIZER;
static int g_initialized = 0;

static const char* level_name(t_log_level level) {
    switch (level) {
        case T_LOG_TRACE: return "TRACE";
        case T_LOG_DEBUG: return "DEBUG";
        case T_LOG_INFO:  return "INFO";
        case T_LOG_WARN:  return "WARN";
        case T_LOG_ERROR: return "ERROR";
        case T_LOG_FATAL: return "FATAL";
        default:          return "LOG";
    }
}

static const char* color_start_for(t_log_level level) {
    switch (level) {
        case T_LOG_TRACE: return "\033[90m";   /* gray */
        case T_LOG_DEBUG: return "\033[34m";   /* blue */
        case T_LOG_INFO:  return "\033[32m";   /* green */
        case T_LOG_WARN:  return "\033[33m";   /* yellow */
        case T_LOG_ERROR: return "\033[31m";   /* red */
        case T_LOG_FATAL: return "\033[1;31m"; /* bold red */
        default:          return "";
    }
}

static const char* color_reset() {
    return "\033[0m";
}

static void log_output(t_log_level level, const char *file, int line,
                       const char *func, const char *timestamp, const char *formatted) {
    const char *base = file ? file : "(unknown)";
    const char *p = strrchr(base, '/');
    if (p) base = p + 1;
    const char *lvl = level_name(level);
    const char *start = color_start_for(level);
    const char *reset = color_reset();
    char line_buf[4096];
    snprintf(line_buf, sizeof(line_buf), "%s[%s]%s %s %s:%d:%s: %s",
             start, lvl, reset, timestamp ? timestamp : "", base, line, func ? func : "(unknown)", formatted);

    pthread_mutex_lock(&g_log_mutex);
    fputs(line_buf, stderr);
    fputs("\n", stderr);
    fflush(stderr);
    pthread_mutex_unlock(&g_log_mutex);
}

void t_log_init(t_log_level min_level) {
    pthread_mutex_lock(&g_log_mutex);
    g_min_level = min_level;
    g_initialized = 1;
    pthread_mutex_unlock(&g_log_mutex);
}

void t_log_shutdown(void) {
    pthread_mutex_lock(&g_log_mutex);
    g_initialized = 0;
    pthread_mutex_unlock(&g_log_mutex);
}

void t_log_set_level(t_log_level level) {
    pthread_mutex_lock(&g_log_mutex);
    g_min_level = level;
    pthread_mutex_unlock(&g_log_mutex);
}

t_log_level t_log_get_level(void) {
    pthread_mutex_lock(&g_log_mutex);
    t_log_level level = g_min_level;
    pthread_mutex_unlock(&g_log_mutex);
    return level;
}

void t_log_write(t_log_level level, const char *file, int line,
                 const char *func, const char *fmt, ...) {
    if (!fmt) return;
    pthread_mutex_lock(&g_log_mutex);
    t_log_level min_level = g_min_level;
    pthread_mutex_unlock(&g_log_mutex);
    if (level < min_level) return;

    va_list ap;
    char msg[1024];
    va_start(ap, fmt);
    vsnprintf(msg, sizeof(msg), fmt, ap);
    va_end(ap);

    char timestamp[32];
    struct timespec ts;
    struct tm tm;
    if (clock_gettime(CLOCK_REALTIME, &ts) != 0 ||
        !localtime_r(&ts.tv_sec, &tm)) {
        snprintf(timestamp, sizeof(timestamp), "??:??:??.???");
    } else {
        snprintf(timestamp, sizeof(timestamp), "%02d:%02d:%02d.%03d",
                 tm.tm_hour, tm.tm_min, tm.tm_sec, (int)(ts.tv_nsec / 1000000));
    }

    log_output(level, file, line, func, timestamp, msg);
}
