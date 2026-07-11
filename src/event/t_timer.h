#ifndef T_TIMER_H
#define T_TIMER_H

#include "t_compiler.h"
#include <stdint.h>
#include <stddef.h>

typedef struct t_timer t_timer;
typedef void (*t_timer_fn)(void *user_data);

t_timer *t_timer_create(void);
void     t_timer_destroy(t_timer *timer);
int64_t  t_timer_add(t_timer *t, int64_t delay_ms, int64_t repeat_ms,
                     t_timer_fn fn, void *user_data);
void     t_timer_cancel(t_timer *t, int64_t id);
/* Returns ms until next expiry, 0 if due, -1 if empty.
 * Returns -2 if a callback destroyed the timer (pointer freed; do not destroy again). */
int64_t  t_timer_process(t_timer *t);
size_t   t_timer_count(const t_timer *t);

#endif /* T_TIMER_H */
