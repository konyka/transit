#ifndef T_FLOWCONTROL_H
#define T_FLOWCONTROL_H

#include <stddef.h>
#include <stdint.h>

typedef struct t_flowcontrol t_flowcontrol;

t_flowcontrol *t_fc_create(size_t max_credits, int64_t refill_interval_ms);
void           t_fc_destroy(t_flowcontrol *fc);

int  t_fc_acquire(t_flowcontrol *fc, size_t count);
void t_fc_release(t_flowcontrol *fc, size_t count);
void t_fc_refill(t_flowcontrol *fc);

size_t    t_fc_available(const t_flowcontrol *fc);
size_t    t_fc_max(const t_flowcontrol *fc);
int       t_fc_is_blocked(const t_flowcontrol *fc);
uint64_t  t_fc_total_acquired(const t_flowcontrol *fc);
uint64_t  t_fc_total_released(const t_flowcontrol *fc);
uint64_t  t_fc_total_rejected(const t_flowcontrol *fc);

#endif
