#ifndef T_RATELIMIT_H
#define T_RATELIMIT_H

#include <stddef.h>
#include <stdint.h>

typedef struct t_ratelimit t_ratelimit;

t_ratelimit *t_ratelimit_create(size_t max_tokens, double refill_rate);
void         t_ratelimit_destroy(t_ratelimit *rl);

int    t_ratelimit_allow(t_ratelimit *rl, uint64_t now_ms);
size_t t_ratelimit_available(t_ratelimit *rl, uint64_t now_ms);

uint64_t t_ratelimit_total_allowed(const t_ratelimit *rl);
uint64_t t_ratelimit_total_rejected(const t_ratelimit *rl);

void t_ratelimit_reset(t_ratelimit *rl);

#endif
