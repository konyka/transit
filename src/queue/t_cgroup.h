#ifndef T_CGROUP_H
#define T_CGROUP_H

#include <stddef.h>
#include <stdint.h>

typedef struct t_cgroup t_cgroup;

typedef void (*t_cgroup_deliver_cb)(const char *topic, const uint8_t *payload, size_t len, void *ud);

t_cgroup *t_cgroup_create(const char *group_id);
void      t_cgroup_destroy(t_cgroup *cg);

int  t_cgroup_add_consumer(t_cgroup *cg, const char *consumer_id, t_cgroup_deliver_cb cb, void *ud);
int  t_cgroup_remove_consumer(t_cgroup *cg, const char *consumer_id);

int  t_cgroup_dispatch(t_cgroup *cg, const char *topic, const uint8_t *payload, size_t len);
/* O(1) RR pick. Returns the next consumer `ud`, or NULL if empty. */
void *t_cgroup_pick(t_cgroup *cg);

size_t t_cgroup_consumer_count(const t_cgroup *cg);
uint64_t t_cgroup_total_dispatched(const t_cgroup *cg);
const char *t_cgroup_id(const t_cgroup *cg);

#endif
