#ifndef T_DOMAIN_H
#define T_DOMAIN_H

#include "t_compiler.h"
#include <stdint.h>
#include <stddef.h>

typedef struct t_domain t_domain;

/* Domain lifecycle */
t_domain *t_domain_create(const char *name);
void      t_domain_destroy(t_domain *domain);
const char *t_domain_name(const t_domain *domain);

/* Queue management within a domain */
int      t_domain_create_queue(t_domain *domain, const char *queue_name, int type, int flags);
int      t_domain_delete_queue(t_domain *domain, const char *queue_name);
size_t   t_domain_queue_count(const t_domain *domain);
void    *t_domain_get_queue(t_domain *domain, const char *queue_name);

/* Publish/subscribe within a domain */
int      t_domain_publish(t_domain *domain, const char *queue_name,
                          const uint8_t *data, size_t len, int priority);
int      t_domain_subscribe(t_domain *domain, const char *queue_name,
                            void (*cb)(const char *, const uint8_t *, size_t, void *), void *ud);

/* Stats */
size_t   t_domain_total_messages(const t_domain *domain);
size_t   t_domain_total_delivered(const t_domain *domain);

#endif /* T_DOMAIN_H */
