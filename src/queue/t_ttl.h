#ifndef T_TTL_H
#define T_TTL_H

#include <stdint.h>
#include <stddef.h>

typedef struct t_ttl_entry {
    uint64_t msg_id;
    uint64_t expire_at;
    char    *topic;
    uint8_t *payload;
    size_t   payload_len;
} t_ttl_entry;

typedef struct t_ttl t_ttl;

typedef void (*t_ttl_expire_cb)(const t_ttl_entry *entry, void *ud);

t_ttl *t_ttl_create(t_ttl_expire_cb cb, void *ud);
void   t_ttl_destroy(t_ttl *ttl);

int  t_ttl_add(t_ttl *ttl, uint64_t msg_id, const char *topic, const uint8_t *payload, size_t len, uint64_t expire_at);
int  t_ttl_remove(t_ttl *ttl, uint64_t msg_id);

/* Returns expired count, or (size_t)-1 if a callback destroyed ttl. */
size_t t_ttl_expire(t_ttl *ttl, uint64_t now);

size_t t_ttl_count(const t_ttl *ttl);
int    t_ttl_is_expired(const t_ttl *ttl, uint64_t msg_id, uint64_t now);

#endif
