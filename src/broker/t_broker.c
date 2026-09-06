#include "t_broker.h"
#include "t_domain.h"
#include "t_dispatch.h"
#include "t_queue.h"
#include "t_map.h"
#include "t_wal.h"
#include "t_cluster.h"
#include "t_raft.h"
#include "t_raft_cmd.h"
#include "t_wire.h"
#include "t_endian.h"
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <stddef.h>
#include <limits.h>
#if T_PLATFORM_WINDOWS
#include <windows.h>
#else
#include <errno.h>
#include <sys/stat.h>
#endif

typedef struct t_broker {
    char *broker_id;
    t_map domains; /* map domain_name -> t_domain* */
    int running;
    int free_pending; /* destroy deferred while delivering or ext_refs > 0 */
    size_t ext_refs;  /* e.g. live t_dispatch handles */
    char *datadir;
    int wal_sync_every;
    t_cluster *cluster;
    t_raft *raft;
    int applying;
} t_broker;

static int broker_any_delivering(const t_broker *broker) {
    if (!broker) return 0;
    t_map_iter it = t_map_iter_begin((t_map *)&broker->domains);
    const char *k;
    void *v;
    while (t_map_iter_next(&it, &k, &v)) {
        if (t_domain_is_delivering((const t_domain *)v)) return 1;
    }
    return 0;
}

/* Returns 1 if broker was freed. */
static int broker_try_complete_destroy(t_broker *broker) {
    if (!broker || !broker->free_pending) return 0;
    if (broker_any_delivering(broker) || broker->ext_refs > 0) return 0;
    t_broker_destroy(broker);
    return 1;
}

/* Remove domains destroyed during fanout while still in the broker map. */
static void broker_reap_domains(t_broker *broker) {
    if (!broker) return;
    for (;;) {
        const char *victim = NULL;
        t_map_iter it = t_map_iter_begin(&broker->domains);
        const char *k;
        void *v;
        while (t_map_iter_next(&it, &k, &v)) {
            t_domain *d = (t_domain *)v;
            if (t_domain_is_free_pending(d) && !t_domain_is_delivering(d)) {
                victim = k;
                break;
            }
        }
        if (!victim) break;
        v = t_map_remove(&broker->domains, victim);
        if (!v) break;
        t_domain_set_broker_owned((t_domain *)v, 0);
        t_domain_destroy((t_domain *)v);
    }
}

static t_domain *broker_find_queue_domain(t_broker *broker, const char *queue_name) {
    t_domain *def = t_broker_get_domain(broker, "default");
    if (def && t_domain_get_queue(def, queue_name)) return def;
    t_map_iter it = t_map_iter_begin(&broker->domains);
    const char *k; void *v;
    while (t_map_iter_next(&it, &k, &v)) {
        t_domain *d = (t_domain *)v;
        if (d == def) continue;
        if (t_domain_get_queue(d, queue_name)) return d;
    }
    return NULL;
}

/* Helpers */
t_broker *t_broker_create(const char *broker_id) {
    if (!broker_id) return NULL;
    t_broker *b = (t_broker *)calloc(1, sizeof(t_broker));
    if (!b) return NULL;
    b->broker_id = strdup(broker_id);
    if (!b->broker_id) {
        free(b);
        return NULL;
    }
    t_map_init(&b->domains);
    b->running = 0;
    /* Create default domain automatically */
    t_domain *def = t_domain_create("default");
    if (!def) {
        free(b->broker_id);
        free(b);
        return NULL;
    }
    t_domain_set_broker_owned(def, 1);
    if (t_map_insert(&b->domains, "default", def) != 0) {
        t_domain_set_broker_owned(def, 0);
        t_domain_destroy(def);
        free(b->broker_id);
        free(b);
        return NULL;
    }
    t_domain_set_accepting(def, 0); /* closed until t_broker_start */
    b->wal_sync_every = 32;
    return b;
}

void t_broker_destroy(t_broker *broker) {
    if (!broker) return;
    if (broker_any_delivering(broker) || broker->ext_refs > 0) {
        broker->free_pending = 1;
        return;
    }
    broker->free_pending = 0;
    /* destroy all domains */
    t_map_iter it = t_map_iter_begin(&broker->domains);
    const char *k; void *v;
    while (t_map_iter_next(&it, &k, &v)) {
        t_domain_set_broker_owned((t_domain *)v, 0);
        t_domain_destroy((t_domain *)v);
    }
    t_map_destroy(&broker->domains);
    free(broker->datadir);
    free(broker->broker_id);
    free(broker);
}

void t_broker_retain(t_broker *broker) {
    if (broker) broker->ext_refs++;
}

void t_broker_release(t_broker *broker) {
    if (!broker || broker->ext_refs == 0) return;
    broker->ext_refs--;
    if (broker->free_pending) t_broker_destroy(broker);
}

const char *t_broker_id(const t_broker *broker) {
    return broker ? broker->broker_id : NULL;
}

int t_broker_start(t_broker *broker) {
    if (!broker || broker->free_pending) return -1;
    broker->running = 1;
    t_map_iter it = t_map_iter_begin(&broker->domains);
    const char *k; void *v;
    while (t_map_iter_next(&it, &k, &v)) {
        t_domain_set_accepting((t_domain *)v, 1);
    }
    return 0;
}

int t_broker_stop(t_broker *broker) {
    if (!broker || broker->free_pending) return -1;
    broker->running = 0;
    t_map_iter it = t_map_iter_begin(&broker->domains);
    const char *k; void *v;
    while (t_map_iter_next(&it, &k, &v)) {
        t_domain_set_accepting((t_domain *)v, 0);
    }
    return 0;
}

int t_broker_is_running(const t_broker *broker) {
    return broker ? broker->running != 0 : 0;
}

t_domain *t_broker_create_domain(t_broker *broker, const char *domain_name) {
    if (!broker || broker->free_pending || !domain_name) return NULL;
    broker_reap_domains(broker);
    if (broker_try_complete_destroy(broker)) return NULL;
    t_domain *existing = (t_domain *)t_map_get(&broker->domains, domain_name);
    if (existing) {
        /* Reject resurrecting a domain still awaiting destroy. */
        if (t_domain_is_free_pending(existing)) return NULL;
        return existing;
    }
    t_domain *d = t_domain_create(domain_name);
    if (!d) return NULL;
    t_domain_set_accepting(d, broker->running);
    t_domain_set_broker_owned(d, 1);
    if (t_map_insert(&broker->domains, domain_name, d) != 0) {
        t_domain_set_broker_owned(d, 0);
        t_domain_destroy(d);
        return NULL;
    }
    return d;
}

t_domain *t_broker_get_domain(t_broker *broker, const char *domain_name) {
    if (!broker || !domain_name) return NULL;
    return (t_domain *)t_map_get(&broker->domains, domain_name);
}

int t_broker_remove_domain(t_broker *broker, const char *domain_name) {
    if (!broker || broker->free_pending || !domain_name) return -1;
    if (strcmp(domain_name, "default") == 0) return -1;
    void *v = t_map_get(&broker->domains, domain_name);
    if (!v) return -1;
    /* Refuse while fanout holds the domain; map would otherwise lose the pointer. */
    if (t_domain_is_delivering((t_domain *)v)) return -1;
    v = t_map_remove(&broker->domains, domain_name);
    t_domain_set_broker_owned((t_domain *)v, 0);
    t_domain_destroy((t_domain *)v);
    return 0;
}

size_t t_broker_domain_count(const t_broker *broker) {
    return broker ? t_map_len(&broker->domains) : 0;
}

int t_broker_set_datadir(t_broker *broker, const char *path) {
    if (!broker || broker->free_pending || !path || !path[0]) return -1;
#if T_PLATFORM_WINDOWS
    if (!CreateDirectoryA(path, NULL)) {
        DWORD err = GetLastError();
        if (err != ERROR_ALREADY_EXISTS) return -1;
    }
    {
        DWORD attrs = GetFileAttributesA(path);
        if (attrs == INVALID_FILE_ATTRIBUTES || !(attrs & FILE_ATTRIBUTE_DIRECTORY))
            return -1;
    }
#else
    if (mkdir(path, 0700) != 0 && errno != EEXIST) return -1;
    struct stat st;
    if (stat(path, &st) != 0 || !S_ISDIR(st.st_mode)) return -1;
#endif
    char *dup = strdup(path);
    if (!dup) return -1;
    free(broker->datadir);
    broker->datadir = dup;
    return 0;
}

const char *t_broker_datadir(const t_broker *broker) {
    return broker ? broker->datadir : NULL;
}

int t_broker_set_wal_sync_every(t_broker *broker, int n) {
    if (!broker || broker->free_pending) return -1;
    broker->wal_sync_every = n < 0 ? 0 : n;
    return 0;
}

int t_broker_set_cluster(t_broker *broker, t_cluster *cluster) {
    if (!broker || broker->free_pending) return -1;
    broker->cluster = cluster;
    return 0;
}

int t_broker_is_leader(const t_broker *broker) {
    if (!broker) return 0;
    if (broker->cluster && !t_cluster_is_leader(broker->cluster)) return 0;
    if (broker->raft && t_raft_state(broker->raft) != T_NODE_LEADER) return 0;
    return 1;
}

t_cluster *t_broker_cluster(t_broker *broker) {
    return broker ? broker->cluster : NULL;
}

static size_t broker_cluster_n(const t_broker *b) {
    if (!b || !b->cluster) return 1;
    size_t n = t_cluster_node_count(b->cluster);
    uint64_t self = t_cluster_self_id(b->cluster);
    if (!t_cluster_get_node((t_cluster *)b->cluster, self)) n++;
    if (n == 0) n = 1;
    return n;
}

static void broker_on_raft(const t_raft_entry *entry, void *ud);

static int broker_snap_encode_cb(uint8_t **out, size_t *len, void *ud);
static int broker_snap_apply_cb(const uint8_t *data, size_t len, void *ud);

int t_broker_set_raft(t_broker *broker, t_raft *raft) {
    if (!broker || broker->free_pending) return -1;
    broker->raft = raft;
    if (raft) {
        t_raft_set_apply_cb(raft, broker_on_raft, broker);
        t_raft_set_snapshot_cb(raft, broker_snap_encode_cb, broker_snap_apply_cb, broker);
    }
    return 0;
}

t_raft *t_broker_raft(t_broker *broker) {
    return broker ? broker->raft : NULL;
}

static int broker_wal_path(char *out, size_t cap, const char *dir,
                           const char *domain, const char *queue) {
    if (!out || !dir || !domain || !queue) return -1;
    if (strchr(domain, '/') || strchr(queue, '/') ||
        strchr(domain, '\\') || strchr(queue, '\\')) return -1;
    int n = snprintf(out, cap, "%s/%s.%s.wal", dir, domain, queue);
    if (n < 0 || (size_t)n >= cap) return -1;
    return 0;
}

static int broker_create_queue_local(t_broker *broker, const char *domain_name,
                                     const char *queue_name, int type, int flags) {
    if (!broker || broker->free_pending || !domain_name || !queue_name) return -1;
    broker_reap_domains(broker);
    if (broker_try_complete_destroy(broker)) return -1;
    t_domain *owner = broker_find_queue_domain(broker, queue_name);
    if (owner && strcmp(t_domain_name(owner), domain_name) != 0) {
        return -1; /* queue name already owned by another domain */
    }
    t_domain *d = t_broker_get_domain(broker, domain_name);
    if (!d) {
        d = t_broker_create_domain(broker, domain_name);
        if (!d) return -1;
    }
    int r = t_domain_create_queue(d, queue_name, type, flags);
    if (r != 0) return r;
    if ((flags & T_QUEUE_FLAG_DURABLE) && !broker->raft) {
        if (!broker->datadir) {
            (void)t_domain_delete_queue(d, queue_name);
            return -1;
        }
        char path[1024];
        if (broker_wal_path(path, sizeof(path), broker->datadir, domain_name, queue_name) != 0) {
            (void)t_domain_delete_queue(d, queue_name);
            return -1;
        }
        t_queue *q = (t_queue *)t_domain_get_queue(d, queue_name);
        if (!q || t_queue_open_wal(q, path, broker->wal_sync_every) != 0) {
            (void)t_domain_delete_queue(d, queue_name);
            return -1;
        }
    }
    return 0;
}

static int broker_delete_queue_local(t_broker *broker, const char *domain_name,
                                     const char *queue_name) {
    if (!broker || broker->free_pending || !domain_name || !queue_name) return -1;
    t_domain *d = t_broker_get_domain(broker, domain_name);
    if (!d) return -1;
    if (broker->applying && !t_domain_get_queue(d, queue_name)) return 0;
    char walpath[1024];
    walpath[0] = 0;
    t_queue *q = (t_queue *)t_domain_get_queue(d, queue_name);
    if (q && t_queue_wal_path(q)) {
        (void)snprintf(walpath, sizeof(walpath), "%s", t_queue_wal_path(q));
    }
    int r = t_domain_delete_queue(d, queue_name);
    if (r == 0 && walpath[0])
        (void)t_wal_unlink(walpath);
    t_dispatch_reap_deferred();
    broker_reap_domains(broker);
    if (broker_try_complete_destroy(broker)) return -1;
    return r;
}

static int broker_propose_named(t_broker *broker, uint8_t type, uint8_t qtype,
                                uint8_t qflags, const char *queue_name);

int t_broker_create_queue(t_broker *broker, const char *domain_name,
                          const char *queue_name, int type, int flags) {
    if (!broker || broker->free_pending || !domain_name || !queue_name) return -1;
    if (broker->raft && !broker->applying) {
        t_domain *owner = broker_find_queue_domain(broker, queue_name);
        if (owner && t_domain_get_queue(owner, queue_name)) return 0;
        if (!t_broker_is_leader(broker)) return -1;
        return broker_propose_named(broker, T_RAFT_CMD_CREATE, (uint8_t)type,
                                    (uint8_t)flags, queue_name);
    }
    return broker_create_queue_local(broker, domain_name, queue_name, type, flags);
}

int t_broker_delete_queue(t_broker *broker, const char *domain_name,
                          const char *queue_name) {
    if (!broker || broker->free_pending || !domain_name || !queue_name) return -1;
    if (broker->raft && !broker->applying) {
        if (!t_broker_is_leader(broker)) return -1;
        return broker_propose_named(broker, T_RAFT_CMD_DELETE, 0, 0, queue_name);
    }
    return broker_delete_queue_local(broker, domain_name, queue_name);
}

static int broker_ensure_queue(t_broker *broker, const char *name,
                               int qtype, int qflags) {
    t_domain *d = broker_find_queue_domain(broker, name);
    if (d && t_domain_get_queue(d, name)) return 0;
    return t_broker_create_queue(broker, "default", name, qtype, qflags);
}

static void broker_on_raft(const t_raft_entry *entry, void *ud) {
    t_broker *b = (t_broker *)ud;
    if (!b || !entry || !entry->data) return;
    b->applying++;
    t_raft_cmd cmd;
    if (t_raft_cmd_decode(entry->data, entry->data_len, &cmd) != 0) {
        b->applying--;
        return;
    }
    char name[T_WIRE_MAX_NAME + 1];
    if (t_wire_name_copy(name, sizeof(name), cmd.name, cmd.name_len) != 0) {
        b->applying--;
        return;
    }
    if (cmd.type == T_RAFT_CMD_PUT) {
        if (broker_ensure_queue(b, name, (int)cmd.qtype, (int)cmd.qflags) == 0) {
            t_domain *d = broker_find_queue_domain(b, name);
            t_queue *q = d ? (t_queue *)t_domain_get_queue(d, name) : NULL;
            if (q)
                (void)t_queue_restore(q, cmd.msg_id, cmd.data, cmd.data_len,
                                      (int)cmd.priority);
        }
    } else if (cmd.type == T_RAFT_CMD_ACK) {
        t_domain *d = broker_find_queue_domain(b, name);
        t_queue *q = d ? (t_queue *)t_domain_get_queue(d, name) : NULL;
        if (q) (void)t_queue_drop(q, cmd.msg_id);
    } else if (cmd.type == T_RAFT_CMD_CREATE) {
        (void)broker_create_queue_local(b, "default", name, (int)cmd.qtype,
                                        (int)cmd.qflags);
    } else if (cmd.type == T_RAFT_CMD_DELETE) {
        (void)broker_delete_queue_local(b, "default", name);
    }
    b->applying--;
}

static int broker_propose(t_broker *broker, uint8_t type,
                          const uint8_t *buf, size_t len) {
    t_raft *r = broker->raft;
    if (!r || t_raft_state(r) != T_NODE_LEADER) return -1;
    uint64_t idx = t_raft_last_log_index(r) + 1;
    if (t_raft_append_entry(r, type, buf, len) != 0) return -1;
    (void)t_raft_replicate(r, idx);
    int rc = t_raft_majority_commit(r, NULL, 0, broker_cluster_n(broker));
    if (rc == -2) return -1;
    if (t_raft_last_applied(r) < idx) return -1;
    (void)t_raft_maybe_snapshot(r, NULL, 0, broker_cluster_n(broker));
    return 0;
}

static int broker_propose_named(t_broker *broker, uint8_t type, uint8_t qtype,
                                uint8_t qflags, const char *queue_name) {
    t_raft_cmd cmd;
    memset(&cmd, 0, sizeof(cmd));
    cmd.type = type;
    cmd.qtype = qtype;
    cmd.qflags = qflags;
    cmd.name = queue_name;
    cmd.name_len = (uint16_t)strlen(queue_name);
    uint8_t buf[288];
    int n = -1;
    if (type == T_RAFT_CMD_CREATE)
        n = t_raft_cmd_encode_create(buf, sizeof(buf), &cmd);
    else if (type == T_RAFT_CMD_DELETE)
        n = t_raft_cmd_encode_delete(buf, sizeof(buf), &cmd);
    if (n < 0) return -1;
    return broker_propose(broker, type, buf, (size_t)n);
}

static int broker_propose_put(t_broker *broker, t_queue *q, const char *queue_name,
                              const uint8_t *data, size_t len, int priority) {
    t_raft_cmd cmd;
    memset(&cmd, 0, sizeof(cmd));
    cmd.type = T_RAFT_CMD_PUT;
    cmd.qtype = (uint8_t)t_queue_get_type(q);
    cmd.qflags = (uint8_t)t_queue_get_flags(q);
    cmd.priority = (priority < 0) ? 0 : (priority > 255 ? 255 : (uint8_t)priority);
    cmd.msg_id = t_raft_last_log_index(broker->raft) + 1;
    cmd.name = queue_name;
    cmd.name_len = (uint16_t)strlen(queue_name);
    cmd.data = data;
    cmd.data_len = (uint32_t)len;
    uint8_t stack[512];
    uint8_t *buf = stack;
    size_t need = 16 + (size_t)cmd.name_len + (size_t)cmd.data_len;
    int heap = 0;
    if (need > sizeof(stack)) {
        buf = (uint8_t *)malloc(need);
        if (!buf) return -1;
        heap = 1;
    }
    int n = t_raft_cmd_encode_put(buf, heap ? need : sizeof(stack), &cmd);
    int r = (n > 0) ? broker_propose(broker, T_RAFT_CMD_PUT, buf, (size_t)n) : -1;
    if (heap) free(buf);
    return r;
}

int t_broker_publish(t_broker *broker, const char *queue_name,
                     const uint8_t *data, size_t len, int priority) {
    if (!broker || broker->free_pending || !broker->running || !queue_name ||
        (len > 0 && !data)) return -1;
    if (!t_broker_is_leader(broker)) return -1;
    t_domain *d = broker_find_queue_domain(broker, queue_name);
    if (!d) return -1;
    int r;
    if (broker->raft) {
        t_queue *q = (t_queue *)t_domain_get_queue(d, queue_name);
        if (!q) return -1;
        r = broker_propose_put(broker, q, queue_name, data, len, priority);
    } else {
        r = t_domain_publish(d, queue_name, data, len, priority);
    }
    t_dispatch_reap_deferred();
    broker_reap_domains(broker);
    if (broker_try_complete_destroy(broker)) return -1;
    return r;
}

int t_broker_ack(t_broker *broker, const char *queue_name, uint64_t msg_id) {
    if (!broker || broker->free_pending || !queue_name) return -1;
    t_domain *d = broker_find_queue_domain(broker, queue_name);
    if (!d) return -1;
    t_queue *q = (t_queue *)t_domain_get_queue(d, queue_name);
    if (!q) return -1;
    if (!broker->raft) return t_queue_ack(q, msg_id);
    if (!t_broker_is_leader(broker)) return -1;
    t_raft_cmd cmd;
    memset(&cmd, 0, sizeof(cmd));
    cmd.type = T_RAFT_CMD_ACK;
    cmd.msg_id = msg_id;
    cmd.name = queue_name;
    cmd.name_len = (uint16_t)strlen(queue_name);
    uint8_t buf[288];
    int n = t_raft_cmd_encode_ack(buf, sizeof(buf), &cmd);
    if (n < 0) return -1;
    int r = broker_propose(broker, T_RAFT_CMD_ACK, buf, (size_t)n);
    t_dispatch_reap_deferred();
    broker_reap_domains(broker);
    if (broker_try_complete_destroy(broker)) return -1;
    return r;
}

int t_broker_subscribe(t_broker *broker, const char *queue_name,
                        t_broker_msg_cb cb, void *ud) {
    if (!broker || broker->free_pending || !broker->running || !queue_name || !cb)
        return -1;
    t_domain *d = broker_find_queue_domain(broker, queue_name);
    if (!d) return -1;
    int r = t_domain_subscribe(d, queue_name, cb, ud);
    t_dispatch_reap_deferred();
    broker_reap_domains(broker);
    if (broker_try_complete_destroy(broker)) return -1;
    return r;
}

int t_broker_unsubscribe(t_broker *broker, const char *queue_name,
                         t_broker_msg_cb cb, void *ud) {
    if (!broker || broker->free_pending || !queue_name || !cb) return -1;
    t_domain *d = broker_find_queue_domain(broker, queue_name);
    if (!d) return -1;
    int r = t_domain_unsubscribe(d, queue_name, cb, ud);
    t_dispatch_reap_deferred();
    broker_reap_domains(broker);
    if (broker_try_complete_destroy(broker)) return -1;
    return r;
}

int t_broker_has_subscription(t_broker *broker, const char *queue_name,
                              t_broker_msg_cb cb, void *ud) {
    if (!broker || !queue_name || !cb) return 0;
    t_domain *d = broker_find_queue_domain(broker, queue_name);
    if (!d) return 0;
    return t_domain_has_subscription(d, queue_name, cb, ud);
}

int t_broker_is_queue_delivering(const t_broker *broker, const char *queue_name) {
    if (!broker || !queue_name) return 0;
    t_domain *d = broker_find_queue_domain((t_broker *)broker, queue_name);
    if (!d) return 0;
    t_queue *q = (t_queue *)t_domain_get_queue(d, queue_name);
    return t_queue_is_delivering(q);
}

size_t t_broker_total_queues(const t_broker *broker) {
    if (!broker) return 0;
    size_t sum = 0;
    t_map_iter it = t_map_iter_begin(&broker->domains);
    const char *k; void *v;
    while (t_map_iter_next(&it, &k, &v)) {
        sum += t_domain_queue_count((t_domain *)v);
    }
    return sum;
}

size_t t_broker_total_messages(const t_broker *broker) {
    if (!broker) return 0;
    size_t sum = 0;
    t_map_iter it = t_map_iter_begin(&broker->domains);
    const char *k; void *v;
    while (t_map_iter_next(&it, &k, &v)) {
        sum += t_domain_total_messages((t_domain *)v);
    }
    return sum;
}

size_t t_broker_total_delivered(const t_broker *broker) {
    if (!broker) return 0;
    size_t sum = 0;
    t_map_iter it = t_map_iter_begin(&broker->domains);
    const char *k; void *v;
    while (t_map_iter_next(&it, &k, &v)) {
        sum += t_domain_total_delivered((t_domain *)v);
    }
    return sum;
}

static int snap_put_u8(uint8_t **p, const uint8_t *end, uint8_t v) {
    if (*p >= end) return -1;
    **p = v;
    (*p)++;
    return 0;
}

static int snap_put_u16(uint8_t **p, const uint8_t *end, uint16_t v) {
    if ((size_t)(end - *p) < 2) return -1;
    uint16_t be = htons(v);
    memcpy(*p, &be, 2);
    *p += 2;
    return 0;
}

static int snap_put_u32(uint8_t **p, const uint8_t *end, uint32_t v) {
    if ((size_t)(end - *p) < 4) return -1;
    uint32_t be = htonl(v);
    memcpy(*p, &be, 4);
    *p += 4;
    return 0;
}

static int snap_put_u64(uint8_t **p, const uint8_t *end, uint64_t v) {
    if ((size_t)(end - *p) < 8) return -1;
    uint32_t hi = htonl((uint32_t)(v >> 32));
    uint32_t lo = htonl((uint32_t)v);
    memcpy(*p, &hi, 4);
    memcpy(*p + 4, &lo, 4);
    *p += 8;
    return 0;
}

static int snap_put_bytes(uint8_t **p, const uint8_t *end, const void *src, size_t n) {
    if (n > 0 && !src) return -1;
    if ((size_t)(end - *p) < n) return -1;
    if (n) memcpy(*p, src, n);
    *p += n;
    return 0;
}

static int snap_get_u8(const uint8_t **p, const uint8_t *end, uint8_t *out) {
    if (*p >= end) return -1;
    *out = **p;
    (*p)++;
    return 0;
}

static int snap_get_u16(const uint8_t **p, const uint8_t *end, uint16_t *out) {
    if ((size_t)(end - *p) < 2) return -1;
    uint16_t be;
    memcpy(&be, *p, 2);
    *out = ntohs(be);
    *p += 2;
    return 0;
}

static int snap_get_u32(const uint8_t **p, const uint8_t *end, uint32_t *out) {
    if ((size_t)(end - *p) < 4) return -1;
    uint32_t be;
    memcpy(&be, *p, 4);
    *out = ntohl(be);
    *p += 4;
    return 0;
}

static int snap_get_u64(const uint8_t **p, const uint8_t *end, uint64_t *out) {
    if ((size_t)(end - *p) < 8) return -1;
    uint32_t hi_be, lo_be;
    memcpy(&hi_be, *p, 4);
    memcpy(&lo_be, *p + 4, 4);
    *out = ((uint64_t)ntohl(hi_be) << 32) | (uint64_t)ntohl(lo_be);
    *p += 8;
    return 0;
}

typedef struct {
    size_t nmsg;
    size_t bytes;
} snap_msg_acc;

static int snap_acc_msg(const t_msg *m, void *ud) {
    snap_msg_acc *a = (snap_msg_acc *)ud;
    if (!m || m->data_len > T_QUEUE_MAX_PAYLOAD) return -1;
    a->nmsg++;
    a->bytes += 8u + 1u + 4u + m->data_len;
    return 0;
}

typedef struct {
    uint8_t *p;
    uint8_t *end;
    int err;
} snap_msg_w;

static int snap_write_msg(const t_msg *m, void *ud) {
    snap_msg_w *w = (snap_msg_w *)ud;
    if (!m || w->err) return -1;
    if (snap_put_u64(&w->p, w->end, m->msg_id) != 0 ||
        snap_put_u8(&w->p, w->end, (uint8_t)(m->priority < 0 ? 0 :
                    (m->priority > 255 ? 255 : m->priority))) != 0 ||
        snap_put_u32(&w->p, w->end, (uint32_t)m->data_len) != 0 ||
        snap_put_bytes(&w->p, w->end, m->data, m->data_len) != 0) {
        w->err = 1;
        return -1;
    }
    return 0;
}

typedef struct {
    uint8_t *buf;
    size_t   len;
    size_t   cap;
} snap_buf;

static int snap_buf_reserve(snap_buf *b, size_t add) {
    if (add > SIZE_MAX - b->len) return -1;
    size_t need = b->len + add;
    if (need <= b->cap) return 0;
    size_t cap = b->cap ? b->cap : 256;
    while (cap < need) {
        if (cap > SIZE_MAX / 2) return -1;
        cap *= 2;
    }
    uint8_t *n = (uint8_t *)realloc(b->buf, cap);
    if (!n) return -1;
    b->buf = n;
    b->cap = cap;
    return 0;
}

typedef struct {
    snap_buf *b;
    int err;
} snap_qctx;

static void snap_write_queue(void *queue, void *ud) {
    snap_qctx *c = (snap_qctx *)ud;
    t_queue *q = (t_queue *)queue;
    if (!q || c->err) return;
    const char *qn = t_queue_name(q);
    size_t nlen = qn ? strlen(qn) : 0;
    if (nlen == 0 || nlen > T_WIRE_MAX_NAME ||
        !t_wire_name_valid(qn, nlen)) {
        c->err = 1;
        return;
    }
    snap_msg_acc acc = {0, 0};
    if (t_queue_each_live(q, snap_acc_msg, &acc) != 0) {
        c->err = 1;
        return;
    }
    size_t qneed = 1u + 1u + 2u + nlen + 4u + acc.bytes;
    if (snap_buf_reserve(c->b, qneed) != 0) {
        c->err = 1;
        return;
    }
    uint8_t *p = c->b->buf + c->b->len;
    uint8_t *end = c->b->buf + c->b->cap;
    if (snap_put_u8(&p, end, (uint8_t)t_queue_get_type(q)) != 0 ||
        snap_put_u8(&p, end, (uint8_t)t_queue_get_flags(q)) != 0 ||
        snap_put_u16(&p, end, (uint16_t)nlen) != 0 ||
        snap_put_bytes(&p, end, qn, nlen) != 0 ||
        snap_put_u32(&p, end, (uint32_t)acc.nmsg) != 0) {
        c->err = 1;
        return;
    }
    snap_msg_w w = { p, end, 0 };
    if (t_queue_each_live(q, snap_write_msg, &w) != 0 || w.err) {
        c->err = 1;
        return;
    }
    c->b->len = (size_t)(w.p - c->b->buf);
}

int t_broker_snapshot_encode(const t_broker *broker, uint8_t **out, size_t *len) {
    if (!broker || !out || !len) return -1;
    *out = NULL;
    *len = 0;
    snap_buf b;
    memset(&b, 0, sizeof(b));
    if (snap_buf_reserve(&b, 4) != 0) return -1;
    b.len = 4;
    uint32_t ndom = 0;
    t_map_iter dit = t_map_iter_begin((t_map *)&broker->domains);
    const char *dk;
    void *dv;
    while (t_map_iter_next(&dit, &dk, &dv)) {
        t_domain *d = (t_domain *)dv;
        const char *dn = t_domain_name(d);
        size_t dlen = dn ? strlen(dn) : 0;
        if (dlen == 0 || dlen > T_WIRE_MAX_NAME) {
            free(b.buf);
            return -1;
        }
        uint32_t nq = (uint32_t)t_domain_queue_count(d);
        size_t hdr = 2 + dlen + 4;
        if (snap_buf_reserve(&b, hdr) != 0) {
            free(b.buf);
            return -1;
        }
        uint8_t *p = b.buf + b.len;
        uint8_t *end = b.buf + b.cap;
        if (snap_put_u16(&p, end, (uint16_t)dlen) != 0 ||
            snap_put_bytes(&p, end, dn, dlen) != 0 ||
            snap_put_u32(&p, end, nq) != 0) {
            free(b.buf);
            return -1;
        }
        b.len = (size_t)(p - b.buf);
        snap_qctx qc = { &b, 0 };
        t_domain_foreach_queue(d, snap_write_queue, &qc);
        if (qc.err) {
            free(b.buf);
            return -1;
        }
        ndom++;
    }
    uint8_t *hp = b.buf;
    if (snap_put_u32(&hp, b.buf + b.cap, ndom) != 0) {
        free(b.buf);
        return -1;
    }
    *out = b.buf;
    *len = b.len;
    return 0;
}

int t_broker_snapshot_apply(t_broker *broker, const uint8_t *data, size_t len) {
    if (!broker || broker->free_pending) return -1;
    if (len > 0 && !data) return -1;
    if (t_broker_total_queues(broker) != 0) return -1;
    const uint8_t *p = data;
    const uint8_t *end = data + len;
    uint32_t ndom = 0;
    if (snap_get_u32(&p, end, &ndom) != 0) return -1;
    broker->applying++;
    int rc = -1;
    for (uint32_t i = 0; i < ndom; i++) {
        uint16_t dlen = 0;
        if (snap_get_u16(&p, end, &dlen) != 0 || dlen == 0 || dlen > T_WIRE_MAX_NAME)
            goto out;
        if ((size_t)(end - p) < dlen) goto out;
        char dname[T_WIRE_MAX_NAME + 1];
        memcpy(dname, p, dlen);
        dname[dlen] = '\0';
        p += dlen;
        uint32_t nq = 0;
        if (snap_get_u32(&p, end, &nq) != 0) goto out;
        for (uint32_t q = 0; q < nq; q++) {
            uint8_t qtype = 0, qflags = 0;
            uint16_t nlen = 0;
            if (snap_get_u8(&p, end, &qtype) != 0 ||
                snap_get_u8(&p, end, &qflags) != 0 ||
                snap_get_u16(&p, end, &nlen) != 0 ||
                nlen == 0 || nlen > T_WIRE_MAX_NAME)
                goto out;
            if ((size_t)(end - p) < nlen) goto out;
            char qname[T_WIRE_MAX_NAME + 1];
            memcpy(qname, p, nlen);
            qname[nlen] = '\0';
            p += nlen;
            if (!t_wire_name_valid(qname, nlen)) goto out;
            if (broker_create_queue_local(broker, dname, qname, (int)qtype,
                                          (int)qflags) != 0)
                goto out;
            t_domain *dom = t_broker_get_domain(broker, dname);
            t_queue *qq = dom ? (t_queue *)t_domain_get_queue(dom, qname) : NULL;
            if (!qq) goto out;
            uint32_t nmsg = 0;
            if (snap_get_u32(&p, end, &nmsg) != 0) goto out;
            for (uint32_t m = 0; m < nmsg; m++) {
                uint64_t id = 0;
                uint8_t pri = 0;
                uint32_t dlenm = 0;
                if (snap_get_u64(&p, end, &id) != 0 ||
                    snap_get_u8(&p, end, &pri) != 0 ||
                    snap_get_u32(&p, end, &dlenm) != 0)
                    goto out;
                if (dlenm > T_QUEUE_MAX_PAYLOAD || (size_t)(end - p) < dlenm)
                    goto out;
                if (t_queue_restore(qq, id, dlenm ? p : NULL, dlenm, (int)pri) != 0)
                    goto out;
                p += dlenm;
            }
        }
    }
    if (p != end) goto out;
    rc = 0;
out:
    broker->applying--;
    return rc;
}

static int broker_snap_encode_cb(uint8_t **out, size_t *len, void *ud) {
    return t_broker_snapshot_encode((const t_broker *)ud, out, len);
}

static int broker_snap_apply_cb(const uint8_t *data, size_t len, void *ud) {
    return t_broker_snapshot_apply((t_broker *)ud, data, len);
}
