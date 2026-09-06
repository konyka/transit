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
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
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

int t_broker_set_raft(t_broker *broker, t_raft *raft) {
    if (!broker || broker->free_pending) return -1;
    broker->raft = raft;
    if (raft) t_raft_set_apply_cb(raft, broker_on_raft, broker);
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

int t_broker_create_queue(t_broker *broker, const char *domain_name,
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

int t_broker_delete_queue(t_broker *broker, const char *domain_name,
                          const char *queue_name) {
    if (!broker || broker->free_pending || !domain_name || !queue_name) return -1;
    t_domain *d = t_broker_get_domain(broker, domain_name);
    if (!d) return -1;
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

static int broker_ensure_queue(t_broker *broker, const char *name,
                               int qtype, int qflags) {
    t_domain *d = broker_find_queue_domain(broker, name);
    if (d && t_domain_get_queue(d, name)) return 0;
    return t_broker_create_queue(broker, "default", name, qtype, qflags);
}

static void broker_on_raft(const t_raft_entry *entry, void *ud) {
    t_broker *b = (t_broker *)ud;
    if (!b || !entry || !entry->data) return;
    t_raft_cmd cmd;
    if (t_raft_cmd_decode(entry->data, entry->data_len, &cmd) != 0) return;
    char name[T_WIRE_MAX_NAME + 1];
    if (t_wire_name_copy(name, sizeof(name), cmd.name, cmd.name_len) != 0)
        return;
    if (cmd.type == T_RAFT_CMD_PUT) {
        if (broker_ensure_queue(b, name, (int)cmd.qtype, (int)cmd.qflags) != 0)
            return;
        t_domain *d = broker_find_queue_domain(b, name);
        t_queue *q = d ? (t_queue *)t_domain_get_queue(d, name) : NULL;
        if (!q) return;
        (void)t_queue_restore(q, cmd.msg_id, cmd.data, cmd.data_len,
                              (int)cmd.priority);
        return;
    }
    if (cmd.type == T_RAFT_CMD_ACK) {
        t_domain *d = broker_find_queue_domain(b, name);
        t_queue *q = d ? (t_queue *)t_domain_get_queue(d, name) : NULL;
        if (!q) return;
        (void)t_queue_drop(q, cmd.msg_id);
    }
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
    return 0;
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
