#include <stdlib.h>
#include <string.h>
#include <stdint.h>

#include "t_client.h"
#include "t_queue.h"

/* Minimal in-process client implementation with queue registry and subscriptions. */
typedef struct t_client_queue_entry {
    char *name;
} t_client_queue_entry;

typedef struct t_client_subscription {
    char *queue;
    t_client_msg_cb cb;
    void *ud;
} t_client_subscription;

struct t_client {
    char *id;
    int connected;

    t_client_queue_entry *queues;
    size_t queues_cap;
    size_t queues_size;

    t_client_subscription *subs;
    size_t subs_cap;
    size_t subs_count;

    size_t published;
    size_t consumed;
    int posting;      /* set while fanout callbacks run */
    int free_pending; /* destroy deferred until post returns */
};

/* Helpers */
static int client_ensure_queues_cap(t_client *c, size_t need) {
    if (c->queues_size >= need) return 0;
    size_t new_cap = (c->queues_cap == 0) ? 4 : c->queues_cap;
    while (need > new_cap) {
        if (new_cap > SIZE_MAX / 2) return -1;
        new_cap *= 2;
    }
    if (new_cap > SIZE_MAX / sizeof(t_client_queue_entry)) return -1;
    t_client_queue_entry *newq = (t_client_queue_entry *)realloc(c->queues, new_cap * sizeof(t_client_queue_entry));
    if (!newq) return -1;
    c->queues = newq;
    c->queues_cap = new_cap;
    return 0;
}

static int client_ensure_subs_cap(t_client *c, size_t need) {
    if (c->subs_count >= need) return 0;
    size_t new_cap = (c->subs_cap == 0) ? 4 : c->subs_cap;
    while (need > new_cap) {
        if (new_cap > SIZE_MAX / 2) return -1;
        new_cap *= 2;
    }
    if (new_cap > SIZE_MAX / sizeof(t_client_subscription)) return -1;
    t_client_subscription *news = (t_client_subscription *)realloc(c->subs, new_cap * sizeof(t_client_subscription));
    if (!news) return -1;
    c->subs = news;
    c->subs_cap = new_cap;
    return 0;
}

/* API */
t_client *t_client_create(const char *client_id) {
    t_client *c = (t_client *)calloc(1, sizeof(t_client));
    if (!c) return NULL;
    c->id = strdup(client_id ? client_id : "");
    if (!c->id) {
        free(c);
        return NULL;
    }
    c->connected = 0;
    c->queues = NULL; c->queues_cap = 0; c->queues_size = 0;
    c->subs = NULL; c->subs_cap = 0; c->subs_count = 0;
    c->published = 0; c->consumed = 0;
    return c;
}

void t_client_destroy(t_client *client) {
    if (!client) return;
    if (client->posting > 0) {
        client->free_pending = 1;
        return;
    }
    if (client->id) free(client->id);
    for (size_t i = 0; i < client->queues_size; ++i) {
        free(client->queues[i].name);
    }
    free(client->queues);
    for (size_t i = 0; i < client->subs_count; ++i) {
        free(client->subs[i].queue);
    }
    free(client->subs);
    free(client);
}

const char *t_client_id(const t_client *client) {
    return client ? client->id : NULL;
}

int t_client_is_connected(const t_client *client) {
    return client ? client->connected != 0 : 0;
}

int t_client_connect(t_client *client, const char *host, uint16_t port) {
    (void)host; (void)port; /* no network in this simplified client */
    if (!client) return -1;
    client->connected = 1;
    return 0;
}

int t_client_disconnect(t_client *client) {
    if (!client) return -1;
    client->connected = 0;
    /* Drop all subscriptions so reconnect cannot revive stale callbacks. */
    for (size_t i = 0; i < client->subs_count; ++i) {
        free(client->subs[i].queue);
    }
    free(client->subs);
    client->subs = NULL;
    client->subs_count = 0;
    client->subs_cap = 0;
    return 0;
}

int t_client_open_queue(t_client *client, const char *queue_name, int flags) {
    (void)flags;
    if (!client || !queue_name || !client->connected) return -1;
    /* ensure exists */
    for (size_t i = 0; i < client->queues_size; ++i) {
        if (strcmp(client->queues[i].name, queue_name) == 0) return 0;
    }
    if (client_ensure_queues_cap(client, client->queues_size + 1) != 0) return -1;
    char *qn = strdup(queue_name);
    if (!qn) return -1;
    client->queues[client->queues_size].name = qn;
    client->queues_size++;
    return 0;
}

int t_client_close_queue(t_client *client, const char *queue_name) {
    if (!client || !queue_name) return -1;
    for (size_t i = 0; i < client->queues_size; ++i) {
        if (strcmp(client->queues[i].name, queue_name) == 0) {
            free(client->queues[i].name);
            /* shift */
            for (size_t j = i; j + 1 < client->queues_size; ++j) {
                client->queues[j] = client->queues[j+1];
            }
            client->queues_size--;
            /* Drop all subscriptions for this queue. */
            (void)t_client_unsubscribe(client, queue_name);
            return 0;
        }
    }
    return -1;
}

int t_client_post(t_client *client, const char *queue_name,
                  const uint8_t *data, size_t len, int priority) {
    (void)priority; /* priority currently unused in this stub */
    if (!client || !queue_name || !client->connected) return -1;
    if (len > 0 && !data) return -1;
    if (len > T_QUEUE_MAX_PAYLOAD) return -1;
    int open = 0;
    for (size_t i = 0; i < client->queues_size; ++i) {
        if (strcmp(client->queues[i].name, queue_name) == 0) {
            open = 1;
            break;
        }
    }
    if (!open) return -1;
    /* Snapshot (cb,ud) so unsubscribe/disconnect in a callback cannot skip peers. */
    size_t n = client->subs_count;
    typedef struct { t_client_msg_cb cb; void *ud; } t_post_snap;
    t_post_snap *snaps = NULL;
    size_t snap_n = 0;
    if (n > 0) {
        snaps = (t_post_snap *)calloc(n, sizeof(*snaps));
        if (!snaps) return -1;
        for (size_t i = 0; i < n; ++i) {
            if (client->subs[i].queue && strcmp(client->subs[i].queue, queue_name) == 0 &&
                client->subs[i].cb) {
                snaps[snap_n].cb = client->subs[i].cb;
                snaps[snap_n].ud = client->subs[i].ud;
                snap_n++;
            }
        }
    }
    client->posting++;
    client->published++;
    size_t delivered = 0;
    for (size_t i = 0; i < snap_n; ++i) {
        snaps[i].cb(queue_name, data, len, snaps[i].ud);
        delivered++;
    }
    client->consumed += delivered;
    client->posting--;
    free(snaps);
    if (client->posting == 0 && client->free_pending) {
        t_client_destroy(client);
        return 0;
    }
    return 0;
}

int t_client_subscribe(t_client *client, const char *queue_name,
                       t_client_msg_cb cb, void *ud) {
    if (!client || !queue_name || !client->connected) return -1;
    for (size_t i = 0; i < client->subs_count; ++i) {
        if (client->subs[i].queue && strcmp(client->subs[i].queue, queue_name) == 0 &&
            client->subs[i].cb == cb && client->subs[i].ud == ud) {
            return -1;
        }
    }
    if (client_ensure_subs_cap(client, client->subs_count + 1) != 0) return -1;
    char *qn = strdup(queue_name);
    if (!qn) return -1;
    client->subs[client->subs_count].queue = qn;
    client->subs[client->subs_count].cb = cb;
    client->subs[client->subs_count].ud = ud;
    client->subs_count++;
    return 0;
}

int t_client_unsubscribe(t_client *client, const char *queue_name) {
    if (!client || !queue_name) return -1;
    int removed = 0;
    for (size_t i = 0; i < client->subs_count; ) {
        if (strcmp(client->subs[i].queue, queue_name) == 0) {
            free(client->subs[i].queue);
            for (size_t j = i; j + 1 < client->subs_count; ++j) {
                client->subs[j] = client->subs[j+1];
            }
            client->subs_count--;
            removed++;
        } else {
            ++i;
        }
    }
    return removed ? 0 : -1;
}

size_t t_client_queue_count(const t_client *client) {
    return client ? client->queues_size : 0;
}

size_t t_client_total_published(const t_client *client) {
    return client ? client->published : 0;
}

size_t t_client_total_consumed(const t_client *client) {
    return client ? client->consumed : 0;
}
