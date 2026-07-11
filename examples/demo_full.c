#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "t_version.h"
#include "t_broker.h"
#include "t_domain.h"
#include "t_dispatch.h"
#include "t_session.h"
#include "t_cluster.h"
#include "t_raft.h"
#include "t_node.h"
#include "t_queue.h"
#include "t_router.h"
#include "t_proto.h"
#include "t_storage.h"
#include "t_crc32c.h"
#include "t_time.h"

static int g_delivered;
static int g_prices_count;
static int g_alerts_count;

static void on_delivery(const char *queue_name, const uint8_t *data, size_t len, void *ud) {
    (void)ud;
    printf("  [deliver] queue='%s' data='%.*s'\n", queue_name, (int)len, (const char *)data);
    g_delivered++;
}

static void q_count_cb(const t_msg *msg, void *ud) {
    (void)msg;
    (*(int *)ud)++;
}

static void simulate_topic_routing(void) {
    printf("\n=== Topic Routing Demo ===\n");
    t_router *router = t_router_create();
    t_queue *prices_q = t_queue_create("prices", T_QUEUE_FIFO, 0);
    t_queue *alerts_q = t_queue_create("alerts", T_QUEUE_BROADCAST, 0);

    t_queue_add_consumer(prices_q, q_count_cb, &g_prices_count);
    t_queue_add_consumer(alerts_q, q_count_cb, &g_alerts_count);

    t_router_bind(router, "stock.prices.*", prices_q);
    t_router_bind(router, "alerts.#", alerts_q);

    const char *topics[] = {
        "stock.prices.nasdaq", "stock.prices.nyse",
        "alerts.critical.disk", "alerts.warning.cpu"
    };
    const char *bodies[] = { "AAPL=150", "GOOG=2800", "disk full", "cpu 95%" };

    void *targets[8];
    for (int i = 0; i < 4; i++) {
        size_t n = t_router_route(router, topics[i], targets, 8);
        printf("  topic='%s' matched=%zu queues\n", topics[i], n);
        for (size_t j = 0; j < n; j++) {
            t_queue_post((t_queue *)targets[j],
                         (const uint8_t *)bodies[i], strlen(bodies[i]), 0);
        }
    }

    printf("  prices_q received: %d messages\n", g_prices_count);
    printf("  alerts_q received: %d messages\n", g_alerts_count);

    t_queue_destroy(prices_q);
    t_queue_destroy(alerts_q);
    t_router_destroy(router);
}

static void simulate_cluster_election(void) {
    printf("\n=== Cluster Election Demo ===\n");
    t_cluster *cluster = t_cluster_create(1);
    t_cluster_add_node(cluster, 1, "10.0.0.1", 4222);
    t_cluster_add_node(cluster, 2, "10.0.0.2", 4222);
    t_cluster_add_node(cluster, 3, "10.0.0.3", 4222);
    printf("  cluster: %zu nodes\n", t_cluster_node_count(cluster));

    t_raft_config cfg = {1, 150, 50};
    t_raft *raft = t_raft_create(&cfg);
    printf("  node-1: term=%lu state=follower\n", (unsigned long)t_raft_current_term(raft));

    t_raft_become_candidate(raft);
    printf("  node-1: term=%lu state=candidate (voted for self)\n",
           (unsigned long)t_raft_current_term(raft));

    t_raft_become_leader(raft);
    t_cluster_set_leader(cluster, 1);
    printf("  node-1: term=%lu state=LEADER\n", (unsigned long)t_raft_current_term(raft));

    const char *entries[] = {"config: nodes=3", "queue: create orders.q", "queue: create trades.q"};
    for (int i = 0; i < 3; i++) {
        t_raft_append_entry(raft, 2, (const uint8_t *)entries[i], strlen(entries[i]) + 1);
    }
    t_raft_advance_commit(raft, 3);
    t_raft_apply_entries(raft);
    printf("  raft log: %zu entries, %zu applied\n",
           t_raft_log_count(raft), t_raft_applied_count(raft));

    t_raft_become_follower(raft, 2);
    t_cluster_set_leader(cluster, 2);
    printf("  node-1: stepped down, term=%lu leader=node-2\n",
           (unsigned long)t_raft_current_term(raft));

    t_raft_destroy(raft);
    t_cluster_destroy(cluster);
}

static void simulate_persistent_storage(void) {
    printf("\n=== Persistent Storage Demo ===\n");
    t_storage *store = t_storage_create(T_STORAGE_MEM, NULL);
    uint64_t keys[] = {1001, 1002, 1003};
    const char *vals[] = {"order: BUY AAPL 100@150", "order: SELL GOOG 50@2800", "order: BUY TSLA 200@900"};

    for (int i = 0; i < 3; i++) {
        t_storage_put(store, keys[i], vals[i], strlen(vals[i]) + 1);
        printf("  stored key=%lu\n", (unsigned long)keys[i]);
    }
    for (int i = 0; i < 3; i++) {
        const void *data = NULL; size_t len = 0;
        if (t_storage_get(store, keys[i], &data, &len) == 0) {
            printf("  lookup key=%lu -> '%s'\n", (unsigned long)keys[i], (const char *)data);
        }
    }

    t_storage_delete(store, 1002);
    printf("  deleted key=1002, contains=%d\n", t_storage_contains(store, 1002));
    t_storage_destroy(store);
}

static void simulate_protocol_wire(void) {
    printf("\n=== Wire Protocol Demo ===\n");
    t_proto_msg msg;
    t_proto_header_init(&msg.header, T_MSG_POST, 13);
    uint8_t payload[] = "hello, world!";
    msg.payload = payload;
    msg.payload_len = 13;

    uint8_t wire[256];
    int n = t_proto_encode_msg(&msg, wire, sizeof(wire));
    printf("  encoded: %d bytes on wire\n", n);

    uint32_t crc = t_crc32c_update(0, wire, (size_t)n);
    printf("  crc32c: 0x%08X\n", (unsigned)crc);

    t_proto_msg decoded;
    memset(&decoded, 0, sizeof(decoded));
    if (t_proto_decode_msg(&decoded, wire, (size_t)n) == 0) {
        printf("  decoded: type=%d payload='%.*s'\n",
               decoded.header.type, (int)decoded.payload_len, (char *)decoded.payload);
        free(decoded.payload);
    }
}

int main(void) {
    printf("Transit MQ v%s — Comprehensive Demo\n", t_version());
    printf("=====================================\n");

    printf("\n=== Broker + Dispatch Demo ===\n");
    t_broker *broker = t_broker_create("demo-broker");
    t_broker_start(broker);
    t_dispatch *dispatch = t_dispatch_create(broker);

    t_broker_create_queue(broker, "default", "orders.q", T_QUEUE_FIFO, 0);
    t_broker_create_queue(broker, "default", "notifications.q", T_QUEUE_BROADCAST, 0);

    t_session *producer = t_session_create(1);
    t_session *consumer1 = t_session_create(2);
    t_session *consumer2 = t_session_create(3);
    t_dispatch_register(dispatch, 1, producer);
    t_dispatch_register(dispatch, 2, consumer1);
    t_dispatch_register(dispatch, 3, consumer2);

    t_session_connect(producer);
    t_session_connect(consumer1);
    t_session_connect(consumer2);
    t_session_update_activity(producer);

    printf("  sessions: %zu registered\n", t_dispatch_session_count(dispatch));
    printf("  producer: id=%lu active=%d\n",
           (unsigned long)t_session_id(producer), t_session_is_active(producer));

    g_delivered = 0;
    t_broker_subscribe(broker, "notifications.q", on_delivery, NULL);

    const char *messages[] = {
        "NEW ORDER: BUY AAPL 100",
        "NEW ORDER: SELL TSLA 50",
        "TRADE FILLED: AAPL @ $150.25"
    };
    for (int i = 0; i < 3; i++) {
        t_dispatch_publish(dispatch, 1, "orders.q",
                           (const uint8_t *)messages[i], strlen(messages[i]), 0);
        t_session_record_send(producer);
    }
    printf("  published: %zu via dispatch\n", t_dispatch_total_published(dispatch));

    for (int i = 0; i < 3; i++) {
        t_broker_publish(broker, "notifications.q",
                         (const uint8_t *)messages[i], strlen(messages[i]), 0);
    }
    printf("  broadcast: %d deliveries\n", g_delivered);

    printf("  broker: queues=%zu messages=%zu delivered=%zu\n",
           t_broker_total_queues(broker),
           t_broker_total_messages(broker),
           t_broker_total_delivered(broker));

    t_dispatch_destroy(dispatch);
    t_broker_stop(broker);
    t_broker_destroy(broker);
    t_session_destroy(producer);
    t_session_destroy(consumer1);
    t_session_destroy(consumer2);

    simulate_topic_routing();
    simulate_cluster_election();
    simulate_persistent_storage();
    simulate_protocol_wire();

    printf("\n=== All demos completed ===\n");
    return 0;
}
