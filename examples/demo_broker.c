#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "t_broker.h"

static int g_received;

static void on_msg(const char *queue_name, const uint8_t *data, size_t len, void *ud) {
    (void)ud;
    printf("[broker] received on '%s': %.*s\n", queue_name, (int)len, (const char *)data);
    g_received++;
}

int main(void) {
    t_broker *broker = t_broker_create("demo-broker");
    if (!broker) {
        fprintf(stderr, "Failed to create broker\n");
        return 1;
    }

    if (t_broker_start(broker) != 0) {
        fprintf(stderr, "Failed to start broker\n");
        t_broker_destroy(broker);
        return 1;
    }

    if (t_broker_create_queue(broker, "default", "demo.q", 2, 0) != 0) {
        fprintf(stderr, "Failed to create queue\n");
        t_broker_stop(broker);
        t_broker_destroy(broker);
        return 1;
    }

    g_received = 0;
    t_broker_subscribe(broker, "demo.q", on_msg, NULL);

    const char *msgs[] = {"hello transit mq 1", "hello transit mq 2", "end"};
    for (size_t i = 0; i < sizeof(msgs) / sizeof(msgs[0]); i++) {
        const char *m = msgs[i];
        t_broker_publish(broker, "demo.q", (const uint8_t *)m, strlen(m), 0);
    }

    printf("Broker stats: queues=%zu, messages=%zu, delivered=%d\n",
           t_broker_total_queues(broker),
           t_broker_total_messages(broker),
           g_received);

    t_broker_stop(broker);
    t_broker_destroy(broker);
    return 0;
}
