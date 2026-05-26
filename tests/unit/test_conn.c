#include "../src/net/t_conn.h"
#include "../src/protocol/t_proto.h"
#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/socket.h>
#include <unistd.h>

/* Lightweight unit tests for t_conn basics. These tests focus on
 * object lifecycle and basic encoding/decoding paths rather than a full
 * integration with the event loop.
 */

static void test_on_msg(t_conn *c, const t_proto_msg *msg, void *ud) {
    (void)c; (void)ud; (void)msg;
}

static void test_on_close(t_conn *c, void *ud) {
    (void)c; (void)ud;
}

int main(void) {
    int sv[2];
    int rc = socketpair(AF_UNIX, SOCK_STREAM, 0, sv);
    assert(rc == 0);
    int a = sv[0], b = sv[1];

    t_evloop *loop = NULL; /* assume tests supply a loop in real-suite; skip if NULL */
    t_conn *c1 = t_conn_create(a, loop);
    t_conn *c2 = t_conn_create(b, loop);

    assert(c1 && c2);
    t_proto_msg m;
    m.header.magic = 0x12345678;
    m.header.payload_len = 0;
    m.header.type = 0;
    m.payload = NULL;
    m.payload_len = 0;
    /* Encoding test: ensure t_conn_send does not crash when no payload */
    t_conn_send(c1, &m);

    t_conn_set_on_msg(c2, test_on_msg, NULL);
    t_conn_set_on_close(c1, test_on_close, NULL);

    /* Cleanup */
    t_conn_destroy(c1);
    t_conn_destroy(c2);
    close(a);
    close(b);
    return 0;
}
