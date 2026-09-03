#include "t_test.h"
#include "t_wire.h"
#include "t_queue.h"
#include "t_error.h"
#include "t_proto.h"
#include <string.h>
#include <stdint.h>

T_TEST(wire_name_valid) {
    T_ASSERT(t_wire_name_valid("orders", 6));
    T_ASSERT(!t_wire_name_valid("", 0));
    T_ASSERT(!t_wire_name_valid(NULL, 0));
    T_ASSERT(!t_wire_name_valid("bad name\n", 9));
    T_ASSERT(!t_wire_name_valid("x", 256));
    char nuls[] = { 'a', 0, 'b' };
    T_ASSERT(!t_wire_name_valid(nuls, 3));
}

T_TEST(wire_open_roundtrip) {
    uint8_t buf[64];
    int n = t_wire_encode_open(buf, sizeof(buf), T_QUEUE_FIFO, T_QUEUE_FLAG_NONE,
                               T_WIRE_MODE_PRODUCER | T_WIRE_MODE_CONSUMER, "q.a");
    T_ASSERT_EQ(n, 3 + 2 + 3);
    t_wire_open o;
    memset(&o, 0, sizeof(o));
    T_ASSERT_EQ(t_wire_decode_open(buf, (size_t)n, &o), 0);
    T_ASSERT_EQ((int)o.qtype, T_QUEUE_FIFO);
    T_ASSERT_EQ((int)o.mode, T_WIRE_MODE_PRODUCER | T_WIRE_MODE_CONSUMER);
    T_ASSERT_EQ((int)o.name_len, 3);
    T_ASSERT_MEM_EQ(o.name, "q.a", 3);
}

T_TEST(wire_open_rejects_bad_name) {
    uint8_t buf[64];
    T_ASSERT(t_wire_encode_open(buf, sizeof(buf), 0, 0, T_WIRE_MODE_PRODUCER, "") < 0);
    T_ASSERT(t_wire_encode_open(buf, sizeof(buf), 0, 0, T_WIRE_MODE_PRODUCER, "has space") < 0);
    T_ASSERT(t_wire_encode_open(NULL, 8, 0, 0, T_WIRE_MODE_PRODUCER, "q") < 0);
}

T_TEST(wire_close_roundtrip) {
    uint8_t buf[32];
    int n = t_wire_encode_close(buf, sizeof(buf), "jobs");
    T_ASSERT(n > 0);
    t_wire_close c;
    T_ASSERT_EQ(t_wire_decode_close(buf, (size_t)n, &c), 0);
    T_ASSERT_EQ((int)c.name_len, 4);
    T_ASSERT_MEM_EQ(c.name, "jobs", 4);
}

T_TEST(wire_post_roundtrip) {
    uint8_t buf[128];
    const uint8_t payload[] = { 1, 2, 3, 4 };
    int n = t_wire_encode_post(buf, sizeof(buf), 7, "events", payload, 4);
    T_ASSERT(n > 0);
    t_wire_post p;
    T_ASSERT_EQ(t_wire_decode_post(buf, (size_t)n, &p), 0);
    T_ASSERT_EQ((int)p.priority, 7);
    T_ASSERT_EQ((int)p.name_len, 6);
    T_ASSERT_EQ((int)p.data_len, 4);
    T_ASSERT_MEM_EQ(p.data, payload, 4);
}

T_TEST(wire_post_empty_payload) {
    uint8_t buf[64];
    int n = t_wire_encode_post(buf, sizeof(buf), 0, "empty", NULL, 0);
    T_ASSERT(n > 0);
    t_wire_post p;
    T_ASSERT_EQ(t_wire_decode_post(buf, (size_t)n, &p), 0);
    T_ASSERT_EQ((int)p.data_len, 0);
    T_ASSERT_NULL(p.data);
}

T_TEST(wire_post_rejects_truncated) {
    uint8_t buf[64];
    int n = t_wire_encode_post(buf, sizeof(buf), 1, "q", (const uint8_t *)"hi", 2);
    T_ASSERT(n > 2);
    t_wire_post p;
    T_ASSERT(t_wire_decode_post(buf, (size_t)n - 1, &p) != 0);
    T_ASSERT(t_wire_decode_post(buf, (size_t)n + 1, &p) != 0); /* trailing junk */
}

T_TEST(wire_push_roundtrip) {
    uint8_t buf[128];
    int n = t_wire_encode_push(buf, sizeof(buf), 0x100000002ULL, 3, "q",
                               (const uint8_t *)"ab", 2);
    T_ASSERT(n > 0);
    t_wire_push p;
    T_ASSERT_EQ(t_wire_decode_push(buf, (size_t)n, &p), 0);
    T_ASSERT_EQ((long long)p.msg_id, (long long)0x100000002ULL);
    T_ASSERT_EQ((int)p.priority, 3);
    T_ASSERT_MEM_EQ(p.data, "ab", 2);
}

T_TEST(wire_ack_negative_status) {
    uint8_t buf[64];
    int n = t_wire_encode_ack(buf, sizeof(buf), T_MSG_POST, (int32_t)T_ERR_BUSY, "q");
    T_ASSERT(n > 0);
    t_wire_ack a;
    T_ASSERT_EQ(t_wire_decode_ack(buf, (size_t)n, &a), 0);
    T_ASSERT_EQ((int)a.req_type, T_MSG_POST);
    T_ASSERT_EQ((int)a.status, (int)T_ERR_BUSY);
    T_ASSERT_EQ((int)a.name_len, 1);
}

T_TEST(wire_ack_empty_name) {
    uint8_t buf[32];
    int n = t_wire_encode_ack(buf, sizeof(buf), T_MSG_HEARTBEAT, 0, NULL);
    T_ASSERT(n > 0);
    t_wire_ack a;
    T_ASSERT_EQ(t_wire_decode_ack(buf, (size_t)n, &a), 0);
    T_ASSERT_EQ((int)a.status, 0);
    T_ASSERT_EQ((int)a.name_len, 0);
}

T_TEST(wire_confirm_roundtrip) {
    uint8_t buf[32];
    int n = t_wire_encode_confirm(buf, sizeof(buf), 99, "dlq");
    T_ASSERT(n > 0);
    t_wire_confirm c;
    T_ASSERT_EQ(t_wire_decode_confirm(buf, (size_t)n, &c), 0);
    T_ASSERT_EQ((long long)c.msg_id, 99);
    T_ASSERT_MEM_EQ(c.name, "dlq", 3);
}

T_TEST(wire_name_copy) {
    char dest[8];
    T_ASSERT_EQ(t_wire_name_copy(dest, sizeof(dest), "abc", 3), 0);
    T_ASSERT_STR_EQ(dest, "abc");
    T_ASSERT(t_wire_name_copy(dest, 3, "abc", 3) != 0); /* no room for NUL */
}

T_TEST(wire_encode_rejects_small_cap) {
    uint8_t tiny[2];
    T_ASSERT(t_wire_encode_close(tiny, sizeof(tiny), "queue") < 0);
}

int main(void) {
    return t_run_all_tests();
}
