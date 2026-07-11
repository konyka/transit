#include "t_test.h"
#include "t_proto.h"
#include "t_crc32c.h"
#include <string.h>
#include <stdlib.h>
#include <arpa/inet.h>

T_TEST(crc32c_empty) {
    uint32_t crc = t_crc32c("", 0);
    T_ASSERT_EQ((int)crc, 0);
}

T_TEST(crc32c_known) {
    uint32_t crc = t_crc32c("123456789", 9);
    T_ASSERT_EQ((int)crc, (int)0xE3069283);
}

T_TEST(crc32c_update) {
    uint32_t crc = t_crc32c_update(0xFFFFFFFF, "1234", 4);
    crc = t_crc32c_update(crc, "56789", 5);
    crc ^= 0xFFFFFFFF;
    T_ASSERT_EQ((int)crc, (int)0xE3069283);
}

T_TEST(proto_header_init) {
    t_proto_header hdr;
    t_proto_header_init(&hdr, T_MSG_POST, 100);
    T_ASSERT_EQ((int)hdr.magic, T_PROTO_MAGIC);
    T_ASSERT_EQ((int)hdr.version, T_PROTO_VERSION);
    T_ASSERT_EQ((int)hdr.type, T_MSG_POST);
    T_ASSERT_EQ((int)hdr.payload_len, 100);
}

T_TEST(proto_encode_decode) {
    t_proto_msg msg;
    t_proto_header_init(&msg.header, T_MSG_ACK, 5);
    uint8_t payload[] = "hello";
    msg.payload = payload;
    msg.payload_len = 5;
    uint8_t buf[256];
    int n = t_proto_encode_msg(&msg, buf, sizeof(buf));
    T_ASSERT(n > 0);
    t_proto_msg msg2;
    memset(&msg2, 0, sizeof(msg2));
    int r = t_proto_decode_msg(&msg2, buf, (size_t)n);
    T_ASSERT_EQ(r, 0);
    T_ASSERT_EQ((int)msg2.header.magic, T_PROTO_MAGIC);
    T_ASSERT_EQ((int)msg2.header.type, T_MSG_ACK);
    T_ASSERT_EQ((int)msg2.header.payload_len, 5);
    T_ASSERT(memcmp(msg2.payload, "hello", 5) == 0);
    free(msg2.payload);
}

T_TEST(proto_validate_bad_magic) {
    t_proto_msg msg;
    memset(&msg, 0, sizeof(msg));
    msg.header.magic = 0xDEAD;
    T_ASSERT(t_proto_msg_validate(&msg) != 0);
}

T_TEST(proto_validate_ok) {
    t_proto_msg msg;
    t_proto_header_init(&msg.header, T_MSG_NOP, 0);
    msg.payload = NULL;
    msg.payload_len = 0;
    T_ASSERT_EQ(t_proto_msg_validate(&msg), 0);
}

T_TEST(proto_decode_rejects_bad_magic) {
    uint8_t buf[64];
    t_proto_msg msg;
    t_proto_header_init(&msg.header, T_MSG_ACK, 0);
    msg.payload = NULL;
    msg.payload_len = 0;
    int n = t_proto_encode_msg(&msg, buf, sizeof(buf));
    T_ASSERT(n > 0);
    uint32_t bad = htonl(0xDEADBEEF);
    memcpy(buf, &bad, 4);
    /* Recompute CRC so failure is due to magic, not checksum. */
    uint32_t zero = 0;
    memcpy(buf + 4, &zero, 4);
    uint32_t crc = t_crc32c(buf, (size_t)n);
    uint32_t crc_be = htonl(crc);
    memcpy(buf + 4, &crc_be, 4);
    t_proto_msg out;
    memset(&out, 0, sizeof(out));
    T_ASSERT(t_proto_decode_msg(&out, buf, (size_t)n) != 0);
}

T_TEST(proto_encode_rejects_null_payload) {
    uint8_t buf[64];
    t_proto_msg msg;
    t_proto_header_init(&msg.header, T_MSG_POST, 4);
    msg.payload = NULL;
    msg.payload_len = 4;
    T_ASSERT(t_proto_encode_msg(&msg, buf, sizeof(buf)) != 0);
}

T_TEST(proto_encode_syncs_header_len) {
    uint8_t buf[64];
    t_proto_msg msg;
    t_proto_header_init(&msg.header, T_MSG_POST, 99); /* stale header len */
    msg.payload = (uint8_t *)"hi";
    msg.payload_len = 2;
    int n = t_proto_encode_msg(&msg, buf, sizeof(buf));
    T_ASSERT_EQ(n, (int)(T_PROTO_HEADER_SIZE + 2));
    t_proto_msg out;
    memset(&out, 0, sizeof(out));
    T_ASSERT_EQ(t_proto_decode_msg(&out, buf, (size_t)n), 0);
    T_ASSERT_EQ((int)out.payload_len, 2);
    free(out.payload);
}

int main(void) {
    return t_run_all_tests();
}
