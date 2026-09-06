#ifndef T_PROTO_H
#define T_PROTO_H

#include "t_compiler.h"
#include <stdint.h>
#include <stddef.h>

#define T_PROTO_MAGIC       0x54524E54  /* "TRNT" */
#define T_PROTO_VERSION     1
#define T_PROTO_HEADER_SIZE 16
#define T_PROTO_MAX_PAYLOAD (16 * 1024 * 1024)

typedef enum t_msg_type {
    T_MSG_NOP = 0,
    T_MSG_OPEN_QUEUE,
    T_MSG_CLOSE_QUEUE,
    T_MSG_POST,
    T_MSG_ACK,
    T_MSG_PUSH,
    T_MSG_CONFIRM,
    T_MSG_REJECT,
    T_MSG_HEARTBEAT,
    T_MSG_CLUSTER,
    T_MSG_AUTH,
    T_MSG_JOIN,
    T_MSG_MAX
} t_msg_type;

typedef struct t_proto_header {
    uint32_t magic;
    uint32_t crc32c;
    uint16_t version;
    uint16_t type;
    uint32_t payload_len;
} t_proto_header;

typedef struct t_proto_msg {
    t_proto_header header;
    uint8_t       *payload;
    size_t         payload_len;
} t_proto_msg;

void t_proto_header_init(t_proto_header *hdr, t_msg_type type, uint32_t payload_len);
int  t_proto_header_encode(const t_proto_header *hdr, uint8_t *buf, size_t buf_len);
int  t_proto_header_decode(t_proto_header *hdr, const uint8_t *buf, size_t buf_len);
int  t_proto_msg_validate(const t_proto_msg *msg);

int  t_proto_encode_msg(const t_proto_msg *msg, uint8_t *buf, size_t buf_len);
int  t_proto_decode_msg(t_proto_msg *msg, const uint8_t *buf, size_t buf_len);

#endif /* T_PROTO_H */
