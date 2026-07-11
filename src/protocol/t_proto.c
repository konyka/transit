#include "t_proto.h"
#include <stdlib.h>
#include <string.h>
#include <arpa/inet.h>
#include "t_crc32c.h"

void t_proto_header_init(t_proto_header *hdr, t_msg_type type, uint32_t payload_len) {
    hdr->magic = T_PROTO_MAGIC;
    hdr->crc32c = 0;
    hdr->version = T_PROTO_VERSION;
    hdr->type = (uint16_t)type;
    hdr->payload_len = payload_len;
}

int t_proto_header_encode(const t_proto_header *hdr, uint8_t *buf, size_t buf_len) {
    if (!hdr || !buf) return -1;
    if (buf_len < T_PROTO_HEADER_SIZE) return -1;
    uint32_t magic_be = htonl(hdr->magic);
    uint32_t crc_be = htonl(hdr->crc32c);
    uint16_t ver_be = htons(hdr->version);
    uint16_t type_be = htons(hdr->type);
    uint32_t plen_be = htonl(hdr->payload_len);
    memcpy(buf + 0, &magic_be, 4);
    memcpy(buf + 4, &crc_be, 4);
    memcpy(buf + 8, &ver_be, 2);
    memcpy(buf + 10, &type_be, 2);
    memcpy(buf + 12, &plen_be, 4);
    return 0;
}

int t_proto_header_decode(t_proto_header *hdr, const uint8_t *buf, size_t buf_len) {
    if (!hdr || !buf) return -1;
    if (buf_len < T_PROTO_HEADER_SIZE) return -1;
    uint32_t magic_be, crc_be, plen_be;
    uint16_t ver_be, type_be;
    memcpy(&magic_be, buf + 0, 4);
    memcpy(&crc_be, buf + 4, 4);
    memcpy(&ver_be, buf + 8, 2);
    memcpy(&type_be, buf + 10, 2);
    memcpy(&plen_be, buf + 12, 4);

    hdr->magic = ntohl(magic_be);
    hdr->crc32c = ntohl(crc_be);
    hdr->version = ntohs(ver_be);
    hdr->type = ntohs(type_be);
    hdr->payload_len = ntohl(plen_be);
    return 0;
}

int t_proto_msg_validate(const t_proto_msg *msg) {
    if (!msg) return -1;
    if (!msg->payload && msg->payload_len > 0) return -1;
    if (msg->header.magic != T_PROTO_MAGIC) return -1;
    if (msg->header.version != T_PROTO_VERSION) return -1;
    if (msg->header.type >= T_MSG_MAX) return -1;
    if (msg->header.payload_len > T_PROTO_MAX_PAYLOAD) return -1;
    if (msg->header.payload_len != msg->payload_len) return -1;
    return 0;
}

int t_proto_encode_msg(const t_proto_msg *msg, uint8_t *buf, size_t buf_len) {
    if (!msg || !buf) return -1;
    if (msg->payload_len > 0 && !msg->payload) return -1;
    size_t total = T_PROTO_HEADER_SIZE + msg->payload_len;
    if (buf_len < total) return -1;
    t_proto_header hdr = msg->header;
    hdr.payload_len = (uint32_t)msg->payload_len;
    hdr.crc32c = 0;
    // encode header with CRC=0
    if (t_proto_header_encode(&hdr, buf, buf_len) != 0) return -1;
    // copy payload
    if (msg->payload_len > 0) {
        memcpy(buf + T_PROTO_HEADER_SIZE, msg->payload, msg->payload_len);
    }
    // compute CRC over header+payload (CRC field in header is zero during calc)
    uint32_t crc = t_crc32c(buf, total);
    uint32_t crc_be = htonl(crc);
    memcpy(buf + 4, &crc_be, 4);
    return (int)total;
}

int t_proto_decode_msg(t_proto_msg *msg, const uint8_t *buf, size_t buf_len) {
    if (!msg || !buf) return -1;
    msg->payload = NULL;
    msg->payload_len = 0;
    if (buf_len < T_PROTO_HEADER_SIZE) return -1;
    t_proto_header hdr;
    if (t_proto_header_decode(&hdr, buf, buf_len) != 0) return -1;
    if (hdr.magic != T_PROTO_MAGIC ||
        hdr.version != T_PROTO_VERSION ||
        hdr.type >= T_MSG_MAX) {
        return -1;
    }
    if (hdr.payload_len > T_PROTO_MAX_PAYLOAD) return -1;
    size_t total = T_PROTO_HEADER_SIZE + hdr.payload_len;
    if (buf_len < total) return -1;
    // verify CRC over header(with CRC=0) + payload
    uint8_t *tmp = (uint8_t*)malloc(total);
    if (!tmp) return -1;
    memcpy(tmp, buf, total);
    uint32_t zero = 0;
    memcpy(tmp + 4, &zero, 4);
    uint32_t crc_calc = t_crc32c(tmp, total);
    free(tmp);
    if (crc_calc != hdr.crc32c) {
        return -1;
    }
    msg->header = hdr;
    msg->payload_len = hdr.payload_len;
    if (hdr.payload_len > 0) {
        msg->payload = (uint8_t*)malloc(hdr.payload_len);
        if (!msg->payload) {
            msg->payload_len = 0;
            return -1;
        }
        memcpy(msg->payload, buf + T_PROTO_HEADER_SIZE, hdr.payload_len);
    }
    return 0;
}
