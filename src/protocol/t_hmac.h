#ifndef T_HMAC_H
#define T_HMAC_H

#include "t_compiler.h"
#include <stdint.h>
#include <stddef.h>

#define T_HMAC_SHA256_LEN 32
#define T_AUTH_PSK_MAX    256
#define T_AUTH_CONTEXT    "transit.auth.v1"

int  t_sha256(uint8_t out[T_HMAC_SHA256_LEN], const uint8_t *data, size_t len);
int  t_hmac_sha256(uint8_t out[T_HMAC_SHA256_LEN],
                   const uint8_t *key, size_t key_len,
                   const uint8_t *data, size_t data_len);
int  t_auth_mac(uint8_t out[T_HMAC_SHA256_LEN],
                const uint8_t *psk, size_t psk_len);
int  t_hmac_equal(const uint8_t *a, const uint8_t *b, size_t n);
void t_hmac_wipe(void *p, size_t n);

#endif /* T_HMAC_H */
