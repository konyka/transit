#include "t_test.h"
#include "t_hmac.h"
#include <string.h>
#include <stdio.h>
#include <stdint.h>

static int parse_hex(const char *hex, uint8_t *out, size_t n) {
    if (!hex || !out) return -1;
    for (size_t i = 0; i < n; i++) {
        unsigned v = 0;
        if (sscanf(hex + i * 2, "%2x", &v) != 1) return -1;
        out[i] = (uint8_t)v;
    }
    return 0;
}

T_TEST(sha256_empty_and_abc) {
    uint8_t out[32], expect[32];
    T_ASSERT_EQ(t_sha256(out, NULL, 0), 0);
    T_ASSERT_EQ(parse_hex("e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
                          expect, 32), 0);
    T_ASSERT_MEM_EQ(out, expect, 32);
    T_ASSERT_EQ(t_sha256(out, (const uint8_t *)"abc", 3), 0);
    T_ASSERT_EQ(parse_hex("ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad",
                          expect, 32), 0);
    T_ASSERT_MEM_EQ(out, expect, 32);
}

T_TEST(hmac_sha256_rfc4231_1) {
    uint8_t key[20];
    memset(key, 0x0b, sizeof(key));
    uint8_t out[32], expect[32];
    T_ASSERT_EQ(t_hmac_sha256(out, key, sizeof(key),
                              (const uint8_t *)"Hi There", 8), 0);
    T_ASSERT_EQ(parse_hex("b0344c61d8db38535ca8afceaf0bf12b881dc200c9833da726e9376c2e32cff7",
                          expect, 32), 0);
    T_ASSERT_MEM_EQ(out, expect, 32);
}

T_TEST(hmac_sha256_rfc4231_2) {
    uint8_t out[32], expect[32];
    T_ASSERT_EQ(t_hmac_sha256(out, (const uint8_t *)"Jefe", 4,
                              (const uint8_t *)"what do ya want for nothing?", 28), 0);
    T_ASSERT_EQ(parse_hex("5bdcc146bf60754e6a042426089575c75a003f089d2739839dec58b964ec3843",
                          expect, 32), 0);
    T_ASSERT_MEM_EQ(out, expect, 32);
}

T_TEST(auth_mac_stable_and_equal) {
    const uint8_t psk[] = "s3cret";
    uint8_t a[32], b[32];
    T_ASSERT_EQ(t_auth_mac(a, psk, sizeof(psk) - 1), 0);
    T_ASSERT_EQ(t_auth_mac(b, psk, sizeof(psk) - 1), 0);
    T_ASSERT(t_hmac_equal(a, b, 32));
    b[0] ^= 1;
    T_ASSERT(!t_hmac_equal(a, b, 32));
    T_ASSERT(t_auth_mac(a, NULL, 0) != 0);
}

int main(void) {
    return t_run_all_tests();
}
