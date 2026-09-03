#include "t_hmac.h"
#include <string.h>

static const uint32_t K256[64] = {
    0x428a2f98u, 0x71374491u, 0xb5c0fbcfu, 0xe9b5dba5u, 0x3956c25bu, 0x59f111f1u,
    0x923f82a4u, 0xab1c5ed5u, 0xd807aa98u, 0x12835b01u, 0x243185beu, 0x550c7dc3u,
    0x72be5d74u, 0x80deb1feu, 0x9bdc06a7u, 0xc19bf174u, 0xe49b69c1u, 0xefbe4786u,
    0x0fc19dc6u, 0x240ca1ccu, 0x2de92c6fu, 0x4a7484aau, 0x5cb0a9dcu, 0x76f988dau,
    0x983e5152u, 0xa831c66du, 0xb00327c8u, 0xbf597fc7u, 0xc6e00bf3u, 0xd5a79147u,
    0x06ca6351u, 0x14292967u, 0x27b70a85u, 0x2e1b2138u, 0x4d2c6dfcu, 0x53380d13u,
    0x650a7354u, 0x766a0abbu, 0x81c2c92eu, 0x92722c85u, 0xa2bfe8a1u, 0xa81a664bu,
    0xc24b8b70u, 0xc76c51a3u, 0xd192e819u, 0xd6990624u, 0xf40e3585u, 0x106aa070u,
    0x19a4c116u, 0x1e376c08u, 0x2748774cu, 0x34b0bcb5u, 0x391c0cb3u, 0x4ed8aa4au,
    0x5b9cca4fu, 0x682e6ff3u, 0x748f82eeu, 0x78a5636fu, 0x84c87814u, 0x8cc70208u,
    0x90befffau, 0xa4506cebu, 0xbef9a3f7u, 0xc67178f2u
};

static uint32_t rotr32(uint32_t x, unsigned n) {
    return (x >> n) | (x << (32u - n));
}

static uint32_t load_be32(const uint8_t *p) {
    return ((uint32_t)p[0] << 24) | ((uint32_t)p[1] << 16) |
           ((uint32_t)p[2] << 8) | (uint32_t)p[3];
}

static void store_be32(uint8_t *p, uint32_t v) {
    p[0] = (uint8_t)(v >> 24);
    p[1] = (uint8_t)(v >> 16);
    p[2] = (uint8_t)(v >> 8);
    p[3] = (uint8_t)v;
}

static void store_be64(uint8_t *p, uint64_t v) {
    store_be32(p, (uint32_t)(v >> 32));
    store_be32(p + 4, (uint32_t)v);
}

typedef struct {
    uint32_t h[8];
    uint64_t nbits;
    uint8_t  buf[64];
    size_t   nbuf;
} sha256_ctx;

static void sha256_init(sha256_ctx *c) {
    c->h[0] = 0x6a09e667u;
    c->h[1] = 0xbb67ae85u;
    c->h[2] = 0x3c6ef372u;
    c->h[3] = 0xa54ff53au;
    c->h[4] = 0x510e527fu;
    c->h[5] = 0x9b05688cu;
    c->h[6] = 0x1f83d9abu;
    c->h[7] = 0x5be0cd19u;
    c->nbits = 0;
    c->nbuf = 0;
}

static void sha256_block(sha256_ctx *c, const uint8_t blk[64]) {
    uint32_t w[64];
    for (int i = 0; i < 16; i++)
        w[i] = load_be32(blk + (size_t)i * 4);
    for (int i = 16; i < 64; i++) {
        uint32_t s0 = rotr32(w[i - 15], 7) ^ rotr32(w[i - 15], 18) ^ (w[i - 15] >> 3);
        uint32_t s1 = rotr32(w[i - 2], 17) ^ rotr32(w[i - 2], 19) ^ (w[i - 2] >> 10);
        w[i] = w[i - 16] + s0 + w[i - 7] + s1;
    }
    uint32_t a = c->h[0], b = c->h[1], cc = c->h[2], d = c->h[3];
    uint32_t e = c->h[4], f = c->h[5], g = c->h[6], h = c->h[7];
    for (int i = 0; i < 64; i++) {
        uint32_t S1 = rotr32(e, 6) ^ rotr32(e, 11) ^ rotr32(e, 25);
        uint32_t ch = (e & f) ^ ((~e) & g);
        uint32_t t1 = h + S1 + ch + K256[i] + w[i];
        uint32_t S0 = rotr32(a, 2) ^ rotr32(a, 13) ^ rotr32(a, 22);
        uint32_t maj = (a & b) ^ (a & cc) ^ (b & cc);
        uint32_t t2 = S0 + maj;
        h = g;
        g = f;
        f = e;
        e = d + t1;
        d = cc;
        cc = b;
        b = a;
        a = t1 + t2;
    }
    c->h[0] += a; c->h[1] += b; c->h[2] += cc; c->h[3] += d;
    c->h[4] += e; c->h[5] += f; c->h[6] += g; c->h[7] += h;
}

static int sha256_update(sha256_ctx *c, const uint8_t *data, size_t len) {
    if (len && !data) return -1;
    if (len > UINT64_MAX / 8) return -1;
    if (c->nbits > UINT64_MAX - (uint64_t)len * 8) return -1;
    c->nbits += (uint64_t)len * 8;
    while (len > 0) {
        size_t room = 64 - c->nbuf;
        size_t n = len < room ? len : room;
        memcpy(c->buf + c->nbuf, data, n);
        c->nbuf += n;
        data += n;
        len -= n;
        if (c->nbuf == 64) {
            sha256_block(c, c->buf);
            c->nbuf = 0;
        }
    }
    return 0;
}

static int sha256_final(sha256_ctx *c, uint8_t out[32]) {
    uint8_t pad[64 + 8];
    size_t n = 0;
    pad[n++] = 0x80;
    size_t used = c->nbuf + 1;
    size_t zeros = (used <= 56) ? (56 - used) : (120 - used);
    memset(pad + n, 0, zeros);
    n += zeros;
    store_be64(pad + n, c->nbits);
    n += 8;
    if (sha256_update(c, pad, n) != 0) return -1;
    /* The length bytes already counted nbits; restore by not double-counting
     * is handled because we added pad after nbits was set for the message.
     * sha256_update also adds pad bits into nbits — that is fine; final
     * hash only needs the padded block processed. */
    for (int i = 0; i < 8; i++)
        store_be32(out + (size_t)i * 4, c->h[i]);
    return 0;
}

int t_sha256(uint8_t out[T_HMAC_SHA256_LEN], const uint8_t *data, size_t len) {
    if (!out) return -1;
    if (len > 0 && !data) return -1;
    sha256_ctx c;
    sha256_init(&c);
    if (sha256_update(&c, data, len) != 0) return -1;
    return sha256_final(&c, out);
}

int t_hmac_sha256(uint8_t out[T_HMAC_SHA256_LEN],
                  const uint8_t *key, size_t key_len,
                  const uint8_t *data, size_t data_len) {
    if (!out) return -1;
    if (key_len > 0 && !key) return -1;
    if (data_len > 0 && !data) return -1;
    uint8_t kpad[64];
    memset(kpad, 0, sizeof(kpad));
    if (key_len > 64) {
        if (t_sha256(kpad, key, key_len) != 0) return -1;
    } else if (key_len) {
        memcpy(kpad, key, key_len);
    }
    uint8_t ipad[64], opad[64];
    for (size_t i = 0; i < 64; i++) {
        ipad[i] = (uint8_t)(kpad[i] ^ 0x36u);
        opad[i] = (uint8_t)(kpad[i] ^ 0x5cu);
    }
    sha256_ctx inner;
    sha256_init(&inner);
    if (sha256_update(&inner, ipad, 64) != 0) return -1;
    if (sha256_update(&inner, data, data_len) != 0) return -1;
    uint8_t inner_hash[32];
    if (sha256_final(&inner, inner_hash) != 0) return -1;
    sha256_ctx outer;
    sha256_init(&outer);
    if (sha256_update(&outer, opad, 64) != 0) return -1;
    if (sha256_update(&outer, inner_hash, 32) != 0) return -1;
    int rc = sha256_final(&outer, out);
    t_hmac_wipe(kpad, sizeof(kpad));
    t_hmac_wipe(ipad, sizeof(ipad));
    t_hmac_wipe(opad, sizeof(opad));
    t_hmac_wipe(inner_hash, sizeof(inner_hash));
    return rc;
}

int t_auth_mac(uint8_t out[T_HMAC_SHA256_LEN], const uint8_t *psk, size_t psk_len) {
    if (!psk || psk_len == 0 || psk_len > T_AUTH_PSK_MAX) return -1;
    return t_hmac_sha256(out, psk, psk_len,
                         (const uint8_t *)T_AUTH_CONTEXT,
                         sizeof(T_AUTH_CONTEXT) - 1);
}

int t_hmac_equal(const uint8_t *a, const uint8_t *b, size_t n) {
    if (n == 0) return 1;
    if (!a || !b) return 0;
    uint8_t d = 0;
    for (size_t i = 0; i < n; i++)
        d = (uint8_t)(d | (a[i] ^ b[i]));
    return d == 0;
}

void t_hmac_wipe(void *p, size_t n) {
    volatile uint8_t *v = (volatile uint8_t *)p;
    if (!v) return;
    while (n--) *v++ = 0;
}
