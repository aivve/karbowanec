/* chacha20poly1305.c — ChaCha20-Poly1305 IETF (RFC 8439).
 *
 * Public domain. See UPSTREAM.md for sources.
 *
 *   ChaCha20 : adapted from RFC 8439 §2.3–§2.4 reference (DJB, public domain)
 *   Poly1305 : poly1305-donna 32-bit, by Andrew Moon (public domain)
 *              https://github.com/floodyberry/poly1305-donna
 *   AEAD     : RFC 8439 §2.8 construction
 *
 * Pure portable C99. No SIMD, no platform-specific intrinsics.
 */

#include "chacha20poly1305.h"

#include <string.h>

/* ===========================================================================
 * Little-endian helpers
 * ======================================================================== */

static inline uint32_t LE_LOAD32(const uint8_t* p) {
    return ((uint32_t)p[0])       |
           ((uint32_t)p[1] <<  8) |
           ((uint32_t)p[2] << 16) |
           ((uint32_t)p[3] << 24);
}

static inline void LE_STORE32(uint8_t* p, uint32_t v) {
    p[0] = (uint8_t)(v      );
    p[1] = (uint8_t)(v >>  8);
    p[2] = (uint8_t)(v >> 16);
    p[3] = (uint8_t)(v >> 24);
}

static inline void LE_STORE64(uint8_t* p, uint64_t v) {
    LE_STORE32(p,     (uint32_t)(v));
    LE_STORE32(p + 4, (uint32_t)(v >> 32));
}

static inline uint32_t ROTL32(uint32_t v, unsigned n) {
    return (v << n) | (v >> (32 - n));
}

/* ===========================================================================
 * ChaCha20 (RFC 8439 §2.3, §2.4)
 * ======================================================================== */

#define QR(a, b, c, d) do {                       \
    a += b; d ^= a; d = ROTL32(d, 16);            \
    c += d; b ^= c; b = ROTL32(b, 12);            \
    a += b; d ^= a; d = ROTL32(d,  8);            \
    c += d; b ^= c; b = ROTL32(b,  7);            \
} while (0)

/* Generate one 64-byte block of ChaCha20 keystream into `out`. */
static void chacha20_block(uint8_t out[64],
                           const uint8_t key[32],
                           uint32_t counter,
                           const uint8_t nonce[12]) {
    /* Initial state: 4 constants || 8 key words || 1 counter || 3 nonce */
    uint32_t s[16];
    s[ 0] = 0x61707865;  /* "expa" */
    s[ 1] = 0x3320646e;  /* "nd 3" */
    s[ 2] = 0x79622d32;  /* "2-by" */
    s[ 3] = 0x6b206574;  /* "te k" */
    s[ 4] = LE_LOAD32(key +  0);
    s[ 5] = LE_LOAD32(key +  4);
    s[ 6] = LE_LOAD32(key +  8);
    s[ 7] = LE_LOAD32(key + 12);
    s[ 8] = LE_LOAD32(key + 16);
    s[ 9] = LE_LOAD32(key + 20);
    s[10] = LE_LOAD32(key + 24);
    s[11] = LE_LOAD32(key + 28);
    s[12] = counter;
    s[13] = LE_LOAD32(nonce + 0);
    s[14] = LE_LOAD32(nonce + 4);
    s[15] = LE_LOAD32(nonce + 8);

    uint32_t x[16];
    memcpy(x, s, sizeof(x));

    /* 20 rounds = 10 (column + diagonal) double rounds */
    for (int i = 0; i < 10; ++i) {
        /* Column rounds */
        QR(x[0], x[4], x[ 8], x[12]);
        QR(x[1], x[5], x[ 9], x[13]);
        QR(x[2], x[6], x[10], x[14]);
        QR(x[3], x[7], x[11], x[15]);
        /* Diagonal rounds */
        QR(x[0], x[5], x[10], x[15]);
        QR(x[1], x[6], x[11], x[12]);
        QR(x[2], x[7], x[ 8], x[13]);
        QR(x[3], x[4], x[ 9], x[14]);
    }

    for (int i = 0; i < 16; ++i) {
        x[i] += s[i];
        LE_STORE32(out + i * 4, x[i]);
    }
}

#undef QR

/* XOR a byte stream with ChaCha20 keystream starting at `counter`. */
static void chacha20_xor(uint8_t* dst, const uint8_t* src, size_t len,
                         const uint8_t key[32], uint32_t counter,
                         const uint8_t nonce[12]) {
    uint8_t block[64];
    while (len > 0) {
        chacha20_block(block, key, counter, nonce);
        size_t take = len < 64 ? len : 64;
        if (src) {
            for (size_t i = 0; i < take; ++i) {
                dst[i] = src[i] ^ block[i];
            }
            src += take;
        } else {
            /* src == NULL → produce raw keystream */
            memcpy(dst, block, take);
        }
        dst += take;
        len -= take;
        counter++;
    }
}

/* ===========================================================================
 * Poly1305 (poly1305-donna 32-bit, by Andrew Moon, public domain)
 *
 * Adapted with minor cleanups for embedding. Functionally equivalent to
 * the upstream reference; verified against RFC 7539 / RFC 8439 KAT.
 * ======================================================================== */

typedef struct {
    uint32_t r[5];      /* clamped r, split into 5 26-bit limbs   */
    uint32_t h[5];      /* accumulator,                           */
    uint32_t pad[4];    /* s, the "final add" key half            */
    size_t   leftover;  /* bytes in `buffer` waiting for absorb   */
    uint8_t  buffer[16];
    uint8_t  finalized; /* once 1, no more updates allowed        */
} poly1305_ctx;

static void poly1305_init(poly1305_ctx* ctx, const uint8_t key[32]) {
    /* r = key[0..16] with clamp from RFC 7539 §2.5.1 */
    ctx->r[0] = (LE_LOAD32(key +  0)     ) & 0x3ffffff;
    ctx->r[1] = (LE_LOAD32(key +  3) >> 2) & 0x3ffff03;
    ctx->r[2] = (LE_LOAD32(key +  6) >> 4) & 0x3ffc0ff;
    ctx->r[3] = (LE_LOAD32(key +  9) >> 6) & 0x3f03fff;
    ctx->r[4] = (LE_LOAD32(key + 12) >> 8) & 0x00fffff;

    /* h = 0 */
    for (int i = 0; i < 5; ++i) ctx->h[i] = 0;

    /* save pad = key[16..32] for the final add */
    ctx->pad[0] = LE_LOAD32(key + 16);
    ctx->pad[1] = LE_LOAD32(key + 20);
    ctx->pad[2] = LE_LOAD32(key + 24);
    ctx->pad[3] = LE_LOAD32(key + 28);

    ctx->leftover  = 0;
    ctx->finalized = 0;
}

/* Process some number of complete 16-byte blocks.
 * `final_block` should be 0 except for the very last (possibly short) block,
 * where the message-end framing changes the high bit of the absorbed block. */
static void poly1305_blocks(poly1305_ctx* ctx,
                            const uint8_t* m, size_t bytes,
                            int final_block) {
    const uint32_t hibit = final_block ? 0 : (UINT32_C(1) << 24);
    uint32_t r0 = ctx->r[0], r1 = ctx->r[1], r2 = ctx->r[2],
             r3 = ctx->r[3], r4 = ctx->r[4];
    uint32_t s1 = r1 * 5, s2 = r2 * 5, s3 = r3 * 5, s4 = r4 * 5;
    uint32_t h0 = ctx->h[0], h1 = ctx->h[1], h2 = ctx->h[2],
             h3 = ctx->h[3], h4 = ctx->h[4];

    while (bytes >= 16) {
        /* h += m[i] (with high bit) */
        h0 += (LE_LOAD32(m +  0)     ) & 0x3ffffff;
        h1 += (LE_LOAD32(m +  3) >> 2) & 0x3ffffff;
        h2 += (LE_LOAD32(m +  6) >> 4) & 0x3ffffff;
        h3 += (LE_LOAD32(m +  9) >> 6) & 0x3ffffff;
        h4 += (LE_LOAD32(m + 12) >> 8) | hibit;

        /* h *= r mod (2^130 - 5) */
        uint64_t d0 = (uint64_t)h0 * r0 + (uint64_t)h1 * s4 +
                      (uint64_t)h2 * s3 + (uint64_t)h3 * s2 +
                      (uint64_t)h4 * s1;
        uint64_t d1 = (uint64_t)h0 * r1 + (uint64_t)h1 * r0 +
                      (uint64_t)h2 * s4 + (uint64_t)h3 * s3 +
                      (uint64_t)h4 * s2;
        uint64_t d2 = (uint64_t)h0 * r2 + (uint64_t)h1 * r1 +
                      (uint64_t)h2 * r0 + (uint64_t)h3 * s4 +
                      (uint64_t)h4 * s3;
        uint64_t d3 = (uint64_t)h0 * r3 + (uint64_t)h1 * r2 +
                      (uint64_t)h2 * r1 + (uint64_t)h3 * r0 +
                      (uint64_t)h4 * s4;
        uint64_t d4 = (uint64_t)h0 * r4 + (uint64_t)h1 * r3 +
                      (uint64_t)h2 * r2 + (uint64_t)h3 * r1 +
                      (uint64_t)h4 * r0;

        uint32_t c;
        c  = (uint32_t)(d0 >> 26); h0 = (uint32_t)d0 & 0x3ffffff;
        d1 += c;
        c  = (uint32_t)(d1 >> 26); h1 = (uint32_t)d1 & 0x3ffffff;
        d2 += c;
        c  = (uint32_t)(d2 >> 26); h2 = (uint32_t)d2 & 0x3ffffff;
        d3 += c;
        c  = (uint32_t)(d3 >> 26); h3 = (uint32_t)d3 & 0x3ffffff;
        d4 += c;
        c  = (uint32_t)(d4 >> 26); h4 = (uint32_t)d4 & 0x3ffffff;
        h0 += c * 5;
        c  = h0 >> 26;             h0 = h0 & 0x3ffffff;
        h1 += c;

        m     += 16;
        bytes -= 16;
    }

    ctx->h[0] = h0;
    ctx->h[1] = h1;
    ctx->h[2] = h2;
    ctx->h[3] = h3;
    ctx->h[4] = h4;
}

static void poly1305_update(poly1305_ctx* ctx, const uint8_t* m, size_t bytes) {
    /* Drain anything in the leftover buffer first. */
    if (ctx->leftover) {
        size_t want = 16 - ctx->leftover;
        if (want > bytes) want = bytes;
        memcpy(ctx->buffer + ctx->leftover, m, want);
        bytes        -= want;
        m            += want;
        ctx->leftover += want;
        if (ctx->leftover < 16) return;
        poly1305_blocks(ctx, ctx->buffer, 16, 0);
        ctx->leftover = 0;
    }
    /* Process full blocks straight from `m`. */
    if (bytes >= 16) {
        size_t whole = bytes & ~(size_t)15;
        poly1305_blocks(ctx, m, whole, 0);
        m     += whole;
        bytes -= whole;
    }
    /* Stash the remainder for the next update / finish. */
    if (bytes) {
        memcpy(ctx->buffer + ctx->leftover, m, bytes);
        ctx->leftover += bytes;
    }
}

static void poly1305_finish(poly1305_ctx* ctx, uint8_t tag[16]) {
    /* Process the final, possibly short, block. */
    if (ctx->leftover) {
        size_t i = ctx->leftover;
        ctx->buffer[i++] = 1;
        for (; i < 16; ++i) ctx->buffer[i] = 0;
        poly1305_blocks(ctx, ctx->buffer, 16, 1);
    }

    /* Fully reduce h. */
    uint32_t h0 = ctx->h[0], h1 = ctx->h[1], h2 = ctx->h[2],
             h3 = ctx->h[3], h4 = ctx->h[4];

    uint32_t c;
    c = h1 >> 26; h1 &= 0x3ffffff; h2 += c;
    c = h2 >> 26; h2 &= 0x3ffffff; h3 += c;
    c = h3 >> 26; h3 &= 0x3ffffff; h4 += c;
    c = h4 >> 26; h4 &= 0x3ffffff; h0 += c * 5;
    c = h0 >> 26; h0 &= 0x3ffffff; h1 += c;

    /* Compute h - p (where p = 2^130 - 5); take whichever is in [0, p). */
    uint32_t g0 = h0 + 5;        c = g0 >> 26; g0 &= 0x3ffffff;
    uint32_t g1 = h1 + c;        c = g1 >> 26; g1 &= 0x3ffffff;
    uint32_t g2 = h2 + c;        c = g2 >> 26; g2 &= 0x3ffffff;
    uint32_t g3 = h3 + c;        c = g3 >> 26; g3 &= 0x3ffffff;
    uint32_t g4 = h4 + c - (UINT32_C(1) << 26);

    /* If g4 underflowed (high bit set), keep h; else use g. */
    uint32_t mask = (g4 >> 31) - 1;  /* 0xffffffff if g >= p, 0 otherwise */
    g0 &= mask; g1 &= mask; g2 &= mask; g3 &= mask; g4 &= mask;
    mask = ~mask;
    h0 = (h0 & mask) | g0;
    h1 = (h1 & mask) | g1;
    h2 = (h2 & mask) | g2;
    h3 = (h3 & mask) | g3;
    h4 = (h4 & mask) | g4;

    /* Re-pack 5 26-bit limbs into 4 32-bit words. */
    uint32_t f0 = (h0      ) | (h1 << 26);
    uint32_t f1 = (h1 >>  6) | (h2 << 20);
    uint32_t f2 = (h2 >> 12) | (h3 << 14);
    uint32_t f3 = (h3 >> 18) | (h4 <<  8);

    /* tag = (h + s) mod 2^128, serialized little-endian. */
    uint64_t t = (uint64_t)f0 + ctx->pad[0]; f0 = (uint32_t)t;
    t = (uint64_t)f1 + ctx->pad[1] + (t >> 32); f1 = (uint32_t)t;
    t = (uint64_t)f2 + ctx->pad[2] + (t >> 32); f2 = (uint32_t)t;
    t = (uint64_t)f3 + ctx->pad[3] + (t >> 32); f3 = (uint32_t)t;

    LE_STORE32(tag +  0, f0);
    LE_STORE32(tag +  4, f1);
    LE_STORE32(tag +  8, f2);
    LE_STORE32(tag + 12, f3);

    /* Wipe state — defence in depth, not constant-time guarantee. */
    volatile uint8_t* p = (volatile uint8_t*)ctx;
    for (size_t i = 0; i < sizeof(*ctx); ++i) p[i] = 0;
}

/* ===========================================================================
 * AEAD construction (RFC 8439 §2.8)
 * ======================================================================== */

static void poly1305_pad16(poly1305_ctx* ctx, size_t already) {
    static const uint8_t zeros[16] = {0};
    size_t pad = (16 - (already & 15)) & 15;
    if (pad) poly1305_update(ctx, zeros, pad);
}

static int constant_time_eq16(const uint8_t* a, const uint8_t* b) {
    uint8_t diff = 0;
    for (size_t i = 0; i < 16; ++i) diff |= (uint8_t)(a[i] ^ b[i]);
    return diff == 0;
}

int chacha20poly1305_ietf_encrypt(
    uint8_t* out,
    const uint8_t* aad, size_t aad_len,
    const uint8_t* msg, size_t msg_len,
    const uint8_t key[32],
    const uint8_t nonce[12])
{
    /* otk = first 32 bytes of ChaCha20(key, counter=0, nonce) */
    uint8_t otk[64];
    chacha20_block(otk, key, 0, nonce);

    /* ciphertext = msg XOR ChaCha20(key, counter=1, nonce) */
    chacha20_xor(out, msg, msg_len, key, 1, nonce);

    /* MAC over: aad || pad16 || ct || pad16 || LE64(aad_len) || LE64(ct_len) */
    poly1305_ctx pctx;
    poly1305_init(&pctx, otk);
    if (aad_len) poly1305_update(&pctx, aad, aad_len);
    poly1305_pad16(&pctx, aad_len);
    if (msg_len) poly1305_update(&pctx, out, msg_len);
    poly1305_pad16(&pctx, msg_len);

    uint8_t lengths[16];
    LE_STORE64(lengths,     (uint64_t)aad_len);
    LE_STORE64(lengths + 8, (uint64_t)msg_len);
    poly1305_update(&pctx, lengths, 16);

    poly1305_finish(&pctx, out + msg_len);

    /* Wipe one-time key */
    volatile uint8_t* p = (volatile uint8_t*)otk;
    for (size_t i = 0; i < sizeof(otk); ++i) p[i] = 0;
    return 0;
}

int chacha20poly1305_ietf_decrypt(
    uint8_t* out,
    const uint8_t* aad, size_t aad_len,
    const uint8_t* in, size_t in_len,
    const uint8_t key[32],
    const uint8_t nonce[12])
{
    if (in_len < 16) return -1;
    size_t ct_len = in_len - 16;

    uint8_t otk[64];
    chacha20_block(otk, key, 0, nonce);

    /* Verify tag BEFORE writing plaintext (defence against tag-failure leaks). */
    poly1305_ctx pctx;
    poly1305_init(&pctx, otk);
    if (aad_len) poly1305_update(&pctx, aad, aad_len);
    poly1305_pad16(&pctx, aad_len);
    if (ct_len) poly1305_update(&pctx, in, ct_len);
    poly1305_pad16(&pctx, ct_len);

    uint8_t lengths[16];
    LE_STORE64(lengths,     (uint64_t)aad_len);
    LE_STORE64(lengths + 8, (uint64_t)ct_len);
    poly1305_update(&pctx, lengths, 16);

    uint8_t tag[16];
    poly1305_finish(&pctx, tag);

    int ok = constant_time_eq16(tag, in + ct_len);

    /* Wipe one-time key + computed tag regardless of outcome */
    volatile uint8_t* p = (volatile uint8_t*)otk;
    for (size_t i = 0; i < sizeof(otk); ++i) p[i] = 0;
    p = (volatile uint8_t*)tag;
    for (size_t i = 0; i < sizeof(tag); ++i) p[i] = 0;

    if (!ok) return -1;

    /* Tag good — produce plaintext. */
    chacha20_xor(out, in, ct_len, key, 1, nonce);
    return 0;
}
