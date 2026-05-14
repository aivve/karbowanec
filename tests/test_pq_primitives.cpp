// Copyright (c) 2026, The Karbo developers
//
// This file is part of Karbo.
//
// KAT-based tests for the PQ Phase 1 crypto primitives. Vectors:
//   * SHA3-256          : FIPS 202 known answers (empty / "abc")
//   * HMAC-SHA3-256     : NIST CAVP "Jefe / what do ya want for nothing?"
//   * HKDF-SHA3-256     : internal consistency + length-extension
//   * ChaCha20-Poly1305 : RFC 8439 §2.8.2 (the canonical AEAD KAT)
//   * ML-KEM-768        : encaps/decaps round-trip, deterministic keygen,
//                         FIPS 203 implicit-rejection on garbage ciphertext
//   * ML-DSA-65         : sign/verify round-trip, deterministic keygen,
//                         signature/message/key tamper rejection
//
// liboqs's own KAT suite covers the lattice primitives byte-exactly against
// the FIPS 203 / 204 reference vectors; we focus here on (a) round-trip
// correctness through our wrappers and (b) determinism of the seed-driven
// keygens that Karbo PQ depends on.

#include "gtest/gtest.h"

#include "crypto_pq/PqHash.h"
#include "crypto_pq/PqAead.h"
#include "crypto_pq/PqKem.h"
#include "crypto_pq/PqDsa.h"

#include <array>
#include <cstdint>
#include <cstring>
#include <string>
#include <vector>

namespace {

std::string to_hex(const uint8_t* data, std::size_t len) {
    static const char* h = "0123456789abcdef";
    std::string out;
    out.reserve(2 * len);
    for (std::size_t i = 0; i < len; ++i) {
        out += h[data[i] >> 4];
        out += h[data[i] & 0xf];
    }
    return out;
}

template <std::size_t N>
std::string to_hex(const std::array<uint8_t, N>& a) {
    return to_hex(a.data(), N);
}

}  // namespace

// ===========================================================================
// SHA3-256 — FIPS 202 known answers
// ===========================================================================

TEST(PqHash_Sha3_256, FIPS_202_Empty) {
    auto h = CryptoPQ::sha3_256("", 0);
    EXPECT_EQ(to_hex(h),
              "a7ffc6f8bf1ed76651c14756a061d662f580ff4de43b49fa82d80a4b80f8434a");
}

TEST(PqHash_Sha3_256, FIPS_202_Abc) {
    auto h = CryptoPQ::sha3_256("abc", 3);
    EXPECT_EQ(to_hex(h),
              "3a985da74fe225b2045c172d6bd390bd855f086e3e9d525b46bfe24511431532");
}

// ===========================================================================
// HMAC-SHA3-256 — NIST CAVP-style vector
//
// Same key/message convention as RFC 4231 Test Case 2, applied with SHA-3.
// ===========================================================================

TEST(PqHash_Hmac, NIST_Jefe_WhatDoYaWant) {
    const char* key = "Jefe";
    const char* msg = "what do ya want for nothing?";
    auto h = CryptoPQ::hmac_sha3_256(key, 4, msg, std::strlen(msg));
    EXPECT_EQ(to_hex(h),
              "c7d4072e788877ae3596bbb0da73b887c9171f93095b294ae857fbe2645e1ba5");
}

TEST(PqHash_Hmac, EmptyKeyEmptyMsgIsDeterministic) {
    // We deliberately do NOT hardcode the expected value here: NIST has
    // not published a HMAC-SHA3-256 KAT for the empty/empty case in a
    // form we can cite verbatim, and the well-known Jefe KAT above is
    // already strong external evidence the construction is correct.
    // This test instead asserts that the empty/empty result is stable
    // (same call twice → same bytes), which catches accidental state
    // pollution between invocations.
    auto h1 = CryptoPQ::hmac_sha3_256("", 0, "", 0);
    auto h2 = CryptoPQ::hmac_sha3_256("", 0, "", 0);
    EXPECT_EQ(h1, h2);
}

// ===========================================================================
// HKDF-SHA3-256 — internal consistency
// ===========================================================================

TEST(PqHash_Hkdf, DefaultIsDeterministic) {
    uint8_t ikm[32] = {0};
    ikm[0] = 1; ikm[31] = 0xff;
    auto a = CryptoPQ::hkdf_sha3_256(ikm, sizeof(ikm), "info", 4);
    auto b = CryptoPQ::hkdf_sha3_256(ikm, sizeof(ikm), "info", 4);
    EXPECT_EQ(a, b);
}

TEST(PqHash_Hkdf, InfoChangesOutput) {
    uint8_t ikm[32] = {0};
    auto a = CryptoPQ::hkdf_sha3_256(ikm, sizeof(ikm), "info1", 5);
    auto b = CryptoPQ::hkdf_sha3_256(ikm, sizeof(ikm), "info2", 5);
    EXPECT_NE(a, b);
}

TEST(PqHash_Hkdf, IkmChangesOutput) {
    uint8_t ikm1[32] = {0}; ikm1[0] = 1;
    uint8_t ikm2[32] = {0}; ikm2[0] = 2;
    auto a = CryptoPQ::hkdf_sha3_256(ikm1, sizeof(ikm1), "x", 1);
    auto b = CryptoPQ::hkdf_sha3_256(ikm2, sizeof(ikm2), "x", 1);
    EXPECT_NE(a, b);
}

TEST(PqHash_Hkdf, ExpandToL96MatchesL32Prefix) {
    // HKDF-Expand: T(1) is the first 32 bytes regardless of total L,
    // so an L=96 expansion's first 32 bytes must equal the L=32 default.
    uint8_t ikm[32] = {0xab};
    std::vector<uint8_t> okm(96);
    bool ok = CryptoPQ::hkdf_sha3_256_explicit(
        ikm, sizeof(ikm), nullptr, 0, "extend", 6,
        okm.data(), okm.size());
    ASSERT_TRUE(ok);
    auto h32 = CryptoPQ::hkdf_sha3_256(ikm, sizeof(ikm), "extend", 6);
    EXPECT_EQ(0, std::memcmp(okm.data(), h32.data(), 32));
}

TEST(PqHash_Hkdf, ExpandTooLargeRejected) {
    uint8_t ikm[1] = {0};
    // RFC 5869 caps L at 255 * HashLen.
    std::vector<uint8_t> okm(255 * 32 + 1);
    bool ok = CryptoPQ::hkdf_sha3_256_explicit(
        ikm, 1, nullptr, 0, "x", 1, okm.data(), okm.size());
    EXPECT_FALSE(ok);
}

TEST(PqHash_Hkdf, ZeroLengthRejected) {
    uint8_t ikm[1] = {0};
    uint8_t okm[1];
    bool ok = CryptoPQ::hkdf_sha3_256_explicit(
        ikm, 1, nullptr, 0, "x", 1, okm, 0);
    EXPECT_FALSE(ok);
}

TEST(PqHash_Hkdf, ExplicitZeroSaltMatchesNullSalt) {
    // RFC 5869 §2.2: empty salt should be treated as HashLen zero bytes.
    // Our wrapper does this automatically when salt==nullptr; an explicit
    // 32-byte zero salt must produce the same output.
    uint8_t ikm[8] = {1, 2, 3, 4, 5, 6, 7, 8};
    uint8_t zero_salt[32] = {0};
    uint8_t a[32], b[32];
    ASSERT_TRUE(CryptoPQ::hkdf_sha3_256_explicit(
        ikm, sizeof(ikm), nullptr, 0, "info", 4, a, sizeof(a)));
    ASSERT_TRUE(CryptoPQ::hkdf_sha3_256_explicit(
        ikm, sizeof(ikm), zero_salt, sizeof(zero_salt), "info", 4, b, sizeof(b)));
    EXPECT_EQ(0, std::memcmp(a, b, 32));
}

// ===========================================================================
// ChaCha20-Poly1305 IETF — RFC 8439 §2.8.2 KAT
// ===========================================================================

TEST(PqAead, RFC8439_Section_2_8_2) {
    const std::string plaintext =
        "Ladies and Gentlemen of the class of '99: If I could offer you "
        "only one tip for the future, sunscreen would be it.";
    ASSERT_EQ(plaintext.size(), 114u);

    CryptoPQ::AeadKey key{
        0x80, 0x81, 0x82, 0x83, 0x84, 0x85, 0x86, 0x87,
        0x88, 0x89, 0x8a, 0x8b, 0x8c, 0x8d, 0x8e, 0x8f,
        0x90, 0x91, 0x92, 0x93, 0x94, 0x95, 0x96, 0x97,
        0x98, 0x99, 0x9a, 0x9b, 0x9c, 0x9d, 0x9e, 0x9f,
    };
    CryptoPQ::AeadNonce nonce{
        0x07, 0x00, 0x00, 0x00,
        0x40, 0x41, 0x42, 0x43, 0x44, 0x45, 0x46, 0x47,
    };
    const uint8_t aad[] = {
        0x50, 0x51, 0x52, 0x53,
        0xc0, 0xc1, 0xc2, 0xc3, 0xc4, 0xc5, 0xc6, 0xc7,
    };

    const std::string expected_ct =
        "d31a8d34648e60db7b86afbc53ef7ec2a4aded51296e08fea9e2b5a736ee62d6"
        "3dbea45e8ca967128 2fafb69da92728b1a71de0a9e060b2905d6a5b67ecd3b3"
        "692ddbd7f2d778b8c9803aee328091b58fab324e4fad675945585808b4831d7b"
        "c3ff4def08e4b7a9de576d26586cec64b6116";
    // Strip whitespace from the literal above (compile-time formatting).
    std::string expected_ct_clean;
    for (char c : expected_ct) if (c != ' ') expected_ct_clean += c;
    const std::string expected_tag = "1ae10b594f09e26a7e902ecbd0600691";

    auto out = CryptoPQ::aead_encrypt(
        key, nonce,
        aad, sizeof(aad),
        plaintext.data(), plaintext.size());

    ASSERT_EQ(out.size(), plaintext.size() + 16);
    EXPECT_EQ(to_hex(out.data(), plaintext.size()), expected_ct_clean);
    EXPECT_EQ(to_hex(out.data() + plaintext.size(), 16), expected_tag);

    // Round-trip
    auto opt = CryptoPQ::aead_decrypt(
        key, nonce,
        aad, sizeof(aad),
        out.data(), out.size());
    ASSERT_TRUE(opt.has_value());
    EXPECT_EQ(std::string(opt->begin(), opt->end()), plaintext);
}

TEST(PqAead, TamperedTagRejected) {
    CryptoPQ::AeadKey   key{};
    CryptoPQ::AeadNonce nonce{};
    auto out = CryptoPQ::aead_encrypt(key, nonce, nullptr, 0, "hi", 2);
    out.back() ^= 0x01;  // flip a bit in the tag
    auto opt = CryptoPQ::aead_decrypt(key, nonce, nullptr, 0, out.data(), out.size());
    EXPECT_FALSE(opt.has_value());
}

TEST(PqAead, TamperedCiphertextRejected) {
    CryptoPQ::AeadKey   key{};
    CryptoPQ::AeadNonce nonce{};
    auto out = CryptoPQ::aead_encrypt(key, nonce, nullptr, 0, "hello", 5);
    out[0] ^= 0x01;  // flip a bit in the ciphertext
    auto opt = CryptoPQ::aead_decrypt(key, nonce, nullptr, 0, out.data(), out.size());
    EXPECT_FALSE(opt.has_value());
}

TEST(PqAead, TamperedAadRejected) {
    CryptoPQ::AeadKey   key{};
    CryptoPQ::AeadNonce nonce{};
    const uint8_t aad_enc[] = {1, 2, 3};
    const uint8_t aad_dec[] = {1, 2, 4};
    auto out = CryptoPQ::aead_encrypt(key, nonce, aad_enc, sizeof(aad_enc), "hi", 2);
    auto opt = CryptoPQ::aead_decrypt(key, nonce, aad_dec, sizeof(aad_dec), out.data(), out.size());
    EXPECT_FALSE(opt.has_value());
}

TEST(PqAead, EmptyMessageOk) {
    CryptoPQ::AeadKey   key{};
    CryptoPQ::AeadNonce nonce{};
    auto out = CryptoPQ::aead_encrypt(key, nonce, nullptr, 0, nullptr, 0);
    EXPECT_EQ(out.size(), 16u);  // tag only
    auto opt = CryptoPQ::aead_decrypt(key, nonce, nullptr, 0, out.data(), out.size());
    ASSERT_TRUE(opt.has_value());
    EXPECT_TRUE(opt->empty());
}

// ===========================================================================
// ML-KEM-768
// ===========================================================================

TEST(PqKem_768, EncapsDecapsRoundTrip) {
    auto [pub, sk] = CryptoPQ::kem_keygen();
    auto [ct, ss_send] = CryptoPQ::kem_encaps(pub);
    auto ss_recv = CryptoPQ::kem_decaps(sk, ct);
    EXPECT_EQ(ss_send, ss_recv);
}

TEST(PqKem_768, DeterministicKeygenReproducible) {
    CryptoPQ::KemKeypairSeed seed{};
    seed[0] = 1; seed[31] = 0x55; seed[63] = 0xff;
    auto [pub1, sk1] = CryptoPQ::kem_keygen_from_seed(seed);
    auto [pub2, sk2] = CryptoPQ::kem_keygen_from_seed(seed);
    EXPECT_EQ(pub1, pub2);
    EXPECT_EQ(sk1, sk2);
}

TEST(PqKem_768, DeterministicKeygenSeedSensitive) {
    CryptoPQ::KemKeypairSeed seed1{}; seed1[0] = 1;
    CryptoPQ::KemKeypairSeed seed2{}; seed2[0] = 2;
    auto [pub1, sk1] = CryptoPQ::kem_keygen_from_seed(seed1);
    auto [pub2, sk2] = CryptoPQ::kem_keygen_from_seed(seed2);
    EXPECT_NE(pub1, pub2);
}

TEST(PqKem_768, DecapsOnGarbageCiphertextDoesNotThrow) {
    // FIPS 203 implicit rejection: malformed ct must yield a pseudorandom
    // ss, not an error. The wallet scan path relies on this — failure must
    // surface via the AEAD tag check, not via an exception here.
    auto [pub, sk] = CryptoPQ::kem_keygen();
    CryptoPQ::KemCiphertext garbage{};
    for (std::size_t i = 0; i < garbage.size(); ++i) {
        garbage[i] = static_cast<uint8_t>(i & 0xff);
    }
    EXPECT_NO_THROW({ (void)CryptoPQ::kem_decaps(sk, garbage); });
}

// ===========================================================================
// ML-DSA-65
// ===========================================================================

TEST(PqDsa_65, SignVerifyRoundTrip) {
    auto [pub, sk] = CryptoPQ::dsa_keygen();
    const char* msg = "the quick brown fox jumps over the lazy dog";
    auto sig = CryptoPQ::dsa_sign(sk, msg, std::strlen(msg));
    EXPECT_TRUE(CryptoPQ::dsa_verify(pub, msg, std::strlen(msg), sig));
}

TEST(PqDsa_65, DeterministicKeygenReproducible) {
    CryptoPQ::DsaKeypairSeed seed{};
    seed[0] = 7; seed[15] = 0x33; seed[31] = 0xab;
    auto [pub1, sk1] = CryptoPQ::dsa_keygen_from_seed(seed);
    auto [pub2, sk2] = CryptoPQ::dsa_keygen_from_seed(seed);
    EXPECT_EQ(pub1, pub2);
    EXPECT_EQ(sk1, sk2);
}

TEST(PqDsa_65, DeterministicKeygenSeedSensitive) {
    CryptoPQ::DsaKeypairSeed s1{}; s1[0] = 1;
    CryptoPQ::DsaKeypairSeed s2{}; s2[0] = 2;
    auto [pub1, sk1] = CryptoPQ::dsa_keygen_from_seed(s1);
    auto [pub2, sk2] = CryptoPQ::dsa_keygen_from_seed(s2);
    EXPECT_NE(pub1, pub2);
}

TEST(PqDsa_65, TamperedSignatureRejected) {
    auto [pub, sk] = CryptoPQ::dsa_keygen();
    const char* msg = "test";
    auto sig = CryptoPQ::dsa_sign(sk, msg, 4);
    sig[100] ^= 0x01;
    EXPECT_FALSE(CryptoPQ::dsa_verify(pub, msg, 4, sig));
}

TEST(PqDsa_65, TamperedMessageRejected) {
    auto [pub, sk] = CryptoPQ::dsa_keygen();
    auto sig = CryptoPQ::dsa_sign(sk, "test", 4);
    EXPECT_TRUE(CryptoPQ::dsa_verify(pub, "test", 4, sig));
    EXPECT_FALSE(CryptoPQ::dsa_verify(pub, "tesT", 4, sig));
}

TEST(PqDsa_65, WrongKeyRejected) {
    auto [pub1, sk1] = CryptoPQ::dsa_keygen();
    auto [pub2, sk2] = CryptoPQ::dsa_keygen();
    const char* msg = "test";
    auto sig = CryptoPQ::dsa_sign(sk1, msg, 4);
    EXPECT_TRUE(CryptoPQ::dsa_verify(pub1, msg, 4, sig));
    EXPECT_FALSE(CryptoPQ::dsa_verify(pub2, msg, 4, sig));
}

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
