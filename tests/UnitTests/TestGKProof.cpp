// Copyright (c) 2016-2026, The Karbo developers
//
// Unit tests for Groth-Kohlweiss one-of-many membership proof.

#include <gtest/gtest.h>

#include "crypto/gk_proof.h"
#include "crypto/pedersen.h"
#include "crypto/random.h"
#include "Denominations.h"

#include <cstring>

extern "C" {
#include "crypto/crypto-ops.h"
}

namespace {

// Generate a random scalar mod L
static void test_random_scalar(Crypto::EllipticCurveScalar& res) {
  unsigned char tmp[64];
  Random::randomBytes(64, tmp);
  sc_reduce(tmp);
  memcpy(&res, tmp, 32);
}

// Encode uint64 as scalar
static void test_uint64_to_scalar(uint64_t val, Crypto::EllipticCurveScalar& s) {
  memset(s.data, 0, 32);
  for (int i = 0; i < 8; ++i)
    s.data[i] = static_cast<uint8_t>(val >> (8 * i));
}

} // anonymous namespace

// Basic round-trip: prove and verify for each denomination index
TEST(GKProof, RoundTripFirstDenomination) {
  Crypto::EllipticCurveScalar r;
  test_random_scalar(r);

  size_t idx = 0;
  uint64_t v = CryptoNote::DENOMINATIONS[idx];

  Crypto::EllipticCurveScalar v_scalar;
  test_uint64_to_scalar(v, v_scalar);

  Crypto::EllipticCurvePoint C;
  ASSERT_TRUE(Crypto::pedersen_commit(v_scalar, r, C));

  Crypto::Hash tx_hash;
  Random::randomBytes(32, tx_hash.data);

  Crypto::GKProof proof;
  ASSERT_TRUE(Crypto::gk_prove(C, v, r, idx, tx_hash, proof));
  ASSERT_TRUE(Crypto::gk_verify(C, proof, tx_hash));
}

TEST(GKProof, RoundTripLastDenomination) {
  Crypto::EllipticCurveScalar r;
  test_random_scalar(r);

  size_t idx = 63;
  uint64_t v = CryptoNote::DENOMINATIONS[idx];

  Crypto::EllipticCurveScalar v_scalar;
  test_uint64_to_scalar(v, v_scalar);

  Crypto::EllipticCurvePoint C;
  ASSERT_TRUE(Crypto::pedersen_commit(v_scalar, r, C));

  Crypto::Hash tx_hash;
  Random::randomBytes(32, tx_hash.data);

  Crypto::GKProof proof;
  ASSERT_TRUE(Crypto::gk_prove(C, v, r, idx, tx_hash, proof));
  ASSERT_TRUE(Crypto::gk_verify(C, proof, tx_hash));
}

TEST(GKProof, RoundTripMiddleDenomination) {
  Crypto::EllipticCurveScalar r;
  test_random_scalar(r);

  size_t idx = 31; // middle of the range
  uint64_t v = CryptoNote::DENOMINATIONS[idx];

  Crypto::EllipticCurveScalar v_scalar;
  test_uint64_to_scalar(v, v_scalar);

  Crypto::EllipticCurvePoint C;
  ASSERT_TRUE(Crypto::pedersen_commit(v_scalar, r, C));

  Crypto::Hash tx_hash;
  Random::randomBytes(32, tx_hash.data);

  Crypto::GKProof proof;
  ASSERT_TRUE(Crypto::gk_prove(C, v, r, idx, tx_hash, proof));
  ASSERT_TRUE(Crypto::gk_verify(C, proof, tx_hash));
}

// Wrong tx_hash should fail verification
TEST(GKProof, WrongTxHashFails) {
  Crypto::EllipticCurveScalar r;
  test_random_scalar(r);

  size_t idx = 10;
  uint64_t v = CryptoNote::DENOMINATIONS[idx];

  Crypto::EllipticCurveScalar v_scalar;
  test_uint64_to_scalar(v, v_scalar);

  Crypto::EllipticCurvePoint C;
  ASSERT_TRUE(Crypto::pedersen_commit(v_scalar, r, C));

  Crypto::Hash tx_hash, wrong_hash;
  Random::randomBytes(32, tx_hash.data);
  Random::randomBytes(32, wrong_hash.data);

  Crypto::GKProof proof;
  ASSERT_TRUE(Crypto::gk_prove(C, v, r, idx, tx_hash, proof));
  ASSERT_FALSE(Crypto::gk_verify(C, proof, wrong_hash));
}

// Tampered f scalar should fail
TEST(GKProof, TamperedScalarFails) {
  Crypto::EllipticCurveScalar r;
  test_random_scalar(r);

  size_t idx = 5;
  uint64_t v = CryptoNote::DENOMINATIONS[idx];

  Crypto::EllipticCurveScalar v_scalar;
  test_uint64_to_scalar(v, v_scalar);

  Crypto::EllipticCurvePoint C;
  ASSERT_TRUE(Crypto::pedersen_commit(v_scalar, r, C));

  Crypto::Hash tx_hash;
  Random::randomBytes(32, tx_hash.data);

  Crypto::GKProof proof;
  ASSERT_TRUE(Crypto::gk_prove(C, v, r, idx, tx_hash, proof));

  // Tamper with f
  proof.f.data[0] ^= 0x01;
  ASSERT_FALSE(Crypto::gk_verify(C, proof, tx_hash));
}

TEST(GKProof, TamperedBitCommitmentResponseFails) {
  Crypto::EllipticCurveScalar r;
  test_random_scalar(r);

  size_t idx = 13;
  uint64_t v = CryptoNote::DENOMINATIONS[idx];

  Crypto::EllipticCurveScalar v_scalar;
  test_uint64_to_scalar(v, v_scalar);

  Crypto::EllipticCurvePoint C;
  ASSERT_TRUE(Crypto::pedersen_commit(v_scalar, r, C));

  Crypto::Hash tx_hash;
  Random::randomBytes(32, tx_hash.data);

  Crypto::GKProof proof;
  ASSERT_TRUE(Crypto::gk_prove(C, v, r, idx, tx_hash, proof));

  proof.za[0].data[0] ^= 0x01;
  ASSERT_FALSE(Crypto::gk_verify(C, proof, tx_hash));

  ASSERT_TRUE(Crypto::gk_prove(C, v, r, idx, tx_hash, proof));
  proof.zb[0].data[0] ^= 0x01;
  ASSERT_FALSE(Crypto::gk_verify(C, proof, tx_hash));
}

TEST(GKProof, TamperedBitCommitmentPointFails) {
  Crypto::EllipticCurveScalar r;
  test_random_scalar(r);

  size_t idx = 8;
  uint64_t v = CryptoNote::DENOMINATIONS[idx];

  Crypto::EllipticCurveScalar v_scalar;
  test_uint64_to_scalar(v, v_scalar);

  Crypto::EllipticCurvePoint C;
  ASSERT_TRUE(Crypto::pedersen_commit(v_scalar, r, C));

  Crypto::Hash tx_hash;
  Random::randomBytes(32, tx_hash.data);

  Crypto::GKProof proof;
  ASSERT_TRUE(Crypto::gk_prove(C, v, r, idx, tx_hash, proof));

  unsigned char identity[32] = {0};
  identity[0] = 1;
  ASSERT_EQ(ge_frombytes_vartime(&proof.I[0], identity), 0);
  ASSERT_FALSE(Crypto::gk_verify(C, proof, tx_hash));
}

// Wrong denomination index should fail at prove time
TEST(GKProof, WrongDenominationIndexFails) {
  Crypto::EllipticCurveScalar r;
  test_random_scalar(r);

  size_t idx = 5;
  uint64_t v = CryptoNote::DENOMINATIONS[idx];

  Crypto::EllipticCurveScalar v_scalar;
  test_uint64_to_scalar(v, v_scalar);

  Crypto::EllipticCurvePoint C;
  ASSERT_TRUE(Crypto::pedersen_commit(v_scalar, r, C));

  Crypto::Hash tx_hash;
  Random::randomBytes(32, tx_hash.data);

  Crypto::GKProof proof;
  // Use wrong index (value mismatch)
  ASSERT_FALSE(Crypto::gk_prove(C, v, r, idx + 1, tx_hash, proof));
}

// Out-of-range index should fail
TEST(GKProof, OutOfRangeIndexFails) {
  Crypto::EllipticCurveScalar r;
  test_random_scalar(r);

  Crypto::EllipticCurveScalar v_scalar;
  test_uint64_to_scalar(1, v_scalar);

  Crypto::EllipticCurvePoint C;
  ASSERT_TRUE(Crypto::pedersen_commit(v_scalar, r, C));

  Crypto::Hash tx_hash;
  Random::randomBytes(32, tx_hash.data);

  Crypto::GKProof proof;
  ASSERT_FALSE(Crypto::gk_prove(C, 1, r, 64, tx_hash, proof));
}

// All 64 denomination indices should round-trip
TEST(GKProof, AllDenominationsRoundTrip) {
  Crypto::Hash tx_hash;
  Random::randomBytes(32, tx_hash.data);

  for (size_t idx = 0; idx < 64; ++idx) {
    Crypto::EllipticCurveScalar r;
    test_random_scalar(r);

    uint64_t v = CryptoNote::DENOMINATIONS[idx];

    Crypto::EllipticCurveScalar v_scalar;
    test_uint64_to_scalar(v, v_scalar);

    Crypto::EllipticCurvePoint C;
    ASSERT_TRUE(Crypto::pedersen_commit(v_scalar, r, C)) << "idx=" << idx;

    Crypto::GKProof proof;
    ASSERT_TRUE(Crypto::gk_prove(C, v, r, idx, tx_hash, proof)) << "prove idx=" << idx;
    ASSERT_TRUE(Crypto::gk_verify(C, proof, tx_hash)) << "verify idx=" << idx;
  }
}
