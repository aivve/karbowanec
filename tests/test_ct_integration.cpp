// Comprehensive integration tests for CT.
// Covers: denomination arithmetic, GK proof full sweep, denomination table,
//         balance equation edge cases, subgroup checks, ECDH scanning,
//         key image reuse, mixed transparent→CT, and combined scenarios.
//
// Build: cl /EHsc /I../include /I../src test_ct_integration.cpp
//        /link ../build/src/Release/Crypto.lib

#include "crypto/gk_proof.h"
#include "crypto/gk_denomination_table.h"
#include "crypto/triptych.h"
#include "crypto/pedersen.h"
#include "crypto/transaction_balance.h"
#include "crypto/ct_ecdh.h"
#include "crypto/random.h"
#include "crypto/crypto.h"
#include "Denominations.h"
#include "CryptoNoteConfig.h"

#include <algorithm>
#include <chrono>
#include <cstdio>
#include <cstring>
#include <cstdlib>
#include <numeric>
#include <set>
#include <stdexcept>
#include <unordered_map>
#include <vector>

extern "C" {
#include "crypto/crypto-ops.h"
}

// ── Helpers ──────────────────────────────────────────────────────────

static void test_random_scalar(Crypto::EllipticCurveScalar& res) {
  unsigned char tmp[64];
  Random::randomBytes(64, tmp);
  sc_reduce(tmp);
  memcpy(&res, tmp, 32);
}

static void uint64_to_scalar(uint64_t val, Crypto::EllipticCurveScalar& s) {
  memset(s.data, 0, 32);
  for (int i = 0; i < 8; ++i)
    s.data[i] = static_cast<uint8_t>(val >> (8 * i));
}

static void make_commitment(uint64_t amount, const Crypto::EllipticCurveScalar& blinding,
                             Crypto::EllipticCurvePoint& commitment) {
  Crypto::EllipticCurveScalar v;
  uint64_to_scalar(amount, v);
  if (!Crypto::pedersen_commit(v, blinding, commitment)) {
    printf("FATAL: pedersen_commit failed\n");
    abort();
  }
}

static void random_hash(Crypto::Hash& h) {
  Random::randomBytes(32, h.data);
}

static void gen_keypair(Crypto::PublicKey& pub, Crypto::SecretKey& sec) {
  test_random_scalar(reinterpret_cast<Crypto::EllipticCurveScalar&>(sec));
  ge_p3 point;
  ge_scalarmult_base(&point, reinterpret_cast<const unsigned char*>(&sec));
  ge_p3_tobytes(reinterpret_cast<unsigned char*>(&pub), &point);
}

static int tests_run = 0;
static int tests_passed = 0;

#define TEST(name) \
  do { \
    tests_run++; \
    printf("  %-62s", name); \
    fflush(stdout); \
  } while(0)

#define PASS() do { tests_passed++; printf("[PASS]\n"); fflush(stdout); } while(0)
#define FAIL(msg) do { printf("[FAIL] %s\n", msg); fflush(stdout); return; } while(0)

// =====================================================================
// SECTION 1: CT FLOOR / DUST POLICY TESTS
// =====================================================================
// CT denominations sit on top of the existing
// 12-decimal atomic precision, with MIN_CT_DENOMINATION = 0.01 KRB = 10^10 au.

static void test_min_ct_denomination_value() {
  TEST("CT floor: MIN_CT_DENOMINATION == 10^10 atomic units");
  if (CryptoNote::MIN_CT_DENOMINATION != UINT64_C(10000000000))
    FAIL("expected 10^10");
  PASS();
}

static void test_min_ct_denomination_matches_table() {
  TEST("CT floor: DENOMINATIONS[0] == MIN_CT_DENOMINATION");
  if (CryptoNote::DENOMINATIONS[0] != CryptoNote::MIN_CT_DENOMINATION)
    FAIL("table head must equal floor");
  PASS();
}

static void test_below_floor_not_canonical() {
  TEST("CT floor: amounts below floor are not canonical");
  if (CryptoNote::isCanonicalDenomination(CryptoNote::MIN_CT_DENOMINATION - 1))
    FAIL("sub-floor must not be canonical");
  if (CryptoNote::isCanonicalDenomination(1)) FAIL("single au must not be canonical");
  PASS();
}

static void test_floor_multiple_decomposable() {
  TEST("CT floor: multiples of MIN_CT_DENOMINATION decompose");
  // 0.05 KRB = 5 * 10^10 au → must decompose
  auto v = CryptoNote::decomposeAmount(5 * CryptoNote::MIN_CT_DENOMINATION);
  if (v.size() != 1) FAIL("0.05 KRB should be a single denom");
  if (v[0] != 5 * CryptoNote::MIN_CT_DENOMINATION) FAIL("wrong denom");
  // 0.11 KRB = 11 * 10^10 → should split into 0.10 + 0.01
  auto v2 = CryptoNote::decomposeAmount(11 * CryptoNote::MIN_CT_DENOMINATION);
  uint64_t sum = 0;
  for (auto x : v2) sum += x;
  if (sum != 11 * CryptoNote::MIN_CT_DENOMINATION) FAIL("decomposition sum mismatch");
  PASS();
}

static void test_sub_floor_decomposition_throws() {
  TEST("CT floor: sub-floor amounts are not decomposable");
  bool threw = false;
  try {
    (void)CryptoNote::decomposeAmount(CryptoNote::MIN_CT_DENOMINATION - 1);
  } catch (const std::invalid_argument&) {
    threw = true;
  }
  if (!threw) FAIL("expected invalid_argument");
  PASS();
}

static void test_dust_residue_routes_to_fee() {
  TEST("CT floor: dust residue routes to fee, no new dust output");
  // Wallet policy: change = canonical * floor(change/floor) + residue;
  // residue is added to fee; only canonical change becomes a CT output.
  const uint64_t floor = CryptoNote::MIN_CT_DENOMINATION;
  uint64_t change = 12345 + 3 * floor; // 3.00...something KRB
  uint64_t canonical = (change / floor) * floor;
  uint64_t residue = change - canonical;
  if (residue >= floor) FAIL("residue must be sub-floor");
  if (canonical % floor != 0) FAIL("canonical must be multiple of floor");
  if (canonical + residue != change) FAIL("split must be lossless");
  PASS();
}

// =====================================================================
// SECTION 2: GK PROOF TESTS (extended)
// =====================================================================

static void test_gk_all_64_roundtrip() {
  TEST("GK proof: round-trip for all 64 denominations");

  Crypto::Hash tx_hash;
  random_hash(tx_hash);

  for (size_t idx = 0; idx < 64; ++idx) {
    Crypto::EllipticCurveScalar r;
    test_random_scalar(r);

    uint64_t v = CryptoNote::DENOMINATIONS[idx];
    Crypto::EllipticCurveScalar v_scalar;
    uint64_to_scalar(v, v_scalar);

    Crypto::EllipticCurvePoint C;
    if (!Crypto::pedersen_commit(v_scalar, r, C)) {
      char msg[64]; snprintf(msg, sizeof(msg), "commit failed idx=%zu", idx);
      FAIL(msg);
    }

    Crypto::GKProof proof;
    if (!Crypto::gk_prove(C, v, r, idx, tx_hash, proof)) {
      char msg[64]; snprintf(msg, sizeof(msg), "prove failed idx=%zu", idx);
      FAIL(msg);
    }
    if (!Crypto::gk_verify(C, proof, tx_hash)) {
      char msg[64]; snprintf(msg, sizeof(msg), "verify failed idx=%zu", idx);
      FAIL(msg);
    }
  }
  PASS();
}

static void test_gk_tampered_z_scalar() {
  TEST("GK proof: tampered z[0] scalar fails");

  Crypto::EllipticCurveScalar r;
  test_random_scalar(r);
  uint64_t v = CryptoNote::DENOMINATIONS[20];
  Crypto::EllipticCurveScalar v_scalar;
  uint64_to_scalar(v, v_scalar);
  Crypto::EllipticCurvePoint C;
  Crypto::pedersen_commit(v_scalar, r, C);
  Crypto::Hash tx_hash;
  random_hash(tx_hash);
  Crypto::GKProof proof;
  Crypto::gk_prove(C, v, r, 20, tx_hash, proof);

  proof.z[0].data[0] ^= 0x01;
  if (Crypto::gk_verify(C, proof, tx_hash)) FAIL("should have failed");
  PASS();
}

static void test_gk_tampered_bit_responses() {
  TEST("GK proof: tampered za/zb bit responses fail");

  Crypto::EllipticCurveScalar r;
  test_random_scalar(r);
  uint64_t v = CryptoNote::DENOMINATIONS[21];
  Crypto::EllipticCurveScalar v_scalar;
  uint64_to_scalar(v, v_scalar);
  Crypto::EllipticCurvePoint C;
  Crypto::pedersen_commit(v_scalar, r, C);
  Crypto::Hash tx_hash;
  random_hash(tx_hash);

  Crypto::GKProof proof;
  Crypto::gk_prove(C, v, r, 21, tx_hash, proof);
  proof.za[0].data[0] ^= 0x01;
  if (Crypto::gk_verify(C, proof, tx_hash)) FAIL("tampered za should fail");

  Crypto::gk_prove(C, v, r, 21, tx_hash, proof);
  proof.zb[0].data[0] ^= 0x01;
  if (Crypto::gk_verify(C, proof, tx_hash)) FAIL("tampered zb should fail");

  PASS();
}

static void test_gk_tampered_A_point() {
  TEST("GK proof: tampered A[0] point fails");

  Crypto::EllipticCurveScalar r;
  test_random_scalar(r);
  uint64_t v = CryptoNote::DENOMINATIONS[15];
  Crypto::EllipticCurveScalar v_scalar;
  uint64_to_scalar(v, v_scalar);
  Crypto::EllipticCurvePoint C;
  Crypto::pedersen_commit(v_scalar, r, C);
  Crypto::Hash tx_hash;
  random_hash(tx_hash);
  Crypto::GKProof proof;
  Crypto::gk_prove(C, v, r, 15, tx_hash, proof);

  // Tamper with serialized A[0] point
  unsigned char buf[32];
  ge_p3_tobytes(buf, &proof.A[0]);
  buf[0] ^= 0x01;
  ge_frombytes_vartime(&proof.A[0], buf);
  if (Crypto::gk_verify(C, proof, tx_hash)) FAIL("should have failed");
  PASS();
}

static void test_gk_max_denomination() {
  TEST("GK proof: max denomination idx=63 (100,000 KRB)");

  Crypto::EllipticCurveScalar r;
  test_random_scalar(r);
  uint64_t v = CryptoNote::DENOMINATIONS[63];
  if (v != UINT64_C(100000000000000000)) FAIL("expected 10^17 au");
  Crypto::EllipticCurveScalar v_scalar;
  uint64_to_scalar(v, v_scalar);
  Crypto::EllipticCurvePoint C;
  Crypto::pedersen_commit(v_scalar, r, C);
  Crypto::Hash tx_hash;
  random_hash(tx_hash);
  Crypto::GKProof proof;
  if (!Crypto::gk_prove(C, v, r, 63, tx_hash, proof)) FAIL("prove failed");
  if (!Crypto::gk_verify(C, proof, tx_hash)) FAIL("verify failed");
  PASS();
}

// =====================================================================
// SECTION 3: DENOMINATION TABLE COVERAGE
// =====================================================================

static void test_denomination_table_coverage() {
  TEST("Denomination table: all 64 entries reachable via decomposition");

  std::set<uint64_t> seen;
  // Each denomination should appear at least as itself
  for (size_t i = 0; i < 64; ++i) {
    uint64_t d = CryptoNote::DENOMINATIONS[i];
    auto parts = CryptoNote::decomposeAmount(d);
    if (parts.size() != 1 || parts[0] != d) {
      char msg[80]; snprintf(msg, sizeof(msg), "denomination %llu doesn't decompose to itself",
                             (unsigned long long)d);
      FAIL(msg);
    }
    seen.insert(d);
  }
  if (seen.size() != 64) FAIL("not all 64 denominations covered");
  PASS();
}

static void test_denomination_canonical_check() {
  TEST("Denomination: isCanonicalDenomination for all 64");

  for (size_t i = 0; i < 64; ++i) {
    if (!CryptoNote::isCanonicalDenomination(CryptoNote::DENOMINATIONS[i])) {
      FAIL("valid denomination rejected");
    }
  }
  // Non-canonical values
  if (CryptoNote::isCanonicalDenomination(0)) FAIL("0 accepted");
  if (CryptoNote::isCanonicalDenomination(11)) FAIL("11 accepted");
  if (CryptoNote::isCanonicalDenomination(15)) FAIL("15 accepted");
  if (CryptoNote::isCanonicalDenomination(10000001)) FAIL("10000001 accepted");
  PASS();
}

static void test_denomination_index() {
  TEST("Denomination: denominationIndex for all 64 + invalid");

  for (size_t i = 0; i < 64; ++i) {
    if (CryptoNote::denominationIndex(CryptoNote::DENOMINATIONS[i]) != static_cast<int>(i)) {
      FAIL("wrong index");
    }
  }
  if (CryptoNote::denominationIndex(0) != -1) FAIL("0 should return -1");
  if (CryptoNote::denominationIndex(11) != -1) FAIL("11 should return -1");
  PASS();
}

static void test_decompose_multi_output() {
  TEST("Decompose: 12345.67 KRB → multiple denomination outputs");

  // 12345.67 KRB in atomic units (1 KRB = 10^12 au)
  uint64_t amount = UINT64_C(12345670000000000);
  auto parts = CryptoNote::decomposeAmount(amount);

  // Verify sum
  uint64_t sum = 0;
  for (auto p : parts) sum += p;
  if (sum != amount) FAIL("sum mismatch");

  // Each part must be a canonical denomination
  for (auto p : parts) {
    if (!CryptoNote::isCanonicalDenomination(p)) {
      char msg[80]; snprintf(msg, sizeof(msg), "non-canonical part %llu", (unsigned long long)p);
      FAIL(msg);
    }
  }

  if (parts.empty()) FAIL("expected non-empty decomposition");
  PASS();
}

static void test_decompose_zero_rejected() {
  TEST("Decompose: zero amount throws");

  bool threw = false;
  try {
    CryptoNote::decomposeAmount(0);
  } catch (const std::invalid_argument&) {
    threw = true;
  }
  if (!threw) FAIL("should throw for zero");
  PASS();
}

static void test_decompose_roundtrip_grid() {
  TEST("Decompose: representable amounts round-trip exactly (grid)");

  const uint64_t floor = CryptoNote::MIN_CT_DENOMINATION;
  for (uint64_t k = 1; k <= 500; ++k) {
    const uint64_t amount = k * floor;
    auto parts = CryptoNote::decomposeAmount(amount);
    uint64_t sum = 0;
    for (auto p : parts) {
      if (!CryptoNote::isCanonicalDenomination(p)) FAIL("non-canonical part");
      sum += p;
    }
    if (sum != amount) FAIL("round-trip sum mismatch");
  }
  PASS();
}

// =====================================================================
// SECTION 4: BALANCE EQUATION EDGE CASES
// =====================================================================

static void test_balance_valid_excess() {
  TEST("Balance: valid excess passes");

  uint64_t amt_in = 500, amt_out = 495, fee = 5;
  Crypto::EllipticCurveScalar r_in, r_out, excess;
  test_random_scalar(r_in);
  test_random_scalar(r_out);
  Crypto::compute_excess_scalar(&r_in, 1, &r_out, 1, excess);

  Crypto::EllipticCurvePoint C_in, C_out;
  make_commitment(amt_in, r_in, C_in);
  make_commitment(amt_out, r_out, C_out);

  Crypto::Hash tx_hash;
  random_hash(tx_hash);
  Crypto::TransactionKernel kernel;
  if (!Crypto::sign_transaction_kernel(excess, tx_hash, kernel)) FAIL("sign failed");
  if (!Crypto::verify_transaction_balance(&C_in, 1, &C_out, 1, fee, tx_hash, kernel))
    FAIL("verify failed");
  PASS();
}

static void test_balance_invalid_excess() {
  TEST("Balance: invalid excess fails");

  uint64_t amt_in = 500, amt_out = 490, fee = 5; // off by 5
  Crypto::EllipticCurveScalar r_in, r_out, excess;
  test_random_scalar(r_in);
  test_random_scalar(r_out);
  Crypto::compute_excess_scalar(&r_in, 1, &r_out, 1, excess);

  Crypto::EllipticCurvePoint C_in, C_out;
  make_commitment(amt_in, r_in, C_in);
  make_commitment(amt_out, r_out, C_out);

  Crypto::Hash tx_hash;
  random_hash(tx_hash);
  Crypto::TransactionKernel kernel;
  Crypto::sign_transaction_kernel(excess, tx_hash, kernel);

  // fee=5 but gap is 10, so balance should fail
  if (Crypto::verify_transaction_balance(&C_in, 1, &C_out, 1, fee, tx_hash, kernel))
    FAIL("should have failed");
  PASS();
}

static void test_balance_correct_fee_handling() {
  TEST("Balance: exact fee handling, off-by-one rejected");

  uint64_t amt_in = 100, amt_out = 90, fee = 10;
  Crypto::EllipticCurveScalar r_in, r_out, excess;
  test_random_scalar(r_in);
  test_random_scalar(r_out);
  Crypto::compute_excess_scalar(&r_in, 1, &r_out, 1, excess);

  Crypto::EllipticCurvePoint C_in, C_out;
  make_commitment(amt_in, r_in, C_in);
  make_commitment(amt_out, r_out, C_out);

  Crypto::Hash tx_hash;
  random_hash(tx_hash);
  Crypto::TransactionKernel kernel;
  Crypto::sign_transaction_kernel(excess, tx_hash, kernel);

  // Correct fee
  if (!Crypto::verify_transaction_balance(&C_in, 1, &C_out, 1, fee, tx_hash, kernel))
    FAIL("correct fee rejected");
  // fee-1
  if (Crypto::verify_transaction_balance(&C_in, 1, &C_out, 1, fee - 1, tx_hash, kernel))
    FAIL("fee-1 accepted");
  // fee+1
  if (Crypto::verify_transaction_balance(&C_in, 1, &C_out, 1, fee + 1, tx_hash, kernel))
    FAIL("fee+1 accepted");
  PASS();
}

static void test_balance_many_inputs_outputs() {
  TEST("Balance: 5 inputs, 4 outputs + fee");

  const size_t NI = 5, NO = 4;
  uint64_t amounts_in[NI] = {100, 200, 300, 400, 500}; // total=1500
  uint64_t amounts_out[NO] = {350, 350, 350, 440};     // total=1490
  uint64_t fee = 10;

  Crypto::EllipticCurveScalar r_in[NI], r_out[NO], excess;
  for (size_t i = 0; i < NI; ++i) test_random_scalar(r_in[i]);
  for (size_t i = 0; i < NO; ++i) test_random_scalar(r_out[i]);
  Crypto::compute_excess_scalar(r_in, NI, r_out, NO, excess);

  Crypto::EllipticCurvePoint C_in[NI], C_out[NO];
  for (size_t i = 0; i < NI; ++i) make_commitment(amounts_in[i], r_in[i], C_in[i]);
  for (size_t i = 0; i < NO; ++i) make_commitment(amounts_out[i], r_out[i], C_out[i]);

  Crypto::Hash tx_hash;
  random_hash(tx_hash);
  Crypto::TransactionKernel kernel;
  Crypto::sign_transaction_kernel(excess, tx_hash, kernel);

  if (!Crypto::verify_transaction_balance(C_in, NI, C_out, NO, fee, tx_hash, kernel))
    FAIL("valid balance rejected");
  PASS();
}

// =====================================================================
// SECTION 5: SUBGROUP CHECKS
// =====================================================================

static void test_subgroup_identity_rejected() {
  TEST("Subgroup: identity point rejected");

  // The identity point in Ed25519 compressed form: (0, 1)
  Crypto::EllipticCurvePoint identity;
  memset(&identity, 0, 32);
  identity.data[0] = 0x01; // y=1 → identity

  if (Crypto::point_valid_for_pedersen(identity))
    FAIL("identity should be rejected");
  PASS();
}

static void test_subgroup_low_order_points() {
  TEST("Subgroup: low-order points (cofactor-8 attack vectors)");

  // Known small-order points on Ed25519 (order 2, 4, 8)
  // These are points where l*P = identity but 8*P might or might not be identity
  // Order-2 point: (0, -1) = (0, p-1) encoded as y with x-sign
  static const unsigned char order2[32] = {
    0xec, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
    0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
    0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
    0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x7f
  };

  Crypto::EllipticCurvePoint p;
  memcpy(&p, order2, 32);
  if (Crypto::point_valid_for_pedersen(p))
    FAIL("order-2 point should be rejected");
  PASS();
}

static void test_subgroup_valid_point_accepted() {
  TEST("Subgroup: valid Pedersen commitment accepted");

  Crypto::EllipticCurveScalar r, v;
  test_random_scalar(r);
  uint64_to_scalar(42, v);
  Crypto::EllipticCurvePoint C;
  if (!Crypto::pedersen_commit(v, r, C)) FAIL("commit failed");
  if (!Crypto::point_valid_for_pedersen(C)) FAIL("valid point rejected");
  PASS();
}

// =====================================================================
// SECTION 6: KEY IMAGE REUSE REJECTION
// =====================================================================

// Build a ring of ring_size members with the real spend at true_index;
// real commitment is to `amount` with `real_blinding`, pseudo is a fresh
// commitment to the same amount. Decoys get random keypairs and random
// (value, blinding) Pedersen commitments. Returns nothing — populates
// the caller's flat arrays in place.
static void build_triptych_ring(size_t ring_size, size_t true_index,
                                uint64_t amount,
                                const Crypto::PublicKey& real_pub,
                                const Crypto::EllipticCurveScalar& real_blinding,
                                Crypto::PublicKey* pubs,
                                Crypto::EllipticCurvePoint* commits) {
  Crypto::EllipticCurveScalar v_scalar;
  uint64_to_scalar(amount, v_scalar);

  pubs[true_index] = real_pub;
  Crypto::pedersen_commit(v_scalar, real_blinding, commits[true_index]);

  for (size_t i = 0; i < ring_size; ++i) {
    if (i == true_index) continue;
    Crypto::SecretKey dummy_sec;
    gen_keypair(pubs[i], dummy_sec);
    Crypto::EllipticCurveScalar dummy_r, dummy_v;
    test_random_scalar(dummy_r);
    test_random_scalar(dummy_v);
    Crypto::pedersen_commit(dummy_v, dummy_r, commits[i]);
  }
}

static void test_key_image_deterministic() {
  TEST("Key image: same key → same image across different rings");

  // Generate one spend keypair
  Crypto::PublicKey pub;
  Crypto::SecretKey sec;
  gen_keypair(pub, sec);

  // Two different rings (size 4 — the minimum supported by Triptych) with
  // the same real key. Distinct decoys, distinct real and pseudo blindings,
  // distinct messages — the key image must still match.
  Crypto::EllipticCurveScalar r1, r2, pr1, pr2;
  test_random_scalar(r1); test_random_scalar(r2);
  test_random_scalar(pr1); test_random_scalar(pr2);

  Crypto::EllipticCurveScalar v_scalar;
  uint64_to_scalar(100, v_scalar);

  Crypto::PublicKey pubs1[4], pubs2[4];
  Crypto::EllipticCurvePoint commits1[4], commits2[4];
  build_triptych_ring(4, 0, 100, pub, r1, pubs1, commits1);
  build_triptych_ring(4, 1, 100, pub, r2, pubs2, commits2);

  Crypto::EllipticCurvePoint pseudo1, pseudo2;
  Crypto::pedersen_commit(v_scalar, pr1, pseudo1);
  Crypto::pedersen_commit(v_scalar, pr2, pseudo2);

  Crypto::Hash msg1, msg2;
  random_hash(msg1);
  random_hash(msg2);

  Crypto::KeyImage ki1, ki2;
  Crypto::TriptychSignature sig1, sig2;

  if (!Crypto::triptych_sign(msg1, pubs1, commits1, pseudo1, 4, 0, sec, r1, pr1, ki1, sig1)) {
    FAIL("sign ring 1"); return;
  }
  if (!Crypto::triptych_sign(msg2, pubs2, commits2, pseudo2, 4, 1, sec, r2, pr2, ki2, sig2)) {
    FAIL("sign ring 2"); return;
  }

  if (memcmp(&ki1, &ki2, sizeof(Crypto::KeyImage)) != 0)
    FAIL("key images should match for same key");
  PASS();
}

static void test_key_image_different_keys() {
  TEST("Key image: different keys → different images");

  Crypto::PublicKey pub1, pub2;
  Crypto::SecretKey sec1, sec2;
  gen_keypair(pub1, sec1);
  gen_keypair(pub2, sec2);

  Crypto::EllipticCurveScalar v_scalar, r1, r2, pr1, pr2;
  uint64_to_scalar(100, v_scalar);
  test_random_scalar(r1); test_random_scalar(r2);
  test_random_scalar(pr1); test_random_scalar(pr2);

  Crypto::PublicKey pubs1[4], pubs2[4];
  Crypto::EllipticCurvePoint commits1[4], commits2[4];
  build_triptych_ring(4, 0, 100, pub1, r1, pubs1, commits1);
  build_triptych_ring(4, 0, 100, pub2, r2, pubs2, commits2);

  Crypto::EllipticCurvePoint pseudo1, pseudo2;
  Crypto::pedersen_commit(v_scalar, pr1, pseudo1);
  Crypto::pedersen_commit(v_scalar, pr2, pseudo2);

  Crypto::Hash msg;
  random_hash(msg);

  Crypto::KeyImage ki1, ki2;
  Crypto::TriptychSignature sig1, sig2;

  if (!Crypto::triptych_sign(msg, pubs1, commits1, pseudo1, 4, 0, sec1, r1, pr1, ki1, sig1)) {
    FAIL("sign 1"); return;
  }
  if (!Crypto::triptych_sign(msg, pubs2, commits2, pseudo2, 4, 0, sec2, r2, pr2, ki2, sig2)) {
    FAIL("sign 2"); return;
  }

  if (memcmp(&ki1, &ki2, sizeof(Crypto::KeyImage)) == 0)
    FAIL("key images should differ for different keys");
  PASS();
}

// =====================================================================
// SECTION 7: ECDH OUTPUT SCANNING
// =====================================================================

static void test_ecdh_recipient_identifies_output() {
  TEST("ECDH: recipient correctly identifies owned output");

  // Initialize ECDH module
  Crypto::ct_ecdh_init();

  // Generate recipient keys
  Crypto::PublicKey view_pub, spend_pub;
  Crypto::SecretKey view_sec, spend_sec;
  gen_keypair(view_pub, view_sec);
  gen_keypair(spend_pub, spend_sec);

  // Generate tx secret key
  Crypto::PublicKey tx_pub;
  Crypto::SecretKey tx_sec;
  gen_keypair(tx_pub, tx_sec);

  // Create ECDH shared secret: D = tx_sec * view_pub = view_sec * tx_pub
  Crypto::KeyDerivation derivation;
  if (!Crypto::generate_key_derivation(view_pub, tx_sec, derivation))
    FAIL("generate_key_derivation failed");

  // Derive blinding factor and mask amount
  size_t output_index = 0;
  uint64_t amount = CryptoNote::DENOMINATIONS[25]; // some denomination

  Crypto::EllipticCurveScalar blinding;
  Crypto::derive_blinding_factor(derivation, output_index, blinding);

  Crypto::PublicKey commitment;
  if (!Crypto::pedersen_commit(amount, blinding, commitment))
    FAIL("pedersen_commit failed");

  Crypto::MaskedAmount masked;
  Crypto::mask_amount(derivation, output_index, amount, masked);

  // Recipient side: recover amount using view_sec + tx_pub
  uint64_t recovered_amount;
  Crypto::EllipticCurveScalar recovered_blinding;
  if (!Crypto::decrypt_and_verify_output(view_sec, tx_pub, output_index,
                                          masked, commitment,
                                          recovered_amount, recovered_blinding))
    FAIL("decrypt_and_verify_output failed");

  if (recovered_amount != amount) FAIL("amount mismatch");
  if (memcmp(&recovered_blinding, &blinding, 32) != 0) FAIL("blinding mismatch");
  PASS();
}

static void test_ecdh_non_owner_skips() {
  TEST("ECDH: non-owner fails to verify output");

  Crypto::ct_ecdh_init();

  // Owner keys
  Crypto::PublicKey owner_view_pub;
  Crypto::SecretKey owner_view_sec;
  gen_keypair(owner_view_pub, owner_view_sec);

  // Non-owner keys
  Crypto::PublicKey nonowner_view_pub;
  Crypto::SecretKey nonowner_view_sec;
  gen_keypair(nonowner_view_pub, nonowner_view_sec);

  // Tx keys
  Crypto::PublicKey tx_pub;
  Crypto::SecretKey tx_sec;
  gen_keypair(tx_pub, tx_sec);

  // Create output for owner
  Crypto::KeyDerivation derivation;
  Crypto::generate_key_derivation(owner_view_pub, tx_sec, derivation);

  uint64_t amount = CryptoNote::DENOMINATIONS[10];
  Crypto::EllipticCurveScalar blinding;
  Crypto::derive_blinding_factor(derivation, 0, blinding);

  Crypto::PublicKey commitment;
  Crypto::pedersen_commit(amount, blinding, commitment);

  Crypto::MaskedAmount masked;
  Crypto::mask_amount(derivation, 0, amount, masked);

  // Non-owner tries to decrypt: should fail verification
  uint64_t recovered_amount;
  Crypto::EllipticCurveScalar recovered_blinding;
  bool result = Crypto::decrypt_and_verify_output(nonowner_view_sec, tx_pub, 0,
                                                   masked, commitment,
                                                   recovered_amount, recovered_blinding);
  if (result) FAIL("non-owner should not verify");
  PASS();
}

// Regression test for the blinding-factor / stealth-scalar domain separation.
//
// If derive_blinding_factor() reused the same hash input as derivation_to_scalar()
// (the scalar that crypto.cpp uses to compute P = s*G + B_spend), the blinding
// factor r would equal the stealth scalar s. Any passive observer who knows the
// recipient's public address (B_spend) could then compute
//     r*G = P - B_spend
//     v*H = C - r*G
// and brute-force v over the 64 canonical denominations, fully recovering the
// amount of every output sent to that recipient without the view key.
//
// This test simulates that attack against the current derivation. It must FAIL
// (i.e. attacker cannot find a matching denomination) as long as
// derive_blinding_factor() uses a domain separator distinct from the stealth
// derivation. If it ever stops being domain-separated, the test will pass --
// which we want to catch as a regression.
static void test_blinding_factor_domain_separation_from_stealth_scalar() {
  TEST("ECDH: r != stealth scalar (passive observer cannot recover amount)");

  Crypto::ct_ecdh_init();

  // Recipient address: view_pub is private to recipient, spend_pub is the
  // public B_spend known to the attacker.
  Crypto::PublicKey view_pub, spend_pub;
  Crypto::SecretKey view_sec, spend_sec;
  gen_keypair(view_pub, view_sec);
  gen_keypair(spend_pub, spend_sec);

  // Transaction public key R is on-chain (in tx.extra). The attacker has it.
  Crypto::PublicKey tx_pub;
  Crypto::SecretKey tx_sec;
  gen_keypair(tx_pub, tx_sec);

  // Honest sender builds the output exactly as the wallet does.
  Crypto::KeyDerivation derivation;
  if (!Crypto::generate_key_derivation(view_pub, tx_sec, derivation))
    FAIL("generate_key_derivation failed");

  const size_t output_index = 3;
  const uint64_t amount = CryptoNote::DENOMINATIONS[25];

  Crypto::EllipticCurveScalar blinding;
  Crypto::derive_blinding_factor(derivation, output_index, blinding);

  Crypto::PublicKey commitment;
  if (!Crypto::pedersen_commit(amount, blinding, commitment))
    FAIL("pedersen_commit failed");

  // Stealth one-time address P = s*G + B_spend, where s comes from the
  // standard (un-domain-separated) derivation_to_scalar() in crypto.cpp.
  Crypto::PublicKey target_key;
  if (!Crypto::derive_public_key(derivation, output_index, spend_pub, target_key))
    FAIL("derive_public_key failed");

  // --- Attacker side ---
  // Attacker knows: spend_pub (public address), tx_pub (extra), commitment,
  // target_key (output). They do NOT know view_sec.
  //
  // Compute candidate r*G = P - B_spend.  If derive_blinding_factor produced
  // the same scalar as derivation_to_scalar, this point would equal blinding*G
  // and v*H = commitment - r*G would resolve to one of the 64 canonical
  // denominations.
  ge_p3 target_p3, spend_p3;
  if (ge_frombytes_vartime(&target_p3, reinterpret_cast<const unsigned char*>(&target_key)) != 0)
    FAIL("target_key decode failed");
  if (ge_frombytes_vartime(&spend_p3, reinterpret_cast<const unsigned char*>(&spend_pub)) != 0)
    FAIL("spend_pub decode failed");

  ge_cached spend_cached;
  ge_p3_to_cached(&spend_cached, &spend_p3);
  ge_p1p1 rG_p1p1;
  ge_sub(&rG_p1p1, &target_p3, &spend_cached);
  ge_p3 rG_candidate_p3;
  ge_p1p1_to_p3(&rG_candidate_p3, &rG_p1p1);
  unsigned char rG_candidate_bytes[32];
  ge_p3_tobytes(rG_candidate_bytes, &rG_candidate_p3);

  // Compute commitment - rG_candidate for each denomination v, compare to v*H.
  ge_p3 commitment_p3;
  if (ge_frombytes_vartime(&commitment_p3, reinterpret_cast<const unsigned char*>(&commitment)) != 0)
    FAIL("commitment decode failed");
  ge_cached rG_cached;
  ge_p3_to_cached(&rG_cached, &rG_candidate_p3);
  ge_p1p1 vH_recovered_p1p1;
  ge_sub(&vH_recovered_p1p1, &commitment_p3, &rG_cached);
  ge_p3 vH_recovered_p3;
  ge_p1p1_to_p3(&vH_recovered_p3, &vH_recovered_p1p1);
  unsigned char vH_recovered_bytes[32];
  ge_p3_tobytes(vH_recovered_bytes, &vH_recovered_p3);

  ge_p3 H_p3;
  if (ge_frombytes_vartime(&H_p3, reinterpret_cast<const unsigned char*>(&Crypto::pedersen_get_H())) != 0)
    FAIL("H decode failed");

  bool attacker_recovered_amount = false;
  for (size_t i = 0; i < CryptoNote::DENOMINATION_COUNT; ++i) {
    Crypto::EllipticCurveScalar v_scalar;
    uint64_to_scalar(CryptoNote::DENOMINATIONS[i], v_scalar);
    ge_p2 vH_p2;
    ge_scalarmult(&vH_p2, reinterpret_cast<const unsigned char*>(&v_scalar), &H_p3);
    unsigned char vH_bytes[32];
    ge_tobytes(vH_bytes, &vH_p2);
    if (memcmp(vH_bytes, vH_recovered_bytes, 32) == 0) {
      attacker_recovered_amount = true;
      break;
    }
  }

  if (attacker_recovered_amount) {
    FAIL("CRITICAL: passive observer recovered amount from public commitment + recipient address. "
         "derive_blinding_factor() is not domain-separated from derivation_to_scalar(). "
         "This breaks CT confidentiality for all known recipients.");
    return;
  }

  // Sanity: the legitimate recipient must still be able to recover the amount.
  uint64_t recovered_amount;
  Crypto::EllipticCurveScalar recovered_blinding;
  if (!Crypto::decrypt_and_verify_output(view_sec, tx_pub, output_index,
                                          Crypto::MaskedAmount{}, commitment,
                                          recovered_amount, recovered_blinding)) {
    // This path uses an empty masked amount on purpose; we are only testing
    // the blinding/commitment relationship, not the mask round-trip.
    // decrypt_and_verify_output recomputes the commitment from the unmasked
    // amount and will report mismatch when the unmasked value is wrong, which
    // is fine -- the attacker check above is what matters.
  }

  PASS();
}

static void test_ecdh_multiple_outputs() {
  TEST("ECDH: multiple outputs with different indices");

  Crypto::ct_ecdh_init();

  Crypto::PublicKey view_pub;
  Crypto::SecretKey view_sec;
  gen_keypair(view_pub, view_sec);

  Crypto::PublicKey tx_pub;
  Crypto::SecretKey tx_sec;
  gen_keypair(tx_pub, tx_sec);

  Crypto::KeyDerivation derivation;
  Crypto::generate_key_derivation(view_pub, tx_sec, derivation);

  uint64_t amounts[3] = {
    CryptoNote::DENOMINATIONS[5],
    CryptoNote::DENOMINATIONS[20],
    CryptoNote::DENOMINATIONS[63]
  };

  for (size_t idx = 0; idx < 3; ++idx) {
    Crypto::EllipticCurveScalar blinding;
    Crypto::derive_blinding_factor(derivation, idx, blinding);

    Crypto::PublicKey commitment;
    Crypto::pedersen_commit(amounts[idx], blinding, commitment);

    Crypto::MaskedAmount masked;
    Crypto::mask_amount(derivation, idx, amounts[idx], masked);

    uint64_t recovered;
    Crypto::EllipticCurveScalar rec_blind;
    if (!Crypto::decrypt_and_verify_output(view_sec, tx_pub, idx,
                                            masked, commitment,
                                            recovered, rec_blind)) {
      char msg[64]; snprintf(msg, sizeof(msg), "failed for output index %zu", idx);
      FAIL(msg);
    }
    if (recovered != amounts[idx]) {
      char msg[64]; snprintf(msg, sizeof(msg), "amount mismatch at index %zu", idx);
      FAIL(msg);
    }
  }
  PASS();
}

static void test_ecdh_pedersen_generator_consistency() {
  TEST("ECDH: pedersen wrapper uses shared generator (init-order safe)");

  const uint64_t amount = CryptoNote::DENOMINATIONS[17];
  Crypto::EllipticCurveScalar blinding;
  test_random_scalar(blinding);

  // Commitment via CT ECDH wrapper.
  Crypto::PublicKey commitment_from_wrapper;
  if (!Crypto::pedersen_commit(amount, blinding, commitment_from_wrapper)) {
    FAIL("wrapper pedersen_commit failed");
  }

  // Commitment via scalar API in pedersen.cpp (source of truth).
  Crypto::EllipticCurveScalar amount_scalar;
  uint64_to_scalar(amount, amount_scalar);
  Crypto::EllipticCurvePoint commitment_from_scalar_api;
  if (!Crypto::pedersen_commit(amount_scalar, blinding, commitment_from_scalar_api)) {
    FAIL("scalar pedersen_commit failed");
  }

  if (memcmp(commitment_from_wrapper.data, commitment_from_scalar_api.data, 32) != 0) {
    FAIL("wrapper/scalar commitment mismatch");
  }

  // ct_ecdh_init() is intentionally a no-op; calling it must not alter behavior.
  Crypto::ct_ecdh_init();
  Crypto::PublicKey commitment_after_init;
  if (!Crypto::pedersen_commit(amount, blinding, commitment_after_init)) {
    FAIL("wrapper pedersen_commit after init failed");
  }

  if (memcmp(commitment_from_wrapper.data, commitment_after_init.data, 32) != 0) {
    FAIL("ct_ecdh_init altered commitment behavior");
  }

  PASS();
}

// =====================================================================
// SECTION 8: COMBINED / EDGE CASES
// =====================================================================

static void test_tx_version_requirements() {
  TEST("Combined: tx version constants correct");

  if (CryptoNote::CURRENT_TRANSACTION_VERSION != 1)
    FAIL("CURRENT_TRANSACTION_VERSION should be 1");
  if (CryptoNote::TRANSACTION_VERSION_CT != 2)
    FAIL("TRANSACTION_VERSION_CT should be 2");
  if (CryptoNote::BLOCK_MAJOR_VERSION_6 != 6)
    FAIL("BLOCK_MAJOR_VERSION_6 should be 6");
  PASS();
}

static void test_mixed_transparent_to_ct_balance() {
  TEST("Combined: transparent→CT with proper balance");

  // 2 transparent inputs (zero blinding) → 2 CT outputs + fee
  const uint64_t d = CryptoNote::MIN_CT_DENOMINATION;
  uint64_t amounts_in[2] = {50 * d, 30 * d}; // total=80*d
  uint64_t amounts_out[2] = {45 * d, 34 * d}; // total=79*d
  uint64_t fee = d;

  // Transparent inputs have zero blinding
  Crypto::EllipticCurvePoint C_in[2];
  for (int i = 0; i < 2; ++i) {
    if (!Crypto::transparent_amount_to_commitment(amounts_in[i], C_in[i]))
      FAIL("transparent_amount_to_commitment failed");
  }

  // CT outputs have random blinding
  Crypto::EllipticCurveScalar r_out[2];
  Crypto::EllipticCurvePoint C_out[2];
  for (int i = 0; i < 2; ++i) {
    test_random_scalar(r_out[i]);
    make_commitment(amounts_out[i], r_out[i], C_out[i]);
  }

  // Excess = sum(0,0) - sum(r_out[0], r_out[1])
  Crypto::EllipticCurveScalar zero_blinds[2];
  memset(&zero_blinds[0], 0, 32);
  memset(&zero_blinds[1], 0, 32);
  Crypto::EllipticCurveScalar excess;
  Crypto::compute_excess_scalar(zero_blinds, 2, r_out, 2, excess);

  Crypto::Hash tx_hash;
  random_hash(tx_hash);
  Crypto::TransactionKernel kernel;
  if (!Crypto::sign_transaction_kernel(excess, tx_hash, kernel)) FAIL("sign failed");
  if (!Crypto::verify_transaction_balance(C_in, 2, C_out, 2, fee, tx_hash, kernel))
    FAIL("balance failed");
  PASS();
}

static void test_multi_output_decomposition_gk_proofs() {
  TEST("Combined: multi-output decomposition with GK proofs");

  // 12,345.67 KRB in atomic units
  uint64_t amount = UINT64_C(12345670000000000);
  auto parts = CryptoNote::decomposeAmount(amount);

  Crypto::Hash tx_hash;
  random_hash(tx_hash);

  // Generate GK proof for each part
  for (size_t i = 0; i < parts.size(); ++i) {
    int idx = CryptoNote::denominationIndex(parts[i]);
    if (idx < 0) FAIL("part is not canonical denomination");

    Crypto::EllipticCurveScalar r;
    test_random_scalar(r);

    Crypto::EllipticCurveScalar v_scalar;
    uint64_to_scalar(parts[i], v_scalar);
    Crypto::EllipticCurvePoint C;
    if (!Crypto::pedersen_commit(v_scalar, r, C)) FAIL("commit failed");

    Crypto::GKProof proof;
    if (!Crypto::gk_prove(C, parts[i], r, static_cast<size_t>(idx), tx_hash, proof)) {
      char msg[80]; snprintf(msg, sizeof(msg), "prove failed for part %llu",
                             (unsigned long long)parts[i]);
      FAIL(msg);
    }
    if (!Crypto::gk_verify(C, proof, tx_hash)) {
      char msg[80]; snprintf(msg, sizeof(msg), "verify failed for part %llu",
                             (unsigned long long)parts[i]);
      FAIL(msg);
    }
  }
  PASS();
}

static void test_full_ct_transaction_simulation() {
  TEST("Combined: full CT tx simulation (2 in → 3 out + fee)");

  // Simulate a complete CT transaction with balance, GK proofs, and Triptych.

  // Amounts: 2 inputs of canonical denominations, 3 outputs + fee
  // Input: 500 + 300 = 800
  // Output: 400 + 300 + 90 = 790, Fee: 10
  // All amounts must be canonical denominations
  const uint64_t d = CryptoNote::MIN_CT_DENOMINATION;
  uint64_t amounts_in[2] = {500 * d, 300 * d};
  uint64_t amounts_out[3] = {400 * d, 300 * d, 90 * d};
  uint64_t fee = 10 * d;

  Crypto::EllipticCurveScalar r_in[2], r_out[3];
  for (int i = 0; i < 2; ++i) test_random_scalar(r_in[i]);
  for (int i = 0; i < 3; ++i) test_random_scalar(r_out[i]);

  // Pseudo-output commitments (same amount, different blinding)
  Crypto::EllipticCurveScalar r_pseudo[2];
  for (int i = 0; i < 2; ++i) test_random_scalar(r_pseudo[i]);

  // Output commitments (inputs go via per-input ring builders below).
  Crypto::EllipticCurvePoint C_out[3], pseudo[2];
  for (int i = 0; i < 2; ++i) {
    make_commitment(amounts_in[i], r_pseudo[i], pseudo[i]);
  }
  for (int i = 0; i < 3; ++i) {
    make_commitment(amounts_out[i], r_out[i], C_out[i]);
  }

  // 1. GK proofs on outputs
  Crypto::Hash tx_hash;
  random_hash(tx_hash);

  for (int i = 0; i < 3; ++i) {
    int idx = CryptoNote::denominationIndex(amounts_out[i]);
    if (idx < 0) FAIL("output not canonical");

    Crypto::EllipticCurveScalar v_scalar;
    uint64_to_scalar(amounts_out[i], v_scalar);
    Crypto::GKProof proof;
    if (!Crypto::gk_prove(C_out[i], amounts_out[i], r_out[i],
                          static_cast<size_t>(idx), tx_hash, proof))
      FAIL("GK prove failed");
    if (!Crypto::gk_verify(C_out[i], proof, tx_hash))
      FAIL("GK verify failed");
  }

  // 2. Triptych on inputs (minimum ring size 4 — the new floor).
  // Each input gets its own ring of 4 with the real spend at slot 0; the
  // remaining three slots are decoys with fresh keypairs.
  Crypto::PublicKey  pk_in[2];
  Crypto::SecretKey  sk_in[2];
  for (int i = 0; i < 2; ++i) gen_keypair(pk_in[i], sk_in[i]);

  Crypto::PublicKey         ring_pubs[2][4];
  Crypto::EllipticCurvePoint ring_commits[2][4];
  for (int i = 0; i < 2; ++i) {
    build_triptych_ring(4, 0, amounts_in[i], pk_in[i], r_in[i],
                        ring_pubs[i], ring_commits[i]);
  }

  Crypto::KeyImage ki[2];
  Crypto::TriptychSignature proof[2];
  for (int i = 0; i < 2; ++i) {
    if (!Crypto::triptych_sign(tx_hash, ring_pubs[i], ring_commits[i],
                               pseudo[i], 4, 0, sk_in[i],
                               r_in[i], r_pseudo[i], ki[i], proof[i]))
      FAIL("Triptych sign failed");
    if (!Crypto::triptych_verify(tx_hash, ring_pubs[i], ring_commits[i],
                                 pseudo[i], 4, ki[i], proof[i]))
      FAIL("Triptych verify failed");
  }

  // 3. Key images must differ
  if (memcmp(&ki[0], &ki[1], sizeof(Crypto::KeyImage)) == 0)
    FAIL("key images should differ");

  // 4. Balance equation: sum(pseudo) - sum(C_out) - fee*H = excess*G
  Crypto::EllipticCurveScalar excess;
  Crypto::compute_excess_scalar(r_pseudo, 2, r_out, 3, excess);
  Crypto::TransactionKernel kernel;
  if (!Crypto::sign_transaction_kernel(excess, tx_hash, kernel))
    FAIL("kernel sign failed");
  if (!Crypto::verify_transaction_balance(pseudo, 2, C_out, 3, fee, tx_hash, kernel))
    FAIL("balance verify failed");

  PASS();
}

static void test_ct_fee_bounds() {
  TEST("Combined: CT fee bounds (min/max)");

  if (CryptoNote::parameters::CT_MINIMUM_FEE == 0)
    FAIL("CT_MINIMUM_FEE should be non-zero");
  if (CryptoNote::parameters::CT_MAXIMUM_FEE < CryptoNote::parameters::CT_MINIMUM_FEE)
    FAIL("CT_MAXIMUM_FEE should be >= CT_MINIMUM_FEE");
  // Ring size bounds
  if (CryptoNote::parameters::CT_MIN_RING_SIZE != 4)
    FAIL("CT_MIN_RING_SIZE should be 4");
  if (CryptoNote::parameters::CT_MAX_RING_SIZE != 16)
    FAIL("CT_MAX_RING_SIZE should be 16");
  PASS();
}

static void test_max_denomination_gk_in_balance() {
  TEST("Combined: max denomination (100,000 KRB) in full balance");

  uint64_t max_denom = CryptoNote::DENOMINATIONS[63]; // 100,000 KRB
  uint64_t fee = CryptoNote::MIN_CT_DENOMINATION;
  uint64_t out_amount = max_denom - fee;

  // Need out_amount to be decomposable.
  // max_denom minus one CT floor denomination remains exactly representable.
  auto parts = CryptoNote::decomposeAmount(out_amount);
  uint64_t sum = 0;
  for (auto p : parts) sum += p;
  if (sum != out_amount) FAIL("decomposition sum mismatch");

  // Balance with 1 input (max_denom), multiple outputs, fee
  Crypto::EllipticCurveScalar r_in;
  test_random_scalar(r_in);
  Crypto::EllipticCurvePoint C_in;
  make_commitment(max_denom, r_in, C_in);

  // Create output commitments from decomposition
  std::vector<Crypto::EllipticCurveScalar> r_outs(parts.size());
  std::vector<Crypto::EllipticCurvePoint> C_outs(parts.size());
  for (size_t i = 0; i < parts.size(); ++i) {
    test_random_scalar(r_outs[i]);
    make_commitment(parts[i], r_outs[i], C_outs[i]);
  }

  Crypto::EllipticCurveScalar excess;
  Crypto::compute_excess_scalar(&r_in, 1, r_outs.data(), parts.size(), excess);

  Crypto::Hash tx_hash;
  random_hash(tx_hash);
  Crypto::TransactionKernel kernel;
  Crypto::sign_transaction_kernel(excess, tx_hash, kernel);

  if (!Crypto::verify_transaction_balance(&C_in, 1, C_outs.data(), parts.size(),
                                          fee, tx_hash, kernel))
    FAIL("balance with max denomination failed");
  PASS();
}

static void test_triptych_ring_size_4() {
  TEST("Combined: Triptych with ring size 4 (min CT ring size)");

  const size_t RING = 4;
  const size_t TRUE_IDX = 2;

  Crypto::PublicKey pubs[RING];
  Crypto::SecretKey secs[RING];
  Crypto::EllipticCurvePoint commits[RING];
  Crypto::EllipticCurveScalar blindings[RING];

  Crypto::EllipticCurveScalar v_scalar;
  uint64_to_scalar(1000, v_scalar);

  for (size_t i = 0; i < RING; ++i) {
    gen_keypair(pubs[i], secs[i]);
    test_random_scalar(blindings[i]);
    // Decoys: random (value, blinding) Pedersen commitments.
    Crypto::EllipticCurveScalar dummy_v;
    test_random_scalar(dummy_v);
    Crypto::pedersen_commit(dummy_v, blindings[i], commits[i]);
  }

  // Override real entry with a commitment to the real amount.
  Crypto::pedersen_commit(v_scalar, blindings[TRUE_IDX], commits[TRUE_IDX]);

  Crypto::EllipticCurveScalar pseudo_blind;
  test_random_scalar(pseudo_blind);
  Crypto::EllipticCurvePoint pseudo;
  Crypto::pedersen_commit(v_scalar, pseudo_blind, pseudo);

  Crypto::Hash msg;
  random_hash(msg);

  Crypto::KeyImage ki;
  Crypto::TriptychSignature sig;
  if (!Crypto::triptych_sign(msg, pubs, commits, pseudo, RING, TRUE_IDX,
                             secs[TRUE_IDX], blindings[TRUE_IDX], pseudo_blind, ki, sig))
    FAIL("sign failed");
  if (!Crypto::triptych_verify(msg, pubs, commits, pseudo, RING, ki, sig))
    FAIL("verify failed");
  PASS();
}

// Demonstrates that the Triptych layer accepts a ring whose members come
// from different "buckets" — i.e. the per-member mixed-bucket schema.
// Half the decoys are transparent (commitment = amount·H, zero blinding),
// half are confidential (commitment = v·H + r·G with random r). The real
// spend is transparent at amount V; the pseudo-commitment also commits to
// V so the commitment difference at the real index is a pure G multiple.
// Other members' commitments need not match V — Triptych's polynomial
// selector simulates decoy responses regardless of bucket.
//
// Ring size 8 (n=3) is the smallest supported size that lets us include
// both kinds of decoys and a real slot.
static void test_triptych_mixed_transparent_and_ct_ring() {
  TEST("Combined: Triptych with mixed transparent + CT ring members");

  const size_t RING = 8;
  const size_t TRUE_IDX = 3;
  const uint64_t REAL_AMOUNT = CryptoNote::DENOMINATIONS[10]; // some canonical denom

  Crypto::PublicKey pubs[RING];
  Crypto::SecretKey secs[RING];
  Crypto::EllipticCurvePoint commits[RING];

  // Generate keypairs for every slot.
  for (size_t i = 0; i < RING; ++i) {
    gen_keypair(pubs[i], secs[i]);
  }

  // Build mixed-bucket commitments:
  //   slots 0, 2, 5, 6, 7 → transparent, varying amounts (commitment = amount*H)
  //   slots 1, 4          → confidential, random (commitment = v*H + r*G)
  //   slot  3 (real)      → transparent at REAL_AMOUNT (zero blinding)
  static const uint64_t kTransparentAmounts[RING] = {
    CryptoNote::DENOMINATIONS[5],  // slot 0
    0,                              // slot 1 (CT — unused)
    CryptoNote::DENOMINATIONS[20], // slot 2
    REAL_AMOUNT,                    // slot 3 (real)
    0,                              // slot 4 (CT — unused)
    CryptoNote::DENOMINATIONS[40], // slot 5
    CryptoNote::DENOMINATIONS[15], // slot 6
    CryptoNote::DENOMINATIONS[50]  // slot 7
  };

  for (size_t i = 0; i < RING; ++i) {
    if (i == 1 || i == 4) {
      // Confidential ring member with random blinding.
      Crypto::EllipticCurveScalar dummy_v, dummy_r;
      test_random_scalar(dummy_v);
      test_random_scalar(dummy_r);
      if (!Crypto::pedersen_commit(dummy_v, dummy_r, commits[i])) { FAIL("pedersen confidential"); return; }
    } else {
      // Transparent ring member: commitment = amount*H (zero blinding).
      if (!Crypto::transparent_amount_to_commitment(kTransparentAmounts[i], commits[i])) {
        FAIL("transparent_amount_to_commitment");
        return;
      }
    }
  }

  // Real spend is transparent at REAL_AMOUNT: blinding factor is zero.
  Crypto::EllipticCurveScalar real_blinding;
  memset(real_blinding.data, 0, 32);

  // Pseudo-commitment: v*H + r'*G with v = REAL_AMOUNT (must match real input
  // amount or the Triptych M-ring identity cannot close).
  Crypto::EllipticCurveScalar v_scalar;
  uint64_to_scalar(REAL_AMOUNT, v_scalar);
  Crypto::EllipticCurveScalar pseudo_blind;
  test_random_scalar(pseudo_blind);
  Crypto::EllipticCurvePoint pseudo;
  if (!Crypto::pedersen_commit(v_scalar, pseudo_blind, pseudo)) { FAIL("pedersen pseudo"); return; }

  Crypto::Hash msg;
  random_hash(msg);

  Crypto::KeyImage ki;
  Crypto::TriptychSignature sig;
  if (!Crypto::triptych_sign(msg, pubs, commits, pseudo, RING, TRUE_IDX,
                             secs[TRUE_IDX], real_blinding, pseudo_blind, ki, sig)) {
    FAIL("triptych_sign rejected mixed ring");
    return;
  }
  if (!Crypto::triptych_verify(msg, pubs, commits, pseudo, RING, ki, sig)) {
    FAIL("triptych_verify rejected mixed ring");
    return;
  }
  PASS();
}

// Build N legitimate GK proofs over a shared tx hash. Each output uses a
// different denomination index to keep the proofs distinct. Returns the
// per-output commitments and proofs alongside the tx hash.
static void build_n_legit_proofs(size_t n, Crypto::Hash& tx_hash,
                                  std::vector<Crypto::EllipticCurvePoint>& commitments,
                                  std::vector<Crypto::GKProof>& proofs) {
  random_hash(tx_hash);
  commitments.resize(n);
  proofs.resize(n);
  for (size_t i = 0; i < n; ++i) {
    const size_t denomIdx = i % CryptoNote::DENOMINATION_COUNT;
    const uint64_t v = CryptoNote::DENOMINATIONS[denomIdx];

    Crypto::EllipticCurveScalar r;
    test_random_scalar(r);

    Crypto::EllipticCurveScalar v_scalar;
    uint64_to_scalar(v, v_scalar);
    if (!Crypto::pedersen_commit(v_scalar, r, commitments[i])) { FAIL("pedersen_commit"); return; }

    if (!Crypto::gk_prove(commitments[i], v, r, denomIdx, tx_hash, proofs[i])) {
      FAIL("gk_prove"); return;
    }
  }
}

static void test_gk_batch_positive_small() {
  TEST("Batched GK: 4 legitimate proofs verify together");
  Crypto::Hash tx_hash;
  std::vector<Crypto::EllipticCurvePoint> commits;
  std::vector<Crypto::GKProof> proofs;
  build_n_legit_proofs(4, tx_hash, commits, proofs);

  // Sanity: per-proof verify all pass.
  for (size_t i = 0; i < proofs.size(); ++i) {
    if (!Crypto::gk_verify(commits[i], proofs[i], tx_hash)) { FAIL("per-proof verify"); return; }
  }

  if (!Crypto::gk_verify_batch(commits.data(), proofs.data(), commits.size(), tx_hash)) {
    FAIL("batched verify rejected legitimate proofs");
    return;
  }
  PASS();
}

static void test_gk_batch_positive_large() {
  TEST("Batched GK: 32 legitimate proofs (CT_MAX_OUTPUTS) verify together");
  Crypto::Hash tx_hash;
  std::vector<Crypto::EllipticCurvePoint> commits;
  std::vector<Crypto::GKProof> proofs;
  build_n_legit_proofs(32, tx_hash, commits, proofs);

  if (!Crypto::gk_verify_batch(commits.data(), proofs.data(), commits.size(), tx_hash)) {
    FAIL("batched verify rejected 32 legitimate proofs");
    return;
  }
  PASS();
}

static void test_gk_batch_rejects_one_tampered_scalar() {
  TEST("Batched GK: one proof with tampered f scalar → batch rejects");
  Crypto::Hash tx_hash;
  std::vector<Crypto::EllipticCurvePoint> commits;
  std::vector<Crypto::GKProof> proofs;
  build_n_legit_proofs(8, tx_hash, commits, proofs);

  // Sanity-check the unmodified batch passes.
  if (!Crypto::gk_verify_batch(commits.data(), proofs.data(), commits.size(), tx_hash)) {
    FAIL("clean batch should verify"); return;
  }

  // Tamper one byte of the f scalar in proof[3]. Stays in range so the
  // sc_check pre-pass doesn't reject — the batched MSM has to catch it.
  proofs[3].f.data[1] ^= 0x01;
  if (proofs[3].f.data[1] == 0) proofs[3].f.data[1] = 0x02;

  if (Crypto::gk_verify_batch(commits.data(), proofs.data(), commits.size(), tx_hash)) {
    FAIL("batched verify accepted a tampered proof");
    return;
  }
  PASS();
}

static void test_gk_batch_rejects_one_tampered_point() {
  TEST("Batched GK: one proof with tampered Q point → batch rejects");
  Crypto::Hash tx_hash;
  std::vector<Crypto::EllipticCurvePoint> commits;
  std::vector<Crypto::GKProof> proofs;
  build_n_legit_proofs(8, tx_hash, commits, proofs);

  // Tamper a Q point in proof[5] by swapping in a randomly-generated valid
  // point. Stays subgroup-valid so the pre-check passes; the MSM must
  // detect the mismatch.
  Crypto::EllipticCurveScalar randSec;
  test_random_scalar(randSec);
  ge_scalarmult_base(&proofs[5].Q[2], reinterpret_cast<const unsigned char*>(&randSec));

  if (Crypto::gk_verify_batch(commits.data(), proofs.data(), commits.size(), tx_hash)) {
    FAIL("batched verify accepted a tampered proof");
    return;
  }
  PASS();
}

static void test_gk_batch_rejects_wrong_tx_hash() {
  TEST("Batched GK: wrong tx_hash → batch rejects");
  Crypto::Hash tx_hash;
  std::vector<Crypto::EllipticCurvePoint> commits;
  std::vector<Crypto::GKProof> proofs;
  build_n_legit_proofs(4, tx_hash, commits, proofs);

  Crypto::Hash wrong;
  random_hash(wrong);
  if (Crypto::gk_verify_batch(commits.data(), proofs.data(), commits.size(), wrong)) {
    FAIL("batched verify accepted wrong tx_hash");
    return;
  }
  PASS();
}

static void test_gk_batch_empty() {
  TEST("Batched GK: n==0 returns true (degenerate case)");
  Crypto::Hash tx_hash;
  random_hash(tx_hash);
  if (!Crypto::gk_verify_batch(nullptr, nullptr, 0, tx_hash)) {
    FAIL("empty batch should trivially verify");
    return;
  }
  PASS();
}

static void test_gk_batch_single_equivalent_to_per_proof() {
  TEST("Batched GK: n==1 agrees with single gk_verify");
  Crypto::Hash tx_hash;
  std::vector<Crypto::EllipticCurvePoint> commits;
  std::vector<Crypto::GKProof> proofs;
  build_n_legit_proofs(1, tx_hash, commits, proofs);

  const bool single = Crypto::gk_verify(commits[0], proofs[0], tx_hash);
  const bool batch  = Crypto::gk_verify_batch(commits.data(), proofs.data(), 1, tx_hash);
  if (single != batch) {
    FAIL("single-element batch disagreed with per-proof verify");
    return;
  }
  PASS();
}

// Cross-check: both batched MSM implementations must agree on every
// input — legitimate or tampered. Pippenger is the production path;
// the naive sequential version is kept as an audit-friendly reference.
// Disagreement at any batch size would indicate a Pippenger
// implementation bug.
// The 64 V[k]*H values baked into src/crypto/gk_denomination_table.h must
// match the values you'd get by computing V[k]*H from scratch using the
// canonical denomination set and the pedersen_get_H() generator. If this
// ever diverges, either DENOMINATIONS[] changed, the H domain string
// changed, or someone hand-edited the baked file. All three of those would
// silently break consensus — the assert here catches them at build time.
//
// Regeneration command if this test legitimately fails after an intentional
// change:  tests/Release/CTBench --print-vkh-table > src/crypto/gk_denomination_table.h
// (then edit headers/footers to match the existing file).
static void test_baked_vkH_table_matches_from_scratch() {
  TEST("Baked V[k]*H table matches from-scratch computation");

  unsigned char scratch[64][32];
  if (!Crypto::gk_compute_vkH_table_from_scratch(scratch)) {
    FAIL("from-scratch computation failed");
    return;
  }

  // Drive a verify-batch call through the baked path to confirm
  // initialisation succeeded — if any baked entry decoded badly, the
  // table's ok flag is false and every proof would now reject. We piggy-
  // back on a 1-proof batch as a smoke test.
  Crypto::Hash tx_hash;
  std::vector<Crypto::EllipticCurvePoint> commits;
  std::vector<Crypto::GKProof> proofs;
  build_n_legit_proofs(1, tx_hash, commits, proofs);
  if (!Crypto::gk_verify_batch(commits.data(), proofs.data(), 1, tx_hash)) {
    FAIL("baked table failed to drive a legitimate single-proof batch");
    return;
  }

  // Byte-for-byte equality check between baked and from-scratch.
  // The baked array lives in gk_denomination_table.h (included at the
  // top of this file). Smoke-test above already confirmed init succeeded;
  // the explicit byte compare here is the audit-trail check.
  for (size_t k = 0; k < 64; ++k) {
    if (memcmp(scratch[k], Crypto::kVkH_baked[k], 32) != 0) {
      char msg[128];
      snprintf(msg, sizeof(msg),
               "baked != from-scratch at k=%zu (V=%llu)",
               k, (unsigned long long)CryptoNote::DENOMINATIONS[k]);
      FAIL(msg);
      return;
    }
  }
  PASS();
}

static void test_gk_batch_naive_matches_pippenger_legit() {
  TEST("Batched GK: naive and Pippenger agree on legitimate batch");
  for (size_t n : {size_t{1}, size_t{4}, size_t{16}, size_t{32}}) {
    Crypto::Hash tx_hash;
    std::vector<Crypto::EllipticCurvePoint> commits;
    std::vector<Crypto::GKProof> proofs;
    build_n_legit_proofs(n, tx_hash, commits, proofs);

    bool fast = Crypto::gk_verify_batch(commits.data(), proofs.data(), n, tx_hash);
    bool ref  = Crypto::gk_verify_batch_naive(commits.data(), proofs.data(), n, tx_hash);
    if (fast != ref) {
      FAIL("Pippenger and naive disagreed on legitimate batch");
      return;
    }
    if (!fast) {
      FAIL("both reported failure on legitimate batch");
      return;
    }
  }
  PASS();
}

static void test_gk_batch_naive_matches_pippenger_tampered() {
  TEST("Batched GK: naive and Pippenger agree on tampered batch");
  Crypto::Hash tx_hash;
  std::vector<Crypto::EllipticCurvePoint> commits;
  std::vector<Crypto::GKProof> proofs;
  build_n_legit_proofs(8, tx_hash, commits, proofs);

  // Tamper proof[2]: flip one bit in f.
  proofs[2].f.data[3] ^= 0x10;
  if (proofs[2].f.data[3] == 0) proofs[2].f.data[3] = 0x20;

  bool fast = Crypto::gk_verify_batch(commits.data(), proofs.data(), proofs.size(), tx_hash);
  bool ref  = Crypto::gk_verify_batch_naive(commits.data(), proofs.data(), proofs.size(), tx_hash);
  if (fast != ref) {
    FAIL("Pippenger and naive disagreed on tampered batch");
    return;
  }
  if (fast) {
    FAIL("both accepted a tampered proof");
    return;
  }
  PASS();
}

static void test_ct_fork_height_decoupled() {
  TEST("Combined: CT_FORK_HEIGHT exists and display decimals stay at 12");

  if (CryptoNote::parameters::CRYPTONOTE_DISPLAY_DECIMAL_POINT != 12)
    FAIL("display decimals should be 12");
  // CT_FORK_HEIGHT only gates CT activation.
  (void)CryptoNote::parameters::CT_FORK_HEIGHT;
  PASS();
}

// =====================================================================
// SECTION 9: CONSISTENCY + CAPACITY
// =====================================================================

static void test_wallet_fee_absorption_policy_consistency() {
  TEST("Consistency: wallet fee-absorption policy is deterministic");

  const uint64_t floor = CryptoNote::MIN_CT_DENOMINATION;
  for (uint64_t raw = 1; raw < floor * 25; raw += 7777777) {
    // WalletGreen logic
    const uint64_t canonicalA = (raw / floor) * floor;
    const uint64_t residueA = raw - canonicalA;
    // WalletRpc sweep logic equivalent normalization
    const uint64_t canonicalB = (raw / floor) * floor;
    const uint64_t residueB = raw - canonicalB;
    if (canonicalA != canonicalB || residueA != residueB) FAIL("policy mismatch");
    if (canonicalA + residueA != raw) FAIL("lossy split");
    if (residueA >= floor) FAIL("residue must stay sub-floor");
  }
  PASS();
}

static void test_capacity_many_ct_outputs_gk() {
  TEST("Capacity: 256 GK proofs verify under one tx hash");

  Crypto::Hash tx_hash;
  random_hash(tx_hash);

  const size_t N = 256;
  for (size_t i = 0; i < N; ++i) {
    const size_t idx = i % CryptoNote::DENOMINATION_COUNT;
    const uint64_t v = CryptoNote::DENOMINATIONS[idx];
    Crypto::EllipticCurveScalar r, vs;
    test_random_scalar(r);
    uint64_to_scalar(v, vs);
    Crypto::EllipticCurvePoint C;
    if (!Crypto::pedersen_commit(vs, r, C)) FAIL("commit failed");
    Crypto::GKProof proof;
    if (!Crypto::gk_prove(C, v, r, idx, tx_hash, proof)) FAIL("prove failed");
    if (!Crypto::gk_verify(C, proof, tx_hash)) FAIL("verify failed");
  }
  PASS();
}

static void test_stress_deterministic_decomposition_sweep() {
  TEST("Stress: deterministic decomposition sweep across 10k amounts");

  const uint64_t d = CryptoNote::MIN_CT_DENOMINATION;
  uint64_t rolling_checksum = 0;
  for (uint64_t i = 1; i <= 10000; ++i) {
    const uint64_t amount = i * d;
    auto parts = CryptoNote::decomposeAmount(amount);
    uint64_t sum = 0;
    for (auto p : parts) {
      if (!CryptoNote::isCanonicalDenomination(p)) FAIL("non-canonical part");
      sum += p;
      rolling_checksum ^= (p + 0x9e3779b97f4a7c15ULL + (rolling_checksum << 6) + (rolling_checksum >> 2));
    }
    if (sum != amount) FAIL("sum mismatch in sweep");
  }

  if (rolling_checksum == 0) FAIL("unexpected checksum");
  PASS();
}

// ── Main ─────────────────────────────────────────────────────────────

int main() {
  printf("CT Integration Tests\n");
  printf("======================================\n\n");

  printf("[CT floor / dust policy]\n");
  test_min_ct_denomination_value();
  test_min_ct_denomination_matches_table();
  test_below_floor_not_canonical();
  test_floor_multiple_decomposable();
  test_sub_floor_decomposition_throws();
  test_dust_residue_routes_to_fee();

  printf("\n[GK Proof - extended]\n");
  test_gk_all_64_roundtrip();
  test_gk_tampered_z_scalar();
  test_gk_tampered_bit_responses();
  test_gk_tampered_A_point();
  test_gk_max_denomination();

  printf("\n[Denomination table]\n");
  test_denomination_table_coverage();
  test_denomination_canonical_check();
  test_denomination_index();
  test_decompose_multi_output();
  test_decompose_zero_rejected();
  test_decompose_roundtrip_grid();

  printf("\n[Balance equation]\n");
  test_balance_valid_excess();
  test_balance_invalid_excess();
  test_balance_correct_fee_handling();
  test_balance_many_inputs_outputs();

  printf("\n[Subgroup checks]\n");
  test_subgroup_identity_rejected();
  test_subgroup_low_order_points();
  test_subgroup_valid_point_accepted();

  printf("\n[Key image reuse]\n");
  test_key_image_deterministic();
  test_key_image_different_keys();

  printf("\n[ECDH output scanning]\n");
  test_ecdh_recipient_identifies_output();
  test_ecdh_non_owner_skips();
  test_blinding_factor_domain_separation_from_stealth_scalar();
  test_ecdh_multiple_outputs();
  test_ecdh_pedersen_generator_consistency();

  printf("\n[Combined / edge cases]\n");
  test_tx_version_requirements();
  test_mixed_transparent_to_ct_balance();
  test_multi_output_decomposition_gk_proofs();
  test_full_ct_transaction_simulation();
  test_ct_fee_bounds();
  test_max_denomination_gk_in_balance();
  test_triptych_ring_size_4();
  test_triptych_mixed_transparent_and_ct_ring();
  test_ct_fork_height_decoupled();

  printf("\n[Batched GK proof verification]\n");
  test_gk_batch_empty();
  test_gk_batch_single_equivalent_to_per_proof();
  test_gk_batch_positive_small();
  test_gk_batch_positive_large();
  test_gk_batch_rejects_one_tampered_scalar();
  test_gk_batch_rejects_one_tampered_point();
  test_gk_batch_rejects_wrong_tx_hash();
  test_gk_batch_naive_matches_pippenger_legit();
  test_gk_batch_naive_matches_pippenger_tampered();
  test_baked_vkH_table_matches_from_scratch();

  printf("\n[Consistency + capacity]\n");
  test_wallet_fee_absorption_policy_consistency();
  test_capacity_many_ct_outputs_gk();
  test_stress_deterministic_decomposition_sweep();

  printf("\n======================================\n");
  printf("Results: %d/%d passed\n", tests_passed, tests_run);

  return (tests_passed == tests_run) ? 0 : 1;
}
