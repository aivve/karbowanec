// Standalone test for transaction balance equation and kernel signatures.
// Build: cl /EHsc /I../include /I../src test_transaction_balance.cpp /link ../build/src/Release/Crypto.lib

#include "crypto/transaction_balance.h"
#include "crypto/pedersen.h"
#include "crypto/random.h"
#include "crypto/crypto.h"

#include <cstdio>
#include <cstring>
#include <vector>

extern "C" {
#include "crypto/crypto-ops.h"
}

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
  bool ok = Crypto::pedersen_commit(v, blinding, commitment);
  if (!ok) {
    printf("FATAL: pedersen_commit failed\n");
    abort();
  }
}

static void random_hash(Crypto::Hash& h) {
  Random::randomBytes(32, h.data);
}

static bool make_basepoint_plus_order2(Crypto::EllipticCurvePoint& result) {
  static const unsigned char order2[32] = {
    0xec, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
    0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
    0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
    0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x7f
  };

  unsigned char one[32] = {0};
  one[0] = 1;

  ge_p3 base;
  ge_scalarmult_base(&base, one);

  ge_p3 torsion;
  if (ge_frombytes_vartime(&torsion, order2) != 0) {
    return false;
  }

  ge_cached torsion_cached;
  ge_p3_to_cached(&torsion_cached, &torsion);

  ge_p1p1 sum;
  ge_add(&sum, &base, &torsion_cached);

  ge_p3 sum_p3;
  ge_p1p1_to_p3(&sum_p3, &sum);
  ge_p3_tobytes(reinterpret_cast<unsigned char*>(&result), &sum_p3);
  return true;
}

static int tests_run = 0;
static int tests_passed = 0;

#define TEST(name) \
  do { \
    ++tests_run; \
    printf("  %-60s", name); \
  } while (0)

#define PASS() \
  do { \
    ++tests_passed; \
    printf("[PASS]\n"); \
  } while (0)

#define FAIL(msg) \
  do { \
    printf("[FAIL] %s\n", msg); \
    return; \
  } while (0)

// ── Test 1: Basic balance equation (1 input, 1 output, fee) ─────────

static void test_basic_balance() {
  TEST("Basic balance: 1 input -> 1 output + fee");

  uint64_t amount_in = 100;
  uint64_t amount_out = 90;
  uint64_t fee = 10;

  Crypto::EllipticCurveScalar r_in, r_out, excess;
  test_random_scalar(r_in);
  test_random_scalar(r_out);

  // Compute excess = r_in - r_out
  Crypto::compute_excess_scalar(&r_in, 1, &r_out, 1, excess);

  // Build commitments
  Crypto::EllipticCurvePoint C_in, C_out;
  make_commitment(amount_in, r_in, C_in);
  make_commitment(amount_out, r_out, C_out);

  // Sign kernel
  Crypto::Hash tx_hash;
  random_hash(tx_hash);

  Crypto::TransactionKernel kernel;
  bool sign_ok = Crypto::sign_transaction_kernel(excess, tx_hash, kernel);
  if (!sign_ok) FAIL("sign_transaction_kernel failed");

  // Verify
  bool verify_ok = Crypto::verify_transaction_balance(
    &C_in, 1, &C_out, 1, fee, tx_hash, kernel);
  if (!verify_ok) FAIL("verify_transaction_balance returned false");

  PASS();
}

// ── Test 2: Multiple inputs and outputs ──────────────────────────────

static void test_multi_io() {
  TEST("Multi I/O: 3 inputs -> 2 outputs + fee");

  uint64_t amounts_in[3] = {50, 30, 20};
  uint64_t amounts_out[2] = {60, 35};
  uint64_t fee = 5; // 100 - 95 = 5

  Crypto::EllipticCurveScalar r_in[3], r_out[2], excess;
  for (int i = 0; i < 3; ++i) test_random_scalar(r_in[i]);
  for (int i = 0; i < 2; ++i) test_random_scalar(r_out[i]);

  Crypto::compute_excess_scalar(r_in, 3, r_out, 2, excess);

  Crypto::EllipticCurvePoint C_in[3], C_out[2];
  for (int i = 0; i < 3; ++i) make_commitment(amounts_in[i], r_in[i], C_in[i]);
  for (int i = 0; i < 2; ++i) make_commitment(amounts_out[i], r_out[i], C_out[i]);

  Crypto::Hash tx_hash;
  random_hash(tx_hash);

  Crypto::TransactionKernel kernel;
  bool sign_ok = Crypto::sign_transaction_kernel(excess, tx_hash, kernel);
  if (!sign_ok) FAIL("sign_transaction_kernel failed");

  bool verify_ok = Crypto::verify_transaction_balance(
    C_in, 3, C_out, 2, fee, tx_hash, kernel);
  if (!verify_ok) FAIL("verify_transaction_balance returned false");

  PASS();
}

// ── Test 3: Zero fee ─────────────────────────────────────────────────

static void test_zero_fee() {
  TEST("Zero fee: balance with fee = 0");

  uint64_t amount_in = 100;
  uint64_t amount_out = 100;
  uint64_t fee = 0;

  Crypto::EllipticCurveScalar r_in, r_out, excess;
  test_random_scalar(r_in);
  test_random_scalar(r_out);
  Crypto::compute_excess_scalar(&r_in, 1, &r_out, 1, excess);

  Crypto::EllipticCurvePoint C_in, C_out;
  make_commitment(amount_in, r_in, C_in);
  make_commitment(amount_out, r_out, C_out);

  Crypto::Hash tx_hash;
  random_hash(tx_hash);

  Crypto::TransactionKernel kernel;
  bool sign_ok = Crypto::sign_transaction_kernel(excess, tx_hash, kernel);
  if (!sign_ok) FAIL("sign_transaction_kernel failed");

  bool verify_ok = Crypto::verify_transaction_balance(
    &C_in, 1, &C_out, 1, fee, tx_hash, kernel);
  if (!verify_ok) FAIL("verify_transaction_balance returned false");

  PASS();
}

// ── Test 4: Wrong fee breaks verification ────────────────────────────

static void test_wrong_fee() {
  TEST("Wrong fee: verification fails with incorrect fee");

  uint64_t amount_in = 100;
  uint64_t amount_out = 90;
  uint64_t fee = 10;

  Crypto::EllipticCurveScalar r_in, r_out, excess;
  test_random_scalar(r_in);
  test_random_scalar(r_out);
  Crypto::compute_excess_scalar(&r_in, 1, &r_out, 1, excess);

  Crypto::EllipticCurvePoint C_in, C_out;
  make_commitment(amount_in, r_in, C_in);
  make_commitment(amount_out, r_out, C_out);

  Crypto::Hash tx_hash;
  random_hash(tx_hash);

  Crypto::TransactionKernel kernel;
  Crypto::sign_transaction_kernel(excess, tx_hash, kernel);

  // Try with wrong fee
  bool verify_ok = Crypto::verify_transaction_balance(
    &C_in, 1, &C_out, 1, fee + 1, tx_hash, kernel);
  if (verify_ok) FAIL("should reject wrong fee");

  PASS();
}

// ── Test 5: Tampered signature ───────────────────────────────────────

static void test_tampered_signature() {
  TEST("Tampered signature: verification fails");

  uint64_t amount_in = 100;
  uint64_t amount_out = 90;
  uint64_t fee = 10;

  Crypto::EllipticCurveScalar r_in, r_out, excess;
  test_random_scalar(r_in);
  test_random_scalar(r_out);
  Crypto::compute_excess_scalar(&r_in, 1, &r_out, 1, excess);

  Crypto::EllipticCurvePoint C_in, C_out;
  make_commitment(amount_in, r_in, C_in);
  make_commitment(amount_out, r_out, C_out);

  Crypto::Hash tx_hash;
  random_hash(tx_hash);

  Crypto::TransactionKernel kernel;
  Crypto::sign_transaction_kernel(excess, tx_hash, kernel);

  // Tamper with signature
  kernel.signature.c.data[0] ^= 0x01;

  bool verify_ok = Crypto::verify_transaction_balance(
    &C_in, 1, &C_out, 1, fee, tx_hash, kernel);
  if (verify_ok) FAIL("should reject tampered signature");

  PASS();
}

// ── Test 6: Wrong tx_hash breaks signature ───────────────────────────

static void test_wrong_tx_hash() {
  TEST("Wrong tx_hash: verification fails");

  uint64_t amount_in = 100;
  uint64_t amount_out = 90;
  uint64_t fee = 10;

  Crypto::EllipticCurveScalar r_in, r_out, excess;
  test_random_scalar(r_in);
  test_random_scalar(r_out);
  Crypto::compute_excess_scalar(&r_in, 1, &r_out, 1, excess);

  Crypto::EllipticCurvePoint C_in, C_out;
  make_commitment(amount_in, r_in, C_in);
  make_commitment(amount_out, r_out, C_out);

  Crypto::Hash tx_hash, wrong_hash;
  random_hash(tx_hash);
  random_hash(wrong_hash);

  Crypto::TransactionKernel kernel;
  Crypto::sign_transaction_kernel(excess, tx_hash, kernel);

  // Verify with wrong hash
  bool verify_ok = Crypto::verify_transaction_balance(
    &C_in, 1, &C_out, 1, fee, wrong_hash, kernel);
  if (verify_ok) FAIL("should reject wrong tx_hash");

  PASS();
}

// ── Test 7: Amounts don't balance → rejection ────────────────────────

static void test_unbalanced_amounts() {
  TEST("Unbalanced amounts: verification fails");

  // Input is 100, output is 80, fee is 10 → missing 10
  uint64_t amount_in = 100;
  uint64_t amount_out = 80;
  uint64_t fee = 10;

  Crypto::EllipticCurveScalar r_in, r_out, excess;
  test_random_scalar(r_in);
  test_random_scalar(r_out);
  Crypto::compute_excess_scalar(&r_in, 1, &r_out, 1, excess);

  Crypto::EllipticCurvePoint C_in, C_out;
  make_commitment(amount_in, r_in, C_in);
  make_commitment(amount_out, r_out, C_out);

  Crypto::Hash tx_hash;
  random_hash(tx_hash);

  Crypto::TransactionKernel kernel;
  Crypto::sign_transaction_kernel(excess, tx_hash, kernel);

  // Amounts: 100 != 80 + 10, so balance check should fail.
  // The excess commitment from the equation will differ from the kernel's.
  bool verify_ok = Crypto::verify_transaction_balance(
    &C_in, 1, &C_out, 1, fee, tx_hash, kernel);
  if (verify_ok) FAIL("should reject unbalanced amounts");

  PASS();
}

// ── Test 8: Transparent amount to commitment ─────────────────────────

static void test_transparent_commitment() {
  TEST("Transparent amount commitment: amount*H");

  uint64_t amount = 42;

  Crypto::EllipticCurvePoint C_transparent;
  bool ok = Crypto::transparent_amount_to_commitment(amount, C_transparent);
  if (!ok) FAIL("transparent_amount_to_commitment failed");

  // Verify by computing via pedersen_commit with zero blinding
  Crypto::EllipticCurveScalar zero_blind;
  memset(&zero_blind, 0, 32);
  Crypto::EllipticCurveScalar v_scalar;
  uint64_to_scalar(amount, v_scalar);

  Crypto::EllipticCurvePoint C_pedersen;
  ok = Crypto::pedersen_commit(v_scalar, zero_blind, C_pedersen);
  if (!ok) FAIL("pedersen_commit with zero blind failed");

  if (memcmp(&C_transparent, &C_pedersen, 32) != 0)
    FAIL("transparent commitment != pedersen_commit(v, 0)");

  PASS();
}

// ── Test 9: Mixed transparent + CT balance ───────────────────────────

static void test_mixed_transparent_ct() {
  TEST("Mixed balance: transparent input -> CT output + fee");

  // Transparent input: amount = 100, implicit blinding = 0
  // CT output: amount = 90, random blinding
  // Fee: 10
  uint64_t amount_in = 100;
  uint64_t amount_out = 90;
  uint64_t fee = 10;

  // Transparent input: commitment = amount*H (zero blinding)
  Crypto::EllipticCurvePoint C_in;
  bool ok = Crypto::transparent_amount_to_commitment(amount_in, C_in);
  if (!ok) FAIL("transparent_amount_to_commitment failed");

  // CT output: commitment = amount_out*H + r_out*G
  Crypto::EllipticCurveScalar r_out;
  test_random_scalar(r_out);
  Crypto::EllipticCurvePoint C_out;
  make_commitment(amount_out, r_out, C_out);

  // Excess: input blinding (0) - output blinding (r_out) = -r_out
  Crypto::EllipticCurveScalar zero_blind;
  memset(&zero_blind, 0, 32);
  Crypto::EllipticCurveScalar excess;
  Crypto::compute_excess_scalar(&zero_blind, 1, &r_out, 1, excess);

  Crypto::Hash tx_hash;
  random_hash(tx_hash);

  Crypto::TransactionKernel kernel;
  bool sign_ok = Crypto::sign_transaction_kernel(excess, tx_hash, kernel);
  if (!sign_ok) FAIL("sign_transaction_kernel failed");

  bool verify_ok = Crypto::verify_transaction_balance(
    &C_in, 1, &C_out, 1, fee, tx_hash, kernel);
  if (!verify_ok) FAIL("verify_transaction_balance returned false");

  PASS();
}

// ── Test 10: Multiple transparent inputs + CT outputs ────────────────

static void test_mixed_multi() {
  TEST("Mixed multi: 2 transparent inputs -> 2 CT outputs + fee");

  uint64_t amounts_in[2] = {60, 40};  // total = 100
  uint64_t amounts_out[2] = {55, 40}; // total = 95
  uint64_t fee = 5;

  // Transparent inputs
  Crypto::EllipticCurvePoint C_in[2];
  for (int i = 0; i < 2; ++i) {
    bool ok = Crypto::transparent_amount_to_commitment(amounts_in[i], C_in[i]);
    if (!ok) FAIL("transparent_amount_to_commitment failed");
  }

  // CT outputs
  Crypto::EllipticCurveScalar r_out[2];
  Crypto::EllipticCurvePoint C_out[2];
  for (int i = 0; i < 2; ++i) {
    test_random_scalar(r_out[i]);
    make_commitment(amounts_out[i], r_out[i], C_out[i]);
  }

  // Excess = sum(0, 0) - sum(r_out[0], r_out[1]) = -(r_out[0] + r_out[1])
  Crypto::EllipticCurveScalar zero_blinds[2];
  memset(&zero_blinds[0], 0, 32);
  memset(&zero_blinds[1], 0, 32);
  Crypto::EllipticCurveScalar excess;
  Crypto::compute_excess_scalar(zero_blinds, 2, r_out, 2, excess);

  Crypto::Hash tx_hash;
  random_hash(tx_hash);

  Crypto::TransactionKernel kernel;
  bool sign_ok = Crypto::sign_transaction_kernel(excess, tx_hash, kernel);
  if (!sign_ok) FAIL("sign_transaction_kernel failed");

  bool verify_ok = Crypto::verify_transaction_balance(
    C_in, 2, C_out, 2, fee, tx_hash, kernel);
  if (!verify_ok) FAIL("verify_transaction_balance returned false");

  PASS();
}

// ── Test 11: compute_excess_commitment directly ──────────────────────

static void test_compute_excess_commitment() {
  TEST("compute_excess_commitment: matches excess*G");

  uint64_t amount_in = 100;
  uint64_t amount_out = 90;
  uint64_t fee = 10;

  Crypto::EllipticCurveScalar r_in, r_out, excess;
  test_random_scalar(r_in);
  test_random_scalar(r_out);
  Crypto::compute_excess_scalar(&r_in, 1, &r_out, 1, excess);

  Crypto::EllipticCurvePoint C_in, C_out;
  make_commitment(amount_in, r_in, C_in);
  make_commitment(amount_out, r_out, C_out);

  // Compute via balance equation
  Crypto::EllipticCurvePoint computed;
  bool ok = Crypto::compute_excess_commitment(
    &C_in, 1, &C_out, 1, fee, computed);
  if (!ok) FAIL("compute_excess_commitment failed");

  // Compute expected: excess*G
  ge_p3 expected_p3;
  ge_scalarmult_base(&expected_p3,
    reinterpret_cast<const unsigned char*>(&excess));
  unsigned char expected_bytes[32];
  ge_p3_tobytes(expected_bytes, &expected_p3);

  if (memcmp(&computed, expected_bytes, 32) != 0)
    FAIL("computed excess != excess*G");

  PASS();
}

// ── Test 12: Large amounts ───────────────────────────────────────────

static void test_large_amounts() {
  TEST("Large amounts: near uint64 max");

  uint64_t amount_in = UINT64_C(1000000000000);  // 1 trillion
  uint64_t amount_out = UINT64_C(999999999990);
  uint64_t fee = 10;

  Crypto::EllipticCurveScalar r_in, r_out, excess;
  test_random_scalar(r_in);
  test_random_scalar(r_out);
  Crypto::compute_excess_scalar(&r_in, 1, &r_out, 1, excess);

  Crypto::EllipticCurvePoint C_in, C_out;
  make_commitment(amount_in, r_in, C_in);
  make_commitment(amount_out, r_out, C_out);

  Crypto::Hash tx_hash;
  random_hash(tx_hash);

  Crypto::TransactionKernel kernel;
  bool sign_ok = Crypto::sign_transaction_kernel(excess, tx_hash, kernel);
  if (!sign_ok) FAIL("sign_transaction_kernel failed");

  bool verify_ok = Crypto::verify_transaction_balance(
    &C_in, 1, &C_out, 1, fee, tx_hash, kernel);
  if (!verify_ok) FAIL("verify_transaction_balance returned false");

  PASS();
}

// ── Test 13: Transparent zero-amount commitment is rejected ──────────

static void test_transparent_zero() {
  TEST("Transparent zero-amount: rejected at the source");

  Crypto::EllipticCurvePoint C;
  // amount=0 would yield 0*H = identity, which fails point_valid_for_pedersen
  // downstream and is never a legitimate transparent ring member (consensus
  // requires KeyOutput.amount != 0). transparent_amount_to_commitment must
  // refuse it directly so callers get a single, clear failure point.
  bool ok = Crypto::transparent_amount_to_commitment(0, C);
  if (ok) FAIL("transparent_amount_to_commitment(0) should fail");

  PASS();
}

// ── Test 14: Kernel signature binds to specific tx_hash ──────────────

static void test_kernel_sig_binding() {
  TEST("Kernel sig binding: different tx_hash → different signature");

  Crypto::EllipticCurveScalar excess;
  test_random_scalar(excess);

  Crypto::Hash hash1, hash2;
  random_hash(hash1);
  random_hash(hash2);

  Crypto::TransactionKernel kernel1, kernel2;
  Crypto::sign_transaction_kernel(excess, hash1, kernel1);
  Crypto::sign_transaction_kernel(excess, hash2, kernel2);

  // Same excess → same excess commitment
  if (memcmp(&kernel1.excess, &kernel2.excess, 32) != 0)
    FAIL("excess commitments should match");

  // Different tx_hash → signatures should differ
  if (memcmp(&kernel1.signature, &kernel2.signature, sizeof(Crypto::Signature)) == 0)
    FAIL("signatures should differ for different hashes");

  PASS();
}

static void test_subgroup_identity_rejected() {
  TEST("Subgroup: identity point rejected");

  Crypto::EllipticCurvePoint identity;
  memset(&identity, 0, 32);
  identity.data[0] = 0x01;

  if (Crypto::point_valid_for_pedersen(identity))
    FAIL("identity should be rejected");

  PASS();
}

static void test_subgroup_low_order_rejected() {
  TEST("Subgroup: pure low-order point rejected");

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

static void test_subgroup_torsion_coset_rejected() {
  TEST("Subgroup: basepoint plus torsion rejected");

  Crypto::EllipticCurvePoint p;
  if (!make_basepoint_plus_order2(p))
    FAIL("torsion-coset construction failed");

  if (Crypto::point_valid_for_pedersen(p))
    FAIL("prime-order point plus torsion should be rejected");

  PASS();
}

static void test_subgroup_valid_commitment_accepted() {
  TEST("Subgroup: valid Pedersen commitment accepted");

  Crypto::EllipticCurveScalar r, v;
  test_random_scalar(r);
  uint64_to_scalar(42, v);

  Crypto::EllipticCurvePoint C;
  if (!Crypto::pedersen_commit(v, r, C))
    FAIL("pedersen_commit failed");

  if (!Crypto::point_valid_for_pedersen(C))
    FAIL("valid commitment should be accepted");

  PASS();
}

// ── Main ─────────────────────────────────────────────────────────────

int main() {
  printf("Transaction Balance Equation & Kernel Signature Tests\n");
  printf("=====================================================\n\n");

  printf("[CT balance equation]\n");
  test_basic_balance();
  test_multi_io();
  test_zero_fee();
  test_large_amounts();
  test_compute_excess_commitment();

  printf("\n[Rejection / negative tests]\n");
  test_wrong_fee();
  test_tampered_signature();
  test_wrong_tx_hash();
  test_unbalanced_amounts();

  printf("\n[Transparent / mixed balance]\n");
  test_transparent_commitment();
  test_transparent_zero();
  test_mixed_transparent_ct();
  test_mixed_multi();

  printf("\n[Kernel signature properties]\n");
  test_kernel_sig_binding();

  printf("\n[Subgroup validation]\n");
  test_subgroup_identity_rejected();
  test_subgroup_low_order_rejected();
  test_subgroup_torsion_coset_rejected();
  test_subgroup_valid_commitment_accepted();

  printf("\n=====================================================\n");
  printf("Results: %d/%d passed\n", tests_passed, tests_run);

  return (tests_passed == tests_run) ? 0 : 1;
}
