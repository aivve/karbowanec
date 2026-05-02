// Standalone test for GK one-of-many proof.
// Build: cl /EHsc /I../include /I../src test_gk_proof.cpp /link ../build/src/Release/Crypto.lib

#include "crypto/gk_proof.h"
#include "crypto/pedersen.h"
#include "crypto/random.h"
#include "Denominations.h"

#include <cstdio>
#include <cstring>

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

static int tests_run = 0;
static int tests_passed = 0;

#define TEST(name) \
  do { \
    tests_run++; \
    printf("  %-50s", name); \
  } while(0)

#define PASS() do { tests_passed++; printf("PASS\n"); } while(0)
#define FAIL(msg) do { printf("FAIL: %s\n", msg); } while(0)

static void test_roundtrip(size_t idx) {
  char name[80];
  snprintf(name, sizeof(name), "Round-trip idx=%zu (v=%llu)", idx,
           (unsigned long long)CryptoNote::DENOMINATIONS[idx]);
  TEST(name);

  Crypto::EllipticCurveScalar r;
  test_random_scalar(r);

  uint64_t v = CryptoNote::DENOMINATIONS[idx];
  Crypto::EllipticCurveScalar v_scalar;
  uint64_to_scalar(v, v_scalar);

  Crypto::EllipticCurvePoint C;
  if (!Crypto::pedersen_commit(v_scalar, r, C)) { FAIL("commit"); return; }

  Crypto::Hash tx_hash;
  Random::randomBytes(32, tx_hash.data);

  Crypto::GKProof proof;
  if (!Crypto::gk_prove(C, v, r, idx, tx_hash, proof)) { FAIL("prove"); return; }
  if (!Crypto::gk_verify(C, proof, tx_hash)) { FAIL("verify"); return; }

  PASS();
}

static void test_wrong_txhash() {
  TEST("Wrong tx_hash fails verification");

  Crypto::EllipticCurveScalar r;
  test_random_scalar(r);

  uint64_t v = CryptoNote::DENOMINATIONS[10];
  Crypto::EllipticCurveScalar v_scalar;
  uint64_to_scalar(v, v_scalar);

  Crypto::EllipticCurvePoint C;
  if (!Crypto::pedersen_commit(v_scalar, r, C)) { FAIL("commit"); return; }

  Crypto::Hash tx_hash, wrong_hash;
  Random::randomBytes(32, tx_hash.data);
  Random::randomBytes(32, wrong_hash.data);

  Crypto::GKProof proof;
  if (!Crypto::gk_prove(C, v, r, 10, tx_hash, proof)) { FAIL("prove"); return; }
  if (Crypto::gk_verify(C, proof, wrong_hash)) { FAIL("should have failed"); return; }

  PASS();
}

static void test_tampered_f() {
  TEST("Tampered f scalar fails");

  Crypto::EllipticCurveScalar r;
  test_random_scalar(r);

  uint64_t v = CryptoNote::DENOMINATIONS[5];
  Crypto::EllipticCurveScalar v_scalar;
  uint64_to_scalar(v, v_scalar);

  Crypto::EllipticCurvePoint C;
  if (!Crypto::pedersen_commit(v_scalar, r, C)) { FAIL("commit"); return; }

  Crypto::Hash tx_hash;
  Random::randomBytes(32, tx_hash.data);

  Crypto::GKProof proof;
  if (!Crypto::gk_prove(C, v, r, 5, tx_hash, proof)) { FAIL("prove"); return; }

  proof.f.data[0] ^= 0x01;
  if (Crypto::gk_verify(C, proof, tx_hash)) { FAIL("should have failed"); return; }

  PASS();
}

static void test_tampered_a_point() {
  TEST("Tampered A point fails");

  Crypto::EllipticCurveScalar r;
  test_random_scalar(r);

  uint64_t v = CryptoNote::DENOMINATIONS[4];
  Crypto::EllipticCurveScalar v_scalar;
  uint64_to_scalar(v, v_scalar);

  Crypto::EllipticCurvePoint C;
  if (!Crypto::pedersen_commit(v_scalar, r, C)) { FAIL("commit"); return; }

  Crypto::Hash tx_hash;
  Random::randomBytes(32, tx_hash.data);

  Crypto::GKProof proof;
  if (!Crypto::gk_prove(C, v, r, 4, tx_hash, proof)) { FAIL("prove"); return; }

  unsigned char identity[32] = {0};
  identity[0] = 1;
  if (ge_frombytes_vartime(&proof.A[0], identity) != 0) { FAIL("identity decode"); return; }
  if (Crypto::gk_verify(C, proof, tx_hash)) { FAIL("should have failed"); return; }

  PASS();
}

static void test_tampered_i_point() {
  TEST("Tampered I point fails");

  Crypto::EllipticCurveScalar r;
  test_random_scalar(r);

  uint64_t v = CryptoNote::DENOMINATIONS[8];
  Crypto::EllipticCurveScalar v_scalar;
  uint64_to_scalar(v, v_scalar);

  Crypto::EllipticCurvePoint C;
  if (!Crypto::pedersen_commit(v_scalar, r, C)) { FAIL("commit"); return; }

  Crypto::Hash tx_hash;
  Random::randomBytes(32, tx_hash.data);

  Crypto::GKProof proof;
  if (!Crypto::gk_prove(C, v, r, 8, tx_hash, proof)) { FAIL("prove"); return; }

  unsigned char identity[32] = {0};
  identity[0] = 1;
  if (ge_frombytes_vartime(&proof.I[0], identity) != 0) { FAIL("identity decode"); return; }
  if (Crypto::gk_verify(C, proof, tx_hash)) { FAIL("should have failed"); return; }

  PASS();
}

static void test_tampered_b_point() {
  TEST("Tampered B point fails");

  Crypto::EllipticCurveScalar r;
  test_random_scalar(r);

  uint64_t v = CryptoNote::DENOMINATIONS[9];
  Crypto::EllipticCurveScalar v_scalar;
  uint64_to_scalar(v, v_scalar);

  Crypto::EllipticCurvePoint C;
  if (!Crypto::pedersen_commit(v_scalar, r, C)) { FAIL("commit"); return; }

  Crypto::Hash tx_hash;
  Random::randomBytes(32, tx_hash.data);

  Crypto::GKProof proof;
  if (!Crypto::gk_prove(C, v, r, 9, tx_hash, proof)) { FAIL("prove"); return; }

  unsigned char identity[32] = {0};
  identity[0] = 1;
  if (ge_frombytes_vartime(&proof.B[0], identity) != 0) { FAIL("identity decode"); return; }
  if (Crypto::gk_verify(C, proof, tx_hash)) { FAIL("should have failed"); return; }

  PASS();
}

static void test_tampered_za_scalar() {
  TEST("Tampered za scalar fails");

  Crypto::EllipticCurveScalar r;
  test_random_scalar(r);

  uint64_t v = CryptoNote::DENOMINATIONS[13];
  Crypto::EllipticCurveScalar v_scalar;
  uint64_to_scalar(v, v_scalar);

  Crypto::EllipticCurvePoint C;
  if (!Crypto::pedersen_commit(v_scalar, r, C)) { FAIL("commit"); return; }

  Crypto::Hash tx_hash;
  Random::randomBytes(32, tx_hash.data);

  Crypto::GKProof proof;
  if (!Crypto::gk_prove(C, v, r, 13, tx_hash, proof)) { FAIL("prove"); return; }

  proof.za[0].data[0] ^= 0x01;
  if (Crypto::gk_verify(C, proof, tx_hash)) { FAIL("should have failed"); return; }

  PASS();
}

static void test_tampered_zb_scalar() {
  TEST("Tampered zb scalar fails");

  Crypto::EllipticCurveScalar r;
  test_random_scalar(r);

  uint64_t v = CryptoNote::DENOMINATIONS[14];
  Crypto::EllipticCurveScalar v_scalar;
  uint64_to_scalar(v, v_scalar);

  Crypto::EllipticCurvePoint C;
  if (!Crypto::pedersen_commit(v_scalar, r, C)) { FAIL("commit"); return; }

  Crypto::Hash tx_hash;
  Random::randomBytes(32, tx_hash.data);

  Crypto::GKProof proof;
  if (!Crypto::gk_prove(C, v, r, 14, tx_hash, proof)) { FAIL("prove"); return; }

  proof.zb[0].data[0] ^= 0x01;
  if (Crypto::gk_verify(C, proof, tx_hash)) { FAIL("should have failed"); return; }

  PASS();
}

static void test_tampered_q_point() {
  TEST("Tampered Q point fails");

  Crypto::EllipticCurveScalar r;
  test_random_scalar(r);

  uint64_t v = CryptoNote::DENOMINATIONS[7];
  Crypto::EllipticCurveScalar v_scalar;
  uint64_to_scalar(v, v_scalar);

  Crypto::EllipticCurvePoint C;
  if (!Crypto::pedersen_commit(v_scalar, r, C)) { FAIL("commit"); return; }

  Crypto::Hash tx_hash;
  Random::randomBytes(32, tx_hash.data);

  Crypto::GKProof proof;
  if (!Crypto::gk_prove(C, v, r, 7, tx_hash, proof)) { FAIL("prove"); return; }

  unsigned char identity[32] = {0};
  identity[0] = 1;
  if (ge_frombytes_vartime(&proof.Q[0], identity) != 0) { FAIL("identity decode"); return; }
  if (Crypto::gk_verify(C, proof, tx_hash)) { FAIL("should have failed"); return; }

  PASS();
}

static void test_noncanonical_scalar_rejected() {
  TEST("Non-canonical z scalar rejected");

  Crypto::EllipticCurveScalar r;
  test_random_scalar(r);

  uint64_t v = CryptoNote::DENOMINATIONS[12];
  Crypto::EllipticCurveScalar v_scalar;
  uint64_to_scalar(v, v_scalar);

  Crypto::EllipticCurvePoint C;
  if (!Crypto::pedersen_commit(v_scalar, r, C)) { FAIL("commit"); return; }

  Crypto::Hash tx_hash;
  Random::randomBytes(32, tx_hash.data);

  Crypto::GKProof proof;
  if (!Crypto::gk_prove(C, v, r, 12, tx_hash, proof)) { FAIL("prove"); return; }

  memset(proof.z[0].data, 0xFF, 32);
  if (Crypto::gk_verify(C, proof, tx_hash)) { FAIL("should have failed"); return; }

  PASS();
}

static void test_wrong_index() {
  TEST("Wrong denomination index rejected");

  Crypto::EllipticCurveScalar r;
  test_random_scalar(r);

  uint64_t v = CryptoNote::DENOMINATIONS[5];
  Crypto::EllipticCurveScalar v_scalar;
  uint64_to_scalar(v, v_scalar);

  Crypto::EllipticCurvePoint C;
  if (!Crypto::pedersen_commit(v_scalar, r, C)) { FAIL("commit"); return; }

  Crypto::Hash tx_hash;
  Random::randomBytes(32, tx_hash.data);

  Crypto::GKProof proof;
  if (Crypto::gk_prove(C, v, r, 6, tx_hash, proof)) { FAIL("should have rejected"); return; }

  PASS();
}

static void test_out_of_range() {
  TEST("Out-of-range index rejected");

  Crypto::EllipticCurveScalar r;
  test_random_scalar(r);

  Crypto::EllipticCurveScalar v_scalar;
  uint64_to_scalar(1, v_scalar);

  Crypto::EllipticCurvePoint C;
  if (!Crypto::pedersen_commit(v_scalar, r, C)) { FAIL("commit"); return; }

  Crypto::Hash tx_hash;
  Random::randomBytes(32, tx_hash.data);

  Crypto::GKProof proof;
  if (Crypto::gk_prove(C, 1, r, 64, tx_hash, proof)) { FAIL("should have rejected"); return; }

  PASS();
}

static void test_identity_commitment_rejected() {
  TEST("Identity commitment rejected in prove");

  Crypto::EllipticCurvePoint C;
  memset(&C, 0, sizeof(C));
  C.data[0] = 0x01;

  Crypto::EllipticCurveScalar r;
  test_random_scalar(r);

  Crypto::Hash tx_hash;
  Random::randomBytes(32, tx_hash.data);

  Crypto::GKProof proof;
  if (Crypto::gk_prove(C, CryptoNote::DENOMINATIONS[0], r, 0, tx_hash, proof)) {
    FAIL("should have rejected"); return;
  }

  PASS();
}

static void test_proof_layout_constants() {
  TEST("Proof layout constants (24 points + 19 scalars = 1376 bytes)");

  const size_t pointSize = sizeof(Crypto::EllipticCurvePoint);
  const size_t scalarSize = sizeof(Crypto::EllipticCurveScalar);

  if (pointSize != 32 || scalarSize != 32) {
    FAIL("unexpected point/scalar size");
    return;
  }

  const size_t expectedWireBytes = (24 * pointSize) + (19 * scalarSize);
  if (expectedWireBytes != 1376) {
    FAIL("unexpected GK wire proof size");
    return;
  }

  if (Crypto::GK_n != 6 || Crypto::GK_N != 64) {
    FAIL("unexpected GK dimensions");
    return;
  }

  PASS();
}

int main() {
  printf("GK One-of-Many Proof Tests\n");
  printf("==========================\n");

  // Test a few specific indices
  test_roundtrip(0);
  test_roundtrip(1);
  test_roundtrip(31);
  test_roundtrip(32);
  test_roundtrip(63);

  // Negative tests
  test_wrong_txhash();
  test_tampered_f();
  test_tampered_i_point();
  test_tampered_a_point();
  test_tampered_b_point();
  test_tampered_q_point();
  test_tampered_za_scalar();
  test_tampered_zb_scalar();
  test_noncanonical_scalar_rejected();
  test_wrong_index();
  test_out_of_range();
  test_identity_commitment_rejected();
  test_proof_layout_constants();

  // Test all 64 denominations
  printf("\nAll-denominations sweep:\n");
  for (size_t i = 0; i < 64; ++i) {
    test_roundtrip(i);
  }

  printf("\n%d/%d tests passed.\n", tests_passed, tests_run);
  return (tests_passed == tests_run) ? 0 : 1;
}
