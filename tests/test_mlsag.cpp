// Standalone test for MLSAG ring signatures.
// Build: cl /EHsc /I../include /I../src test_mlsag.cpp /link ../build/src/Release/Crypto.lib

#include "crypto/mlsag.h"
#include "crypto/pedersen.h"
#include "crypto/random.h"

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

// Generate a keypair: pub = sec * G
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
    printf("  %-55s", name); \
  } while(0)

#define PASS() do { tests_passed++; printf("PASS\n"); } while(0)
#define FAIL(msg) do { printf("FAIL: %s\n", msg); } while(0)

// Create a ring with random decoys plus one real input at true_index.
// Returns ring pubkeys, ring commitments, pseudo commitment, and secrets.
struct TestRing {
  std::vector<Crypto::PublicKey> pubkeys;
  std::vector<Crypto::EllipticCurvePoint> commits;
  Crypto::EllipticCurvePoint pseudo_commit;
  Crypto::SecretKey spend_key;
  Crypto::EllipticCurveScalar real_blinding;
  Crypto::EllipticCurveScalar pseudo_blinding;
  uint64_t amount;
};

static bool build_test_ring(size_t ring_size, size_t true_index, TestRing& ring) {
  ring.pubkeys.resize(ring_size);
  ring.commits.resize(ring_size);
  ring.amount = 1000000; // arbitrary

  Crypto::EllipticCurveScalar v_scalar;
  uint64_to_scalar(ring.amount, v_scalar);

  // Generate real keypair at true_index
  gen_keypair(ring.pubkeys[true_index], ring.spend_key);

  // Generate real commitment: C_real = v*H + r_real*G
  test_random_scalar(ring.real_blinding);
  if (!Crypto::pedersen_commit(v_scalar, ring.real_blinding, ring.commits[true_index]))
    return false;

  // Generate decoy keypairs and commitments
  for (size_t i = 0; i < ring_size; ++i) {
    if (i == true_index) continue;
    Crypto::SecretKey dummy_sec;
    gen_keypair(ring.pubkeys[i], dummy_sec);

    // Decoy commitments: random valid points (any Pedersen commitment)
    Crypto::EllipticCurveScalar dummy_r, dummy_v;
    test_random_scalar(dummy_r);
    test_random_scalar(dummy_v);
    if (!Crypto::pedersen_commit(dummy_v, dummy_r, ring.commits[i]))
      return false;
  }

  // Generate pseudo-output commitment: C' = v*H + r'*G (same amount!)
  test_random_scalar(ring.pseudo_blinding);
  if (!Crypto::pedersen_commit(v_scalar, ring.pseudo_blinding, ring.pseudo_commit))
    return false;

  return true;
}

static void test_sign_verify(size_t ring_size, size_t true_index) {
  char name[80];
  snprintf(name, sizeof(name), "Sign/verify ring_size=%zu true_index=%zu",
           ring_size, true_index);
  TEST(name);

  TestRing ring;
  if (!build_test_ring(ring_size, true_index, ring)) { FAIL("build_ring"); return; }

  Crypto::Hash msg;
  Random::randomBytes(32, msg.data);

  Crypto::KeyImage key_image;
  Crypto::MLSAGSignature sig;
  if (!Crypto::mlsag_sign(msg,
        ring.pubkeys.data(), ring.commits.data(), ring.pseudo_commit,
        ring_size, true_index,
        ring.spend_key, ring.real_blinding, ring.pseudo_blinding,
        key_image, sig)) {
    FAIL("sign"); return;
  }

  if (!Crypto::mlsag_verify(msg,
        ring.pubkeys.data(), ring.commits.data(), ring.pseudo_commit,
        ring_size, key_image, sig)) {
    FAIL("verify"); return;
  }

  PASS();
}

static void test_wrong_message() {
  TEST("Wrong message fails verification");

  TestRing ring;
  if (!build_test_ring(4, 1, ring)) { FAIL("build_ring"); return; }

  Crypto::Hash msg, wrong_msg;
  Random::randomBytes(32, msg.data);
  Random::randomBytes(32, wrong_msg.data);

  Crypto::KeyImage key_image;
  Crypto::MLSAGSignature sig;
  if (!Crypto::mlsag_sign(msg,
        ring.pubkeys.data(), ring.commits.data(), ring.pseudo_commit,
        4, 1, ring.spend_key, ring.real_blinding, ring.pseudo_blinding,
        key_image, sig)) {
    FAIL("sign"); return;
  }

  if (Crypto::mlsag_verify(wrong_msg,
        ring.pubkeys.data(), ring.commits.data(), ring.pseudo_commit,
        4, key_image, sig)) {
    FAIL("should have failed"); return;
  }

  PASS();
}

static void test_tampered_response() {
  TEST("Tampered response scalar fails");

  TestRing ring;
  if (!build_test_ring(4, 2, ring)) { FAIL("build_ring"); return; }

  Crypto::Hash msg;
  Random::randomBytes(32, msg.data);

  Crypto::KeyImage key_image;
  Crypto::MLSAGSignature sig;
  if (!Crypto::mlsag_sign(msg,
        ring.pubkeys.data(), ring.commits.data(), ring.pseudo_commit,
        4, 2, ring.spend_key, ring.real_blinding, ring.pseudo_blinding,
        key_image, sig)) {
    FAIL("sign"); return;
  }

  // Tamper with a response scalar
  sig.ss[0][0].data[0] ^= 0x01;
  if (Crypto::mlsag_verify(msg,
        ring.pubkeys.data(), ring.commits.data(), ring.pseudo_commit,
        4, key_image, sig)) {
    FAIL("should have failed"); return;
  }

  PASS();
}

static void test_wrong_pseudo_commit() {
  TEST("Wrong pseudo commitment fails");

  TestRing ring;
  if (!build_test_ring(4, 0, ring)) { FAIL("build_ring"); return; }

  Crypto::Hash msg;
  Random::randomBytes(32, msg.data);

  Crypto::KeyImage key_image;
  Crypto::MLSAGSignature sig;
  if (!Crypto::mlsag_sign(msg,
        ring.pubkeys.data(), ring.commits.data(), ring.pseudo_commit,
        4, 0, ring.spend_key, ring.real_blinding, ring.pseudo_blinding,
        key_image, sig)) {
    FAIL("sign"); return;
  }

  // Create a different pseudo commitment
  Crypto::EllipticCurveScalar wrong_r;
  test_random_scalar(wrong_r);
  Crypto::EllipticCurveScalar v_scalar;
  uint64_to_scalar(ring.amount, v_scalar);
  Crypto::EllipticCurvePoint wrong_pseudo;
  Crypto::pedersen_commit(v_scalar, wrong_r, wrong_pseudo);

  if (Crypto::mlsag_verify(msg,
        ring.pubkeys.data(), ring.commits.data(), wrong_pseudo,
        4, key_image, sig)) {
    FAIL("should have failed"); return;
  }

  PASS();
}

static void test_wrong_key_image() {
  TEST("Wrong key image fails");

  TestRing ring;
  if (!build_test_ring(4, 1, ring)) { FAIL("build_ring"); return; }

  Crypto::Hash msg;
  Random::randomBytes(32, msg.data);

  Crypto::KeyImage key_image;
  Crypto::MLSAGSignature sig;
  if (!Crypto::mlsag_sign(msg,
        ring.pubkeys.data(), ring.commits.data(), ring.pseudo_commit,
        4, 1, ring.spend_key, ring.real_blinding, ring.pseudo_blinding,
        key_image, sig)) {
    FAIL("sign"); return;
  }

  // Tamper with key image
  key_image.data[0] ^= 0x01;
  if (Crypto::mlsag_verify(msg,
        ring.pubkeys.data(), ring.commits.data(), ring.pseudo_commit,
        4, key_image, sig)) {
    FAIL("should have failed"); return;
  }

  PASS();
}

static void test_identity_key_image_rejected() {
  TEST("Identity key image rejected");

  TestRing ring;
  if (!build_test_ring(4, 1, ring)) { FAIL("build_ring"); return; }

  Crypto::Hash msg;
  Random::randomBytes(32, msg.data);

  Crypto::KeyImage key_image;
  Crypto::MLSAGSignature sig;
  if (!Crypto::mlsag_sign(msg,
        ring.pubkeys.data(), ring.commits.data(), ring.pseudo_commit,
        4, 1, ring.spend_key, ring.real_blinding, ring.pseudo_blinding,
        key_image, sig)) {
    FAIL("sign"); return;
  }

  memset(&key_image, 0, sizeof(key_image));
  key_image.data[0] = 0x01;

  if (Crypto::mlsag_verify(msg,
        ring.pubkeys.data(), ring.commits.data(), ring.pseudo_commit,
        4, key_image, sig)) {
    FAIL("should have failed"); return;
  }

  PASS();
}

static void test_ring_size_1() {
  TEST("Ring size 1 (trivial ring)");

  TestRing ring;
  if (!build_test_ring(1, 0, ring)) { FAIL("build_ring"); return; }

  Crypto::Hash msg;
  Random::randomBytes(32, msg.data);

  Crypto::KeyImage key_image;
  Crypto::MLSAGSignature sig;
  if (!Crypto::mlsag_sign(msg,
        ring.pubkeys.data(), ring.commits.data(), ring.pseudo_commit,
        1, 0, ring.spend_key, ring.real_blinding, ring.pseudo_blinding,
        key_image, sig)) {
    FAIL("sign"); return;
  }

  if (!Crypto::mlsag_verify(msg,
        ring.pubkeys.data(), ring.commits.data(), ring.pseudo_commit,
        1, key_image, sig)) {
    FAIL("verify"); return;
  }

  PASS();
}

static void test_key_image_consistency() {
  TEST("Key image consistent across rings");

  // Same spend key should produce same key image regardless of ring
  Crypto::SecretKey spend_key;
  Crypto::PublicKey pub;
  gen_keypair(pub, spend_key);

  Crypto::EllipticCurveScalar r1, r2, v_scalar;
  test_random_scalar(r1);
  test_random_scalar(r2);
  uint64_to_scalar(500, v_scalar);

  Crypto::EllipticCurvePoint C1, C2, pseudo1, pseudo2;
  Crypto::pedersen_commit(v_scalar, r1, C1);
  Crypto::pedersen_commit(v_scalar, r2, C2);

  // Ring 1: just the real key
  Crypto::EllipticCurveScalar pr1, pr2;
  test_random_scalar(pr1);
  test_random_scalar(pr2);
  Crypto::pedersen_commit(v_scalar, pr1, pseudo1);
  Crypto::pedersen_commit(v_scalar, pr2, pseudo2);

  Crypto::Hash msg;
  Random::randomBytes(32, msg.data);

  Crypto::KeyImage ki1, ki2;
  Crypto::MLSAGSignature sig1, sig2;

  Crypto::mlsag_sign(msg, &pub, &C1, pseudo1, 1, 0,
                     spend_key, r1, pr1, ki1, sig1);
  Crypto::mlsag_sign(msg, &pub, &C2, pseudo2, 1, 0,
                     spend_key, r2, pr2, ki2, sig2);

  if (memcmp(&ki1, &ki2, sizeof(Crypto::KeyImage)) != 0) {
    FAIL("key images differ"); return;
  }

  PASS();
}

int main() {
  printf("MLSAG Ring Signature Tests\n");
  printf("==========================\n\n");

  // Basic sign/verify with various ring sizes and true indices
  test_sign_verify(2, 0);
  test_sign_verify(2, 1);
  test_sign_verify(4, 0);
  test_sign_verify(4, 1);
  test_sign_verify(4, 2);
  test_sign_verify(4, 3);
  test_sign_verify(8, 0);
  test_sign_verify(8, 4);
  test_sign_verify(8, 7);
  test_sign_verify(16, 0);
  test_sign_verify(16, 8);
  test_sign_verify(16, 15);

  printf("\nEdge cases:\n");
  test_ring_size_1();

  printf("\nNegative tests:\n");
  test_wrong_message();
  test_tampered_response();
  test_wrong_pseudo_commit();
  test_wrong_key_image();
  test_identity_key_image_rejected();

  printf("\nKey image tests:\n");
  test_key_image_consistency();

  printf("\n%d/%d tests passed.\n", tests_passed, tests_run);
  return (tests_passed == tests_run) ? 0 : 1;
}
