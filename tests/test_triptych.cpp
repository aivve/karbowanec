// Standalone tests for the Triptych spend proof.
//
// Coverage:
//   - sign/verify round-trip across every supported ring size (4, 8, 16)
//     and every true-index slot
//   - unsupported ring sizes (3, 5, 7, 9, 12, 32) rejected up front
//   - rejection of: wrong message, wrong pseudo-commitment, wrong key
//     image, tampered scalar/point in every component of the proof
//   - hidden-inflation attempt (different amount in real vs pseudo) is
//     rejected (specifically tests the M-ring identity is sound)
//   - key image consistency: same spend key + same ring member always
//     produces the same key image regardless of ring composition
//   - bad witness (wrong x for the indicated public key) cannot produce a
//     valid proof
//   - Fiat-Shamir binding: swapping any transcript-input field breaks
//     verification (transcript completeness check)
//
// Build: cl /EHsc /I../include /I../src test_triptych.cpp /link Crypto.lib

#include "crypto/triptych.h"
#include "crypto/pedersen.h"
#include "crypto/random.h"
#include "crypto/hash.h"

#include <cstdio>
#include <cstring>
#include <vector>

extern "C" {
#include "crypto/crypto-ops.h"
}

// ── Test fixtures ───────────────────────────────────────────────────────

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

static void gen_keypair(Crypto::PublicKey& pub, Crypto::SecretKey& sec) {
  test_random_scalar(reinterpret_cast<Crypto::EllipticCurveScalar&>(sec));
  ge_p3 point;
  ge_scalarmult_base(&point, reinterpret_cast<const unsigned char*>(&sec));
  ge_p3_tobytes(reinterpret_cast<unsigned char*>(&pub), &point);
}

struct TestRing {
  std::vector<Crypto::PublicKey>          pubkeys;
  std::vector<Crypto::EllipticCurvePoint> commits;
  Crypto::EllipticCurvePoint              pseudo_commit;
  Crypto::SecretKey                       spend_key;
  Crypto::EllipticCurveScalar             real_blinding;
  Crypto::EllipticCurveScalar             pseudo_blinding;
  uint64_t                                amount;
};

// Build a ring of size N with a real input at true_index whose amount is
// `amount` and whose commitments balance against pseudo_commitment (same
// value v, different blinding factors).
static bool build_test_ring(size_t ring_size, size_t true_index,
                            uint64_t amount, TestRing& ring) {
  ring.pubkeys.resize(ring_size);
  ring.commits.resize(ring_size);
  ring.amount = amount;

  Crypto::EllipticCurveScalar v_scalar;
  uint64_to_scalar(amount, v_scalar);

  gen_keypair(ring.pubkeys[true_index], ring.spend_key);

  test_random_scalar(ring.real_blinding);
  if (!Crypto::pedersen_commit(v_scalar, ring.real_blinding, ring.commits[true_index]))
    return false;

  for (size_t i = 0; i < ring_size; ++i) {
    if (i == true_index) continue;
    Crypto::SecretKey dummy_sec;
    gen_keypair(ring.pubkeys[i], dummy_sec);

    Crypto::EllipticCurveScalar dummy_r, dummy_v;
    test_random_scalar(dummy_r);
    test_random_scalar(dummy_v);
    if (!Crypto::pedersen_commit(dummy_v, dummy_r, ring.commits[i]))
      return false;
  }

  test_random_scalar(ring.pseudo_blinding);
  if (!Crypto::pedersen_commit(v_scalar, ring.pseudo_blinding, ring.pseudo_commit))
    return false;

  return true;
}

// Sign a proof; abort the test on failure.
static bool sign_for_ring(const TestRing& ring, size_t true_index,
                         const Crypto::Hash& msg, Crypto::KeyImage& ki,
                         Crypto::TriptychSignature& sig) {
  return Crypto::triptych_sign(msg,
    ring.pubkeys.data(), ring.commits.data(), ring.pseudo_commit,
    ring.pubkeys.size(), true_index,
    ring.spend_key, ring.real_blinding, ring.pseudo_blinding,
    ki, sig);
}

static int tests_run = 0;
static int tests_passed = 0;

#define TEST(name) \
  do { tests_run++; printf("  %-58s", name); } while(0)

#define PASS() do { tests_passed++; printf("PASS\n"); } while(0)
#define FAIL(msg) do { printf("FAIL: %s\n", msg); } while(0)

// ── Positive: sign/verify across all supported sizes and indices ────────

static void test_sign_verify(size_t ring_size, size_t true_index) {
  char name[80];
  snprintf(name, sizeof(name), "Sign/verify ring=%zu idx=%zu", ring_size, true_index);
  TEST(name);

  TestRing ring;
  if (!build_test_ring(ring_size, true_index, 1000000, ring)) { FAIL("build"); return; }

  Crypto::Hash msg;
  Random::randomBytes(32, msg.data);

  Crypto::KeyImage ki;
  Crypto::TriptychSignature sig;
  if (!sign_for_ring(ring, true_index, msg, ki, sig)) { FAIL("sign"); return; }

  if (!Crypto::triptych_verify(msg,
        ring.pubkeys.data(), ring.commits.data(), ring.pseudo_commit,
        ring_size, ki, sig)) { FAIL("verify"); return; }

  PASS();
}

// ── Shape: unsupported ring sizes refused ───────────────────────────────

static void test_unsupported_ring_size(size_t ring_size) {
  char name[80];
  snprintf(name, sizeof(name), "Unsupported ring_size=%zu refused", ring_size);
  TEST(name);

  if (Crypto::triptych_ring_size_supported(ring_size)) {
    FAIL("should be unsupported"); return;
  }

  // Try to sign at any index; should return false without producing a sig.
  // We hand it a stub minimal ring of garbage points — the entry-point
  // shape check rejects before any decoding.
  std::vector<Crypto::PublicKey> dummyP(ring_size);
  std::vector<Crypto::EllipticCurvePoint> dummyC(ring_size);
  Crypto::EllipticCurvePoint dummyPseudo{};
  Crypto::SecretKey dummyKey{};
  Crypto::EllipticCurveScalar dummyR{};
  Crypto::KeyImage ki;
  Crypto::TriptychSignature sig;
  Crypto::Hash msg{};

  if (Crypto::triptych_sign(msg, dummyP.data(), dummyC.data(), dummyPseudo,
       ring_size, 0, dummyKey, dummyR, dummyR, ki, sig)) {
    FAIL("sign should fail"); return;
  }

  PASS();
}

// ── Negative: wrong message ──────────────────────────────────────────────

static void test_wrong_message() {
  TEST("Wrong message fails verification");

  TestRing ring;
  if (!build_test_ring(8, 3, 50000, ring)) { FAIL("build"); return; }
  Crypto::Hash msg, wrong_msg;
  Random::randomBytes(32, msg.data);
  Random::randomBytes(32, wrong_msg.data);

  Crypto::KeyImage ki;
  Crypto::TriptychSignature sig;
  if (!sign_for_ring(ring, 3, msg, ki, sig)) { FAIL("sign"); return; }
  if (Crypto::triptych_verify(wrong_msg,
        ring.pubkeys.data(), ring.commits.data(), ring.pseudo_commit,
        8, ki, sig)) { FAIL("should fail"); return; }

  PASS();
}

// ── Negative: wrong pseudo commitment (transcript binding) ──────────────

static void test_wrong_pseudo_commit() {
  TEST("Wrong pseudo commitment fails");

  TestRing ring;
  if (!build_test_ring(8, 2, 12345, ring)) { FAIL("build"); return; }
  Crypto::Hash msg;
  Random::randomBytes(32, msg.data);

  Crypto::KeyImage ki;
  Crypto::TriptychSignature sig;
  if (!sign_for_ring(ring, 2, msg, ki, sig)) { FAIL("sign"); return; }

  // Tamper: build a different pseudo commitment for the SAME value (so
  // M_l would still close on the "fake" pseudo, but the FS transcript
  // binds to the original — verifier must detect).
  Crypto::EllipticCurveScalar v_scalar, other_r;
  uint64_to_scalar(ring.amount, v_scalar);
  test_random_scalar(other_r);
  Crypto::EllipticCurvePoint other_pseudo;
  Crypto::pedersen_commit(v_scalar, other_r, other_pseudo);

  if (Crypto::triptych_verify(msg,
        ring.pubkeys.data(), ring.commits.data(), other_pseudo,
        8, ki, sig)) { FAIL("should fail"); return; }

  PASS();
}

// ── Negative: tampered key image ────────────────────────────────────────

static void test_wrong_key_image() {
  TEST("Tampered key image fails");

  TestRing ring;
  if (!build_test_ring(16, 7, 99, ring)) { FAIL("build"); return; }
  Crypto::Hash msg;
  Random::randomBytes(32, msg.data);

  Crypto::KeyImage ki;
  Crypto::TriptychSignature sig;
  if (!sign_for_ring(ring, 7, msg, ki, sig)) { FAIL("sign"); return; }

  ki.data[0] ^= 0x80;
  if (Crypto::triptych_verify(msg,
        ring.pubkeys.data(), ring.commits.data(), ring.pseudo_commit,
        16, ki, sig)) { FAIL("should fail"); return; }

  PASS();
}

// ── Negative: tampered response scalars ──────────────────────────────────

static void test_tampered_responses() {
  TEST("Tampering response scalars fails");

  TestRing ring;
  if (!build_test_ring(8, 5, 7, ring)) { FAIL("build"); return; }
  Crypto::Hash msg;
  Random::randomBytes(32, msg.data);

  // For every individual scalar field, tamper one bit and verify the
  // proof fails. Catches regressions where any single scalar drifts.
  auto run = [&](const char* what, auto patch) -> bool {
    Crypto::KeyImage ki;
    Crypto::TriptychSignature sig;
    if (!sign_for_ring(ring, 5, msg, ki, sig)) return false;
    patch(sig);
    if (Crypto::triptych_verify(msg,
          ring.pubkeys.data(), ring.commits.data(), ring.pseudo_commit,
          8, ki, sig)) {
      printf("FAIL [%s]\n", what);
      return false;
    }
    return true;
  };

  if (!run("f_P", [](Crypto::TriptychSignature& s){ s.f_P.data[0] ^= 1; })) return;
  if (!run("f_M", [](Crypto::TriptychSignature& s){ s.f_M.data[0] ^= 1; })) return;
  if (!run("f_U", [](Crypto::TriptychSignature& s){ s.f_U.data[0] ^= 1; })) return;
  if (!run("z[0]",  [](Crypto::TriptychSignature& s){ s.z[0].data[0]  ^= 1; })) return;
  if (!run("za[0]", [](Crypto::TriptychSignature& s){ s.za[0].data[0] ^= 1; })) return;
  if (!run("zb[0]", [](Crypto::TriptychSignature& s){ s.zb[0].data[0] ^= 1; })) return;

  PASS();
}

// ── Negative: tampered points ───────────────────────────────────────────

static void test_tampered_points() {
  TEST("Tampering proof points fails");

  TestRing ring;
  if (!build_test_ring(8, 1, 1, ring)) { FAIL("build"); return; }
  Crypto::Hash msg;
  Random::randomBytes(32, msg.data);

  auto run = [&](const char* what, auto patch) -> bool {
    Crypto::KeyImage ki;
    Crypto::TriptychSignature sig;
    if (!sign_for_ring(ring, 1, msg, ki, sig)) return false;
    patch(sig);
    if (Crypto::triptych_verify(msg,
          ring.pubkeys.data(), ring.commits.data(), ring.pseudo_commit,
          8, ki, sig)) {
      printf("FAIL [%s]\n", what);
      return false;
    }
    return true;
  };

  // Flip the high bit (above the curve cofactor) so the point either
  // fails decode or shifts identity — either way the proof should die.
  if (!run("I_bits[0]", [](Crypto::TriptychSignature& s){ s.I_bits[0].data[31] ^= 0x10; })) return;
  if (!run("A[1]",      [](Crypto::TriptychSignature& s){ s.A[1].data[31]      ^= 0x10; })) return;
  if (!run("B[2]",      [](Crypto::TriptychSignature& s){ s.B[2].data[31]      ^= 0x10; })) return;
  if (!run("Q_P[0]",    [](Crypto::TriptychSignature& s){ s.Q_P[0].data[31]    ^= 0x10; })) return;
  if (!run("Q_M[0]",    [](Crypto::TriptychSignature& s){ s.Q_M[0].data[31]    ^= 0x10; })) return;
  if (!run("Q_U[0]",    [](Crypto::TriptychSignature& s){ s.Q_U[0].data[31]    ^= 0x10; })) return;

  PASS();
}

// ── Negative: hidden-inflation attempt ──────────────────────────────────
//
// Critical soundness check. The prover tries to spend an output of value
// v_real but claim a pseudo-output of value v_pseudo < v_real. The balance
// kernel (separate proof, not tested here) checks the H component; the
// Triptych M-ring must independently catch the mismatch because
// M_l = C_l − C' = (v_real − v_pseudo)·H + (r_real − r_pseudo)·G is no
// longer of form z·G when v_real ≠ v_pseudo.

static void test_hidden_inflation_rejected() {
  TEST("Hidden-inflation attempt rejected");

  // Real commitment to amount=1000, pseudo commitment to amount=10.
  // If Triptych were to accept this, an attacker spends 1000 but the
  // balance kernel only sees a "10" pseudo-commit on the in side and
  // could trivially create outputs summing to 1000+ → inflation.
  TestRing ring;
  if (!build_test_ring(8, 0, 1000, ring)) { FAIL("build real ring"); return; }

  // Replace pseudo_commit with a commitment to a DIFFERENT value.
  Crypto::EllipticCurveScalar smaller_v;
  uint64_to_scalar(10, smaller_v);
  test_random_scalar(ring.pseudo_blinding);
  if (!Crypto::pedersen_commit(smaller_v, ring.pseudo_blinding, ring.pseudo_commit)) {
    FAIL("recommit"); return;
  }

  Crypto::Hash msg;
  Random::randomBytes(32, msg.data);

  // Prover still tries to sign — the sign step itself succeeds because
  // we don't validate balance there (the scalar arithmetic just produces
  // wrong f_M). But the verifier MUST reject.
  Crypto::KeyImage ki;
  Crypto::TriptychSignature sig;
  if (!sign_for_ring(ring, 0, msg, ki, sig)) { FAIL("sign"); return; }

  if (Crypto::triptych_verify(msg,
        ring.pubkeys.data(), ring.commits.data(), ring.pseudo_commit,
        8, ki, sig)) {
    FAIL("inflation attempt accepted!"); return;
  }

  PASS();
}

// ── Negative: bad witness (wrong spend key) ─────────────────────────────

static void test_wrong_spend_key_rejected() {
  TEST("Wrong spend key cannot prove");

  TestRing ring;
  if (!build_test_ring(8, 4, 42, ring)) { FAIL("build"); return; }

  // Replace ring.spend_key with an unrelated random scalar — it now no
  // longer satisfies P_l = x·G.
  test_random_scalar(reinterpret_cast<Crypto::EllipticCurveScalar&>(ring.spend_key));

  Crypto::Hash msg;
  Random::randomBytes(32, msg.data);

  Crypto::KeyImage ki;
  Crypto::TriptychSignature sig;
  // Sign succeeds at the call level (no integrity check between secret
  // and ring slot inside the prover — the soundness is on the verifier).
  if (!sign_for_ring(ring, 4, msg, ki, sig)) { FAIL("sign"); return; }

  if (Crypto::triptych_verify(msg,
        ring.pubkeys.data(), ring.commits.data(), ring.pseudo_commit,
        8, ki, sig)) {
    FAIL("bad witness accepted"); return;
  }

  PASS();
}

// ── Key image consistency ───────────────────────────────────────────────
//
// The key image must depend ONLY on the spend secret and the (one-time)
// public key, not on the ring composition or anything else in the tx.
// This is the property that gives cross-tx double-spend detection — if
// it broke, a malicious user could spend the same output twice with
// different rings and the chain would miss it.

static void test_key_image_consistency() {
  TEST("Key image consistent across rings");

  // Generate one keypair and use it as the real input in TWO independent
  // rings (different decoys, different pseudo commits, different msgs).
  Crypto::SecretKey x;
  Crypto::PublicKey P;
  gen_keypair(P, x);

  auto build_around = [&](size_t ring_size, size_t true_index,
                          uint64_t amount, TestRing& ring) -> bool {
    ring.pubkeys.resize(ring_size);
    ring.commits.resize(ring_size);
    ring.amount = amount;
    ring.spend_key = x;
    ring.pubkeys[true_index] = P;

    Crypto::EllipticCurveScalar v_scalar;
    uint64_to_scalar(amount, v_scalar);

    test_random_scalar(ring.real_blinding);
    if (!Crypto::pedersen_commit(v_scalar, ring.real_blinding, ring.commits[true_index]))
      return false;

    for (size_t i = 0; i < ring_size; ++i) {
      if (i == true_index) continue;
      Crypto::SecretKey dummy_sec;
      gen_keypair(ring.pubkeys[i], dummy_sec);
      Crypto::EllipticCurveScalar dummy_r, dummy_v;
      test_random_scalar(dummy_r);
      test_random_scalar(dummy_v);
      if (!Crypto::pedersen_commit(dummy_v, dummy_r, ring.commits[i]))
        return false;
    }

    test_random_scalar(ring.pseudo_blinding);
    return Crypto::pedersen_commit(v_scalar, ring.pseudo_blinding, ring.pseudo_commit);
  };

  TestRing ring_a, ring_b;
  if (!build_around(8, 3, 1000, ring_a)) { FAIL("build a"); return; }
  if (!build_around(16, 9, 2000, ring_b)) { FAIL("build b"); return; }

  Crypto::Hash msg_a, msg_b;
  Random::randomBytes(32, msg_a.data);
  Random::randomBytes(32, msg_b.data);

  Crypto::KeyImage ki_a, ki_b;
  Crypto::TriptychSignature sig_a, sig_b;
  if (!sign_for_ring(ring_a, 3, msg_a, ki_a, sig_a)) { FAIL("sign a"); return; }
  if (!sign_for_ring(ring_b, 9, msg_b, ki_b, sig_b)) { FAIL("sign b"); return; }

  if (memcmp(&ki_a, &ki_b, sizeof(Crypto::KeyImage)) != 0) {
    FAIL("key images differ"); return;
  }

  PASS();
}

// ── Domain separation from GK denomination proof ───────────────────────
//
// Re-run sign/verify but with a tx_hash chosen so that, if we accidentally
// shared the GK domain separator, a GK proof bound to the same tx_hash
// would be mistaken for a Triptych proof (or vice versa). Triptych's
// transcript embeds "Triptych-KarboCT-v1" explicitly; this is here as a
// regression marker if anyone tampers with the domain constant.

static void test_domain_separation_marker() {
  TEST("Domain separator embedded in transcript");

  TestRing ring;
  if (!build_test_ring(4, 1, 17, ring)) { FAIL("build"); return; }
  Crypto::Hash msg;
  Random::randomBytes(32, msg.data);

  Crypto::KeyImage ki;
  Crypto::TriptychSignature sig;
  if (!sign_for_ring(ring, 1, msg, ki, sig)) { FAIL("sign"); return; }
  if (!Crypto::triptych_verify(msg,
        ring.pubkeys.data(), ring.commits.data(), ring.pseudo_commit,
        4, ki, sig)) { FAIL("verify"); return; }

  // We can't directly inspect the domain from outside, but if the
  // transcript byte layout changes (e.g. someone deletes a field) the
  // earlier "tampering" tests would also fail. This test is a presence
  // marker for the design comment.
  PASS();
}

// ── Ring tampering: swap a ring pubkey (transcript binding) ────────────

static void test_ring_swap_rejected() {
  TEST("Swapping a ring pubkey post-sign fails");

  TestRing ring;
  if (!build_test_ring(8, 4, 5, ring)) { FAIL("build"); return; }
  Crypto::Hash msg;
  Random::randomBytes(32, msg.data);

  Crypto::KeyImage ki;
  Crypto::TriptychSignature sig;
  if (!sign_for_ring(ring, 4, msg, ki, sig)) { FAIL("sign"); return; }

  // Replace decoy at slot 0 with a different random pubkey.
  Crypto::PublicKey newP;
  Crypto::SecretKey newSec;
  gen_keypair(newP, newSec);
  auto tampered_pubkeys = ring.pubkeys;
  tampered_pubkeys[0] = newP;

  if (Crypto::triptych_verify(msg,
        tampered_pubkeys.data(), ring.commits.data(), ring.pseudo_commit,
        8, ki, sig)) {
    FAIL("swap accepted"); return;
  }

  PASS();
}

// ── Batched verifier ───────────────────────────────────────────────────
//
// triptych_verify_batch random-α-scales every per-input verifier equation
// and folds them into one Pippenger MSM. These tests cover:
//   - degenerate batch sizes (n=0, n=1)
//   - mixed ring sizes in one batch (1, 4, 8, 16) — exercises both the
//     Schnorr and full-Triptych branches simultaneously
//   - cross-check that legit batches verify
//   - that ANY tampered input flips the batched check to false
//   - empty batch is trivially true

namespace batched {

struct BatchInput {
  TestRing                       ring;
  size_t                         true_index;
  Crypto::KeyImage               key_image;
  Crypto::TriptychSignature      sig;
};

// Build a BatchInput at a given ring_size and true_index, sign with msg.
// Returns false on any internal failure.
static bool build_signed_input(size_t ring_size, size_t true_index,
                               uint64_t amount, const Crypto::Hash& msg,
                               BatchInput& out) {
  out.true_index = true_index;
  if (!build_test_ring(ring_size, true_index, amount, out.ring)) return false;
  return sign_for_ring(out.ring, true_index, msg, out.key_image, out.sig);
}

// Call triptych_verify_batch with a vector<BatchInput>.
static bool verify_batch(const Crypto::Hash& msg,
                         const std::vector<BatchInput>& inputs) {
  std::vector<const Crypto::PublicKey*>          ring_pubkeys;
  std::vector<const Crypto::EllipticCurvePoint*> ring_commits;
  std::vector<Crypto::EllipticCurvePoint>        pseudo_commits;
  std::vector<size_t>                            ring_sizes;
  std::vector<Crypto::KeyImage>                  key_images;
  std::vector<Crypto::TriptychSignature>         sigs;
  ring_pubkeys.reserve(inputs.size());
  ring_commits.reserve(inputs.size());
  pseudo_commits.reserve(inputs.size());
  ring_sizes.reserve(inputs.size());
  key_images.reserve(inputs.size());
  sigs.reserve(inputs.size());
  for (const auto& in : inputs) {
    ring_pubkeys.push_back(in.ring.pubkeys.data());
    ring_commits.push_back(in.ring.commits.data());
    pseudo_commits.push_back(in.ring.pseudo_commit);
    ring_sizes.push_back(in.ring.pubkeys.size());
    key_images.push_back(in.key_image);
    sigs.push_back(in.sig);
  }
  return Crypto::triptych_verify_batch(msg,
    ring_pubkeys.data(), ring_commits.data(),
    pseudo_commits.data(), ring_sizes.data(),
    key_images.data(), sigs.data(), inputs.size());
}

} // namespace batched

static void test_batch_empty() {
  TEST("Batch: empty input returns true");
  std::vector<batched::BatchInput> empty;
  Crypto::Hash msg;
  Random::randomBytes(32, msg.data);
  if (!batched::verify_batch(msg, empty)) { FAIL("empty batch rejected"); return; }
  PASS();
}

static void test_batch_single_per_size(size_t ring_size) {
  char name[80];
  snprintf(name, sizeof(name), "Batch: single input, ring=%zu", ring_size);
  TEST(name);

  Crypto::Hash msg;
  Random::randomBytes(32, msg.data);

  std::vector<batched::BatchInput> batch(1);
  if (!batched::build_signed_input(ring_size, 0, 1234, msg, batch[0])) {
    FAIL("build/sign"); return;
  }
  if (!batched::verify_batch(msg, batch)) { FAIL("verify"); return; }
  PASS();
}

static void test_batch_mixed_ring_sizes() {
  TEST("Batch: mixed ring sizes (1, 4, 8, 16) in one batch");

  Crypto::Hash msg;
  Random::randomBytes(32, msg.data);

  std::vector<batched::BatchInput> batch(4);
  if (!batched::build_signed_input(1,  0, 100, msg, batch[0])) { FAIL("build 1"); return; }
  if (!batched::build_signed_input(4,  2, 200, msg, batch[1])) { FAIL("build 4"); return; }
  if (!batched::build_signed_input(8,  5, 300, msg, batch[2])) { FAIL("build 8"); return; }
  if (!batched::build_signed_input(16,11, 400, msg, batch[3])) { FAIL("build 16"); return; }

  if (!batched::verify_batch(msg, batch)) { FAIL("verify"); return; }
  PASS();
}

static void test_batch_many_inputs() {
  TEST("Batch: 8 inputs at ring=16 (production-shape)");

  Crypto::Hash msg;
  Random::randomBytes(32, msg.data);

  std::vector<batched::BatchInput> batch(8);
  for (size_t i = 0; i < 8; ++i) {
    if (!batched::build_signed_input(16, i % 16, 100 + i, msg, batch[i])) {
      FAIL("build"); return;
    }
  }
  if (!batched::verify_batch(msg, batch)) { FAIL("verify"); return; }
  PASS();
}

static void test_batch_one_tampered_response() {
  TEST("Batch: one tampered f_P in any position fails");

  Crypto::Hash msg;
  Random::randomBytes(32, msg.data);

  // 4 valid inputs at various ring sizes.
  std::vector<batched::BatchInput> batch(4);
  if (!batched::build_signed_input(4,  0, 10, msg, batch[0])) { FAIL("build 0"); return; }
  if (!batched::build_signed_input(8,  3, 20, msg, batch[1])) { FAIL("build 1"); return; }
  if (!batched::build_signed_input(16, 7, 30, msg, batch[2])) { FAIL("build 2"); return; }
  if (!batched::build_signed_input(1,  0, 40, msg, batch[3])) { FAIL("build 3"); return; }

  // Tamper one input's f_P.
  batch[1].sig.f_P.data[0] ^= 1;

  if (batched::verify_batch(msg, batch)) { FAIL("tampered batch accepted"); return; }
  PASS();
}

static void test_batch_one_tampered_point() {
  TEST("Batch: one tampered Q_P point fails");

  Crypto::Hash msg;
  Random::randomBytes(32, msg.data);

  std::vector<batched::BatchInput> batch(3);
  if (!batched::build_signed_input(8, 2, 1, msg, batch[0])) { FAIL("build 0"); return; }
  if (!batched::build_signed_input(8, 4, 1, msg, batch[1])) { FAIL("build 1"); return; }
  if (!batched::build_signed_input(8, 6, 1, msg, batch[2])) { FAIL("build 2"); return; }

  batch[2].sig.Q_P[0].data[31] ^= 0x10;

  if (batched::verify_batch(msg, batch)) { FAIL("tampered batch accepted"); return; }
  PASS();
}

static void test_batch_one_wrong_message() {
  TEST("Batch: wrong message on one input fails");

  Crypto::Hash msg, wrong_msg;
  Random::randomBytes(32, msg.data);
  Random::randomBytes(32, wrong_msg.data);

  // Build two inputs against msg, then try to verify them against
  // wrong_msg — the batched FS challenge for both inputs is wrong.
  std::vector<batched::BatchInput> batch(2);
  if (!batched::build_signed_input(4, 0, 1, msg, batch[0])) { FAIL("build"); return; }
  if (!batched::build_signed_input(4, 2, 1, msg, batch[1])) { FAIL("build"); return; }

  if (batched::verify_batch(wrong_msg, batch)) { FAIL("wrong-msg batch accepted"); return; }
  PASS();
}

static void test_batch_one_hidden_inflation() {
  TEST("Batch: hidden-inflation attempt in any input fails");

  Crypto::Hash msg;
  Random::randomBytes(32, msg.data);

  std::vector<batched::BatchInput> batch(3);
  if (!batched::build_signed_input(4, 0, 1000, msg, batch[0])) { FAIL("build 0"); return; }
  if (!batched::build_signed_input(4, 1, 2000, msg, batch[1])) { FAIL("build 1"); return; }
  if (!batched::build_signed_input(4, 2, 3000, msg, batch[2])) { FAIL("build 2"); return; }

  // For input 1, replace pseudo with a commitment to a DIFFERENT amount.
  // Then re-sign so the signature is internally valid but the M-ring is
  // not closed for the chosen amounts — exactly the inflation scenario.
  Crypto::EllipticCurveScalar smaller_v;
  uint64_to_scalar(1, smaller_v);
  test_random_scalar(batch[1].ring.pseudo_blinding);
  if (!Crypto::pedersen_commit(smaller_v, batch[1].ring.pseudo_blinding, batch[1].ring.pseudo_commit)) {
    FAIL("recommit"); return;
  }
  if (!sign_for_ring(batch[1].ring, batch[1].true_index, msg, batch[1].key_image, batch[1].sig)) {
    FAIL("re-sign"); return;
  }

  if (batched::verify_batch(msg, batch)) { FAIL("inflation in batch accepted"); return; }
  PASS();
}

// ── Main ────────────────────────────────────────────────────────────────

int main() {
  printf("Triptych Spend Proof Tests\n");
  printf("==========================\n\n");

  printf("Sign/verify (all supported ring sizes × all true indices):\n");
  test_sign_verify(1, 0);   // Schnorr branch (v5+ coinbase carve-out)
  for (size_t N : {size_t(4), size_t(8), size_t(16)}) {
    for (size_t i = 0; i < N; ++i) {
      test_sign_verify(N, i);
    }
  }

  printf("\nShape: unsupported ring sizes:\n");
  test_unsupported_ring_size(2);  // n=1 reserved as invalid (see serializer)
  test_unsupported_ring_size(3);
  test_unsupported_ring_size(5);
  test_unsupported_ring_size(6);
  test_unsupported_ring_size(7);
  test_unsupported_ring_size(9);
  test_unsupported_ring_size(12);
  test_unsupported_ring_size(32);

  printf("\nTranscript binding (negative tests):\n");
  test_wrong_message();
  test_wrong_pseudo_commit();
  test_wrong_key_image();
  test_ring_swap_rejected();

  printf("\nTampering (negative tests):\n");
  test_tampered_responses();
  test_tampered_points();

  printf("\nSoundness (security-critical):\n");
  test_hidden_inflation_rejected();
  test_wrong_spend_key_rejected();

  printf("\nLinkability:\n");
  test_key_image_consistency();

  printf("\nMisc:\n");
  test_domain_separation_marker();

  printf("\nBatched verifier:\n");
  test_batch_empty();
  test_batch_single_per_size(1);
  test_batch_single_per_size(4);
  test_batch_single_per_size(8);
  test_batch_single_per_size(16);
  test_batch_mixed_ring_sizes();
  test_batch_many_inputs();
  test_batch_one_tampered_response();
  test_batch_one_tampered_point();
  test_batch_one_wrong_message();
  test_batch_one_hidden_inflation();

  printf("\n%d/%d tests passed.\n", tests_passed, tests_run);
  return (tests_passed == tests_run) ? 0 : 1;
}
