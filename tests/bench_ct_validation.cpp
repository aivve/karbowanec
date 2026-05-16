// CT worst-case validation perf benchmark.
//
// Measures the cryptographic verification time the blockchain pays per CT
// transaction at various input/output/ring-size configurations. Excludes
// per-ring-member LMDB lookups (those are I/O-bound and depend on the
// daemon's running state); this benchmark focuses on the CPU cost of
// Triptych verify, GK proof verify, balance equation, and kernel signature.
//
// Numbers from this run let you decide whether the structural caps
// (CT_MAX_INPUTS=128, CT_MAX_OUTPUTS=64, CT_MAX_RING_SIZE=16) need
// to drop, or whether they're already comfortably below a 1s per-tx
// validation budget.
//
// Build: linked from tests/CMakeLists.txt as `CTBench`. Run with no args
// to sweep the default configurations; pass `--config <preset>` to run a
// single preset.

#include "crypto/gk_proof.h"
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
#include <cstdint>
#include <string>
#include <vector>

extern "C" {
#include "crypto/crypto-ops.h"
}

using clock_t_ = std::chrono::steady_clock;

static void rand_scalar(Crypto::EllipticCurveScalar& res) {
  unsigned char tmp[64];
  Random::randomBytes(64, tmp);
  sc_reduce(tmp);
  memcpy(&res, tmp, 32);
}

static void u64_to_scalar(uint64_t v, Crypto::EllipticCurveScalar& s) {
  memset(s.data, 0, 32);
  for (int i = 0; i < 8; ++i) s.data[i] = static_cast<uint8_t>(v >> (8 * i));
}

static void gen_keypair(Crypto::PublicKey& pub, Crypto::SecretKey& sec) {
  rand_scalar(reinterpret_cast<Crypto::EllipticCurveScalar&>(sec));
  ge_p3 P;
  ge_scalarmult_base(&P, reinterpret_cast<const unsigned char*>(&sec));
  ge_p3_tobytes(reinterpret_cast<unsigned char*>(&pub), &P);
}

static void random_hash(Crypto::Hash& h) {
  Random::randomBytes(32, h.data);
}

// ─── Generated CT tx (verify-only payload) ─────────────────────────────
//
// The fields below mirror what the validator's Triptych/GK/balance steps
// actually consume — we skip the wire layout and ring-member DB resolution
// because they aren't part of the crypto cost. Build is slow (signing);
// verify is what the blockchain pays per accepted tx.

struct CtTxStub {
  size_t numInputs;
  size_t ringSize;
  size_t numOutputs;
  uint64_t fee;
  Crypto::Hash txHash;

  // Per-input
  std::vector<std::vector<Crypto::PublicKey>>              ringPubkeys;
  std::vector<std::vector<Crypto::EllipticCurvePoint>>     ringCommits;
  std::vector<Crypto::EllipticCurvePoint>                  pseudoCommits;
  std::vector<Crypto::KeyImage>                            keyImages;
  std::vector<Crypto::TriptychSignature>                   triptychSigs;

  // Per-output
  std::vector<Crypto::EllipticCurvePoint>                  outputCommits;
  std::vector<Crypto::GKProof>                             gkProofs;

  // Kernel
  Crypto::TransactionKernel                                kernel;
};

// Build a worst-case-ish CT tx for benchmarking verify time. We don't go
// through the actual TransactionBuilder because it pulls in wallet plumbing
// (deterministic tx key, addOutput etc.) we don't need here. The crypto
// objects produced are bit-identical in structure to what the validator
// would receive from the wire.
static void buildCtTx(size_t numInputs, size_t ringSize, size_t numOutputs,
                      CtTxStub& tx) {
  tx.numInputs = numInputs;
  tx.ringSize = ringSize;
  tx.numOutputs = numOutputs;
  tx.fee = 0;
  random_hash(tx.txHash);

  if (!Crypto::triptych_ring_size_supported(ringSize)) {
    std::fprintf(stderr, "buildCtTx: unsupported ring size %zu (must be 4, 8, or 16)\n", ringSize);
    std::exit(1);
  }

  // ── Inputs ──────────────────────────────────────────────────────────
  tx.ringPubkeys.assign(numInputs, std::vector<Crypto::PublicKey>(ringSize));
  tx.ringCommits.assign(numInputs, std::vector<Crypto::EllipticCurvePoint>(ringSize));
  tx.pseudoCommits.resize(numInputs);
  tx.keyImages.resize(numInputs);
  tx.triptychSigs.resize(numInputs);

  std::vector<Crypto::SecretKey> spendSecs(numInputs);
  std::vector<Crypto::EllipticCurveScalar> realBlindings(numInputs);
  std::vector<Crypto::EllipticCurveScalar> pseudoBlindings(numInputs);

  for (size_t i = 0; i < numInputs; ++i) {
    // All inputs spend the smallest denomination; the exact value doesn't
    // matter for verify time, only that all members balance pairwise.
    const uint64_t amount = CryptoNote::DENOMINATIONS[0];
    const size_t trueIdx = ringSize / 2;

    // Real spend keypair
    gen_keypair(tx.ringPubkeys[i][trueIdx], spendSecs[i]);

    // Real commitment + blinding
    rand_scalar(realBlindings[i]);
    Crypto::EllipticCurveScalar amount_scalar;
    u64_to_scalar(amount, amount_scalar);
    if (!Crypto::pedersen_commit(amount_scalar, realBlindings[i], tx.ringCommits[i][trueIdx])) {
      std::fprintf(stderr, "pedersen_commit(real) failed at input %zu\n", i);
      std::exit(1);
    }

    // Decoy keypairs and arbitrary commitments
    for (size_t k = 0; k < ringSize; ++k) {
      if (k == trueIdx) continue;
      Crypto::SecretKey dummy;
      gen_keypair(tx.ringPubkeys[i][k], dummy);
      Crypto::EllipticCurveScalar dv, dr;
      rand_scalar(dv);
      rand_scalar(dr);
      Crypto::pedersen_commit(dv, dr, tx.ringCommits[i][k]);
    }

    // Pseudo-commitment to the real amount with fresh blinding
    rand_scalar(pseudoBlindings[i]);
    if (!Crypto::pedersen_commit(amount_scalar, pseudoBlindings[i], tx.pseudoCommits[i])) {
      std::fprintf(stderr, "pedersen_commit(pseudo) failed at input %zu\n", i);
      std::exit(1);
    }

    if (!Crypto::triptych_sign(tx.txHash,
                               tx.ringPubkeys[i].data(),
                               tx.ringCommits[i].data(),
                               tx.pseudoCommits[i],
                               ringSize, trueIdx,
                               spendSecs[i], realBlindings[i], pseudoBlindings[i],
                               tx.keyImages[i], tx.triptychSigs[i])) {
      std::fprintf(stderr, "triptych_sign failed at input %zu\n", i);
      std::exit(1);
    }
  }

  // ── Outputs ─────────────────────────────────────────────────────────
  tx.outputCommits.resize(numOutputs);
  tx.gkProofs.resize(numOutputs);

  std::vector<Crypto::EllipticCurveScalar> outputBlindings(numOutputs);

  // To make outputs balance vs inputs, all outputs use the smallest
  // denomination too; we then derive fee = sum(in) - sum(out) at the end.
  // numInputs * amount = numOutputs * amount + fee
  //                fee = (numInputs - numOutputs) * amount    if positive
  // If outputs > inputs, set fee = 0 and absorb the diff by adjusting
  // (this benchmark is about verify cost, not balance correctness; we
  // patch the kernel below).
  const uint64_t denom = CryptoNote::DENOMINATIONS[0];
  for (size_t j = 0; j < numOutputs; ++j) {
    rand_scalar(outputBlindings[j]);
    Crypto::EllipticCurveScalar amount_scalar;
    u64_to_scalar(denom, amount_scalar);
    if (!Crypto::pedersen_commit(amount_scalar, outputBlindings[j], tx.outputCommits[j])) {
      std::fprintf(stderr, "pedersen_commit(out) failed at output %zu\n", j);
      std::exit(1);
    }

    // GK proof: index 0 (smallest denom)
    if (!Crypto::gk_prove(tx.outputCommits[j], denom, outputBlindings[j], /*denomIndex=*/0,
                          tx.txHash, tx.gkProofs[j])) {
      std::fprintf(stderr, "gk_prove failed at output %zu\n", j);
      std::exit(1);
    }
  }

  // Balance: sum(pseudoCommits) - sum(outputCommits) - fee*H = excess*G.
  // For numInputs * denom == numOutputs * denom + fee, fee balances.
  // For uneven counts, we set fee accordingly (signed-ish by overflow,
  // but for benchmark we use |numInputs - numOutputs| * denom and accept
  // the kernel will be wrong if numOutputs > numInputs — verify will
  // still run, which is what we're timing).
  if (numInputs >= numOutputs) {
    tx.fee = (numInputs - numOutputs) * denom;
  } else {
    tx.fee = 0;
  }
  Crypto::EllipticCurveScalar excess;
  Crypto::compute_excess_scalar(pseudoBlindings.data(), numInputs,
                                 outputBlindings.data(), numOutputs, excess);
  if (!Crypto::sign_transaction_kernel(excess, tx.txHash, tx.kernel)) {
    std::fprintf(stderr, "sign_transaction_kernel failed\n");
    std::exit(1);
  }
}

struct VerifyTiming {
  double triptychMicros = 0.0;
  double gkMicros = 0.0;
  double balanceMicros = 0.0;
  double totalMicros = 0.0;
  bool allPassed = true;
};

// Run the CPU-bound portion of the validator pipeline. Skips ring-member
// DB resolution (the validator does m_db.getKeyOutput per member; that's
// I/O cost we don't measure here) and skips subgroup pre-checks on inputs
// (cheap; in the same ballpark as one Triptych scalarmult).
static VerifyTiming verifyCtTx(const CtTxStub& tx) {
  VerifyTiming t;

  // Triptych verify per input
  {
    auto start = clock_t_::now();
    for (size_t i = 0; i < tx.numInputs; ++i) {
      bool ok = Crypto::triptych_verify(tx.txHash,
                                        tx.ringPubkeys[i].data(),
                                        tx.ringCommits[i].data(),
                                        tx.pseudoCommits[i],
                                        tx.ringSize,
                                        tx.keyImages[i],
                                        tx.triptychSigs[i]);
      if (!ok) {
        t.allPassed = false;
        std::fprintf(stderr, "[bench] Triptych verify FAILED at input %zu\n", i);
      }
    }
    auto end = clock_t_::now();
    t.triptychMicros = std::chrono::duration<double, std::micro>(end - start).count();
  }

  // GK verify per output
  {
    auto start = clock_t_::now();
    for (size_t j = 0; j < tx.numOutputs; ++j) {
      bool ok = Crypto::gk_verify(tx.outputCommits[j], tx.gkProofs[j], tx.txHash);
      if (!ok) {
        t.allPassed = false;
        std::fprintf(stderr, "[bench] GK verify FAILED at output %zu\n", j);
      }
    }
    auto end = clock_t_::now();
    t.gkMicros = std::chrono::duration<double, std::micro>(end - start).count();
  }

  // Balance equation + kernel signature
  {
    auto start = clock_t_::now();
    bool ok = Crypto::verify_transaction_balance(tx.pseudoCommits.data(), tx.numInputs,
                                                 tx.outputCommits.data(), tx.numOutputs,
                                                 tx.fee, tx.txHash, tx.kernel);
    if (!ok) {
      // Expected to fail when numOutputs > numInputs (synthetic tx). We
      // still timed the work — that's the point.
      t.allPassed = false;
    }
    auto end = clock_t_::now();
    t.balanceMicros = std::chrono::duration<double, std::micro>(end - start).count();
  }

  t.totalMicros = t.triptychMicros + t.gkMicros + t.balanceMicros;
  return t;
}

// Same as verifyCtTx but uses Crypto::triptych_verify_batch over all
// inputs and Crypto::gk_verify_batch over all outputs (the consensus
// fast path). Balance step is identical.
static VerifyTiming verifyCtTxBatchedGK(const CtTxStub& tx) {
  VerifyTiming t;

  // Batched Triptych verify across all inputs in one Pippenger MSM.
  {
    std::vector<const Crypto::PublicKey*>           ring_pubkeys;
    std::vector<const Crypto::EllipticCurvePoint*>  ring_commits;
    std::vector<size_t>                             ring_sizes;
    ring_pubkeys.reserve(tx.numInputs);
    ring_commits.reserve(tx.numInputs);
    ring_sizes.reserve(tx.numInputs);
    for (size_t i = 0; i < tx.numInputs; ++i) {
      ring_pubkeys.push_back(tx.ringPubkeys[i].data());
      ring_commits.push_back(tx.ringCommits[i].data());
      ring_sizes.push_back(tx.ringSize);
    }

    auto start = clock_t_::now();
    bool ok = Crypto::triptych_verify_batch(
      tx.txHash,
      ring_pubkeys.data(),
      ring_commits.data(),
      tx.pseudoCommits.data(),
      ring_sizes.data(),
      tx.keyImages.data(),
      tx.triptychSigs.data(),
      tx.numInputs);
    if (!ok) {
      t.allPassed = false;
      std::fprintf(stderr, "[bench] BATCHED Triptych verify FAILED\n");
    }
    auto end = clock_t_::now();
    t.triptychMicros = std::chrono::duration<double, std::micro>(end - start).count();
  }

  // Batched GK verify over all outputs at once.
  {
    auto start = clock_t_::now();
    bool ok = Crypto::gk_verify_batch(tx.outputCommits.data(),
                                       tx.gkProofs.data(),
                                       tx.numOutputs,
                                       tx.txHash);
    if (!ok) {
      t.allPassed = false;
      std::fprintf(stderr, "[bench] BATCHED GK verify FAILED\n");
    }
    auto end = clock_t_::now();
    t.gkMicros = std::chrono::duration<double, std::micro>(end - start).count();
  }

  // Balance equation + kernel signature (same path as the non-batched flow).
  {
    auto start = clock_t_::now();
    bool ok = Crypto::verify_transaction_balance(tx.pseudoCommits.data(), tx.numInputs,
                                                 tx.outputCommits.data(), tx.numOutputs,
                                                 tx.fee, tx.txHash, tx.kernel);
    if (!ok) {
      t.allPassed = false;
    }
    auto end = clock_t_::now();
    t.balanceMicros = std::chrono::duration<double, std::micro>(end - start).count();
  }

  t.totalMicros = t.triptychMicros + t.gkMicros + t.balanceMicros;
  return t;
}

// ─── Worst-case size estimate ──────────────────────────────────────────
//
// Per-input wire cost at ring size R, with n = log2(R) ∈ {2, 3, 4}:
//   ~32 (pseudo) + 32 (key image) + R * (varint amount + varint offset
//   + 32 pubkey + 32 commit) + Triptych (1 byte n + (9n + 3) × 32) + tag byte
//   = 64 + R × (96 + 9 amount + 5 offset) + 1 + (9n + 3) × 32 + 1
//   = 66 + R × 110 + (9n + 3) × 32   bytes
//
// Per-output wire cost: 32 target_key + 32 commit + 8 masked + ~5 amount
//                       + GK proof = ~77 + 1376 = ~1453 bytes
//
// Plus kernel ~96, extra ~33, version/fee ~10
static size_t estimateWireSize(size_t numInputs, size_t ringSize, size_t numOutputs) {
  size_t n = (ringSize == 4) ? 2 : (ringSize == 8) ? 3 : 4;  // log2(R)
  size_t triptychBytes = 1 + (9 * n + 3) * 32;
  size_t perInput = 66 + ringSize * 110 + triptychBytes;
  size_t perOutput = 1453;
  size_t fixed = 96 + 33 + 10;
  return numInputs * perInput + numOutputs * perOutput + fixed;
}

struct Preset {
  const char* name;
  size_t numInputs;
  size_t ringSize;
  size_t numOutputs;
  size_t iterations;
  const char* note;
};

static const Preset kPresets[] = {
  {"typical",       1,   4,   2,  20, "single 1-in/2-out, min ring (n=2)"},
  {"common-ct",     2,  16,   3,  10, "2 inputs ring 16 (n=4), 3 outputs"},
  {"size-medium",  10,  16,  10,   5, "10 in / 10 out, ring 16"},
  {"size-heavy",   50,  16,  50,   3, "50 in / 50 out, ring 16"},
  {"input-bound", 100,  16,  17,   3, "~size-cap, input-heavy"},
  {"output-bound", 50,  16,  64,   3, "~size-cap, output-heavy"},
  {"structural-max", 128, 16, 64,  1, "structural caps (CT_MAX_INPUTS, CT_MAX_OUTPUTS)"},
};
static const size_t kNumPresets = sizeof(kPresets) / sizeof(kPresets[0]);

static void runPreset(const Preset& p, bool batchedGK) {
  const size_t wireSize = estimateWireSize(p.numInputs, p.ringSize, p.numOutputs);
  const double wireKb = wireSize / 1024.0;
  const size_t sizeCap = CryptoNote::parameters::MAX_TRANSACTION_SIZE_LIMIT;

  std::printf("\n[%s%s] %s\n", p.name, batchedGK ? " +batched-gk" : "", p.note);
  std::printf("  inputs=%zu ring=%zu outputs=%zu iterations=%zu\n",
              p.numInputs, p.ringSize, p.numOutputs, p.iterations);
  std::printf("  estimated wire size ≈ %.1f KB (consensus cap = %.1f KB)%s\n",
              wireKb, sizeCap / 1024.0,
              wireSize > sizeCap ? "  ⚠ exceeds size cap" : "");

  std::printf("  building tx... ");
  std::fflush(stdout);
  auto buildStart = clock_t_::now();
  CtTxStub tx;
  buildCtTx(p.numInputs, p.ringSize, p.numOutputs, tx);
  auto buildEnd = clock_t_::now();
  double buildMs = std::chrono::duration<double, std::milli>(buildEnd - buildStart).count();
  std::printf("done in %.1f ms\n", buildMs);

  auto runOnce = [&]() {
    return batchedGK ? verifyCtTxBatchedGK(tx) : verifyCtTx(tx);
  };

  // Warm-up
  (void)runOnce();

  // Timed iterations
  double sumTriptych = 0, sumGk = 0, sumBalance = 0, sumTotal = 0;
  double minTotal = 1e18, maxTotal = 0;
  for (size_t it = 0; it < p.iterations; ++it) {
    VerifyTiming t = runOnce();
    sumTriptych += t.triptychMicros;
    sumGk += t.gkMicros;
    sumBalance += t.balanceMicros;
    sumTotal += t.totalMicros;
    if (t.totalMicros < minTotal) minTotal = t.totalMicros;
    if (t.totalMicros > maxTotal) maxTotal = t.totalMicros;
  }
  double avgTriptych = sumTriptych / p.iterations;
  double avgGk = sumGk / p.iterations;
  double avgBalance = sumBalance / p.iterations;
  double avgTotal = sumTotal / p.iterations;

  const double triptychPerInputUs = p.numInputs > 0 ? avgTriptych / p.numInputs : 0;
  const double gkPerOutputUs      = p.numOutputs > 0 ? avgGk / p.numOutputs : 0;

  std::printf("  Triptych verify : %8.1f ms total  (%.1f us / input)%s\n",
              avgTriptych / 1000.0, triptychPerInputUs,
              batchedGK ? "  [batched]" : "");
  std::printf("  GK       verify : %8.1f ms total  (%.1f us / output)%s\n",
              avgGk / 1000.0, gkPerOutputUs,
              batchedGK ? "  [batched]" : "");
  std::printf("  Balance+kern    : %8.1f ms total\n", avgBalance / 1000.0);
  std::printf("  TOTAL        : %8.1f ms  (min %.1f, max %.1f ms over %zu iters)\n",
              avgTotal / 1000.0, minTotal / 1000.0, maxTotal / 1000.0, p.iterations);

  // Verdict against 1-second-per-tx DoS budget.
  const double budgetMs = 1000.0;
  if (avgTotal / 1000.0 > budgetMs) {
    std::printf("  >>> EXCEEDS 1s/tx budget — DoS vector if structurally reachable\n");
  } else if (avgTotal / 1000.0 > budgetMs * 0.5) {
    std::printf("  >>> within budget but >50%% of it — tighten caps if reachable\n");
  } else {
    std::printf("  OK well below 1s/tx budget\n");
  }
}

// One-shot generator: compute V[k]*H for k=0..63 from scratch and print
// the 32-byte compressed encodings as a C++ literal initialiser. The
// output is pasted into src/crypto/gk_denomination_table.h and held as
// build-time-visible constants. See gk_proof.cpp's VkHTable comment for
// the rationale.
static void printVkHTable() {
  ge_p3 H_p3;
  const Crypto::EllipticCurvePoint& H = Crypto::pedersen_get_H();
  if (ge_frombytes_vartime(&H_p3,
      reinterpret_cast<const unsigned char*>(&H)) != 0) {
    std::fprintf(stderr, "FATAL: H decode failed\n");
    std::exit(1);
  }
  std::printf("// Generated by `CTBench --print-vkh-table`. Do not hand-edit.\n");
  std::printf("// V[k]*H in compressed encoding, k=0..63, where:\n");
  std::printf("//   V[k] = CryptoNote::DENOMINATIONS[k]\n");
  std::printf("//   H    = hash_to_point(\"CN-amount-generator\")\n");
  std::printf("// A unit test (BakedVkHTableMatchesFromScratch) verifies this\n");
  std::printf("// table against the from-scratch computation on every build.\n");
  std::printf("static const unsigned char kVkH_baked[64][32] = {\n");
  for (size_t k = 0; k < 64; ++k) {
    Crypto::EllipticCurveScalar v_scalar;
    std::memset(v_scalar.data, 0, 32);
    const uint64_t v = CryptoNote::DENOMINATIONS[k];
    for (int i = 0; i < 8; ++i) v_scalar.data[i] = static_cast<uint8_t>(v >> (8 * i));

    ge_p2 vkH_p2;
    ge_scalarmult(&vkH_p2, reinterpret_cast<const unsigned char*>(&v_scalar), &H_p3);
    unsigned char bytes[32];
    ge_tobytes(bytes, &vkH_p2);

    std::printf("  /* k=%2zu, V=%llu */ {", k, (unsigned long long)v);
    for (int b = 0; b < 32; ++b) {
      if (b % 8 == 0 && b > 0) std::printf(" ");
      std::printf("0x%02x,", bytes[b]);
    }
    std::printf(" },\n");
  }
  std::printf("};\n");
}

int main(int argc, char** argv) {
  // One-shot flag: print the V[k]*H table and exit, without running benches.
  for (int i = 1; i < argc; ++i) {
    if (std::string(argv[i]) == "--print-vkh-table") {
      printVkHTable();
      return 0;
    }
  }

  std::printf("CT validation worst-case benchmark\n");
  std::printf("==================================\n");
  std::printf("CT_MIN_RING_SIZE        = %zu\n", CryptoNote::parameters::CT_MIN_RING_SIZE);
  std::printf("CT_MAX_RING_SIZE        = %zu\n", CryptoNote::parameters::CT_MAX_RING_SIZE);
  std::printf("CT_MAX_INPUTS           = %zu\n", CryptoNote::parameters::CT_MAX_INPUTS);
  std::printf("CT_MAX_OUTPUTS          = %zu\n", CryptoNote::parameters::CT_MAX_OUTPUTS);
  std::printf("MAX_TRANSACTION_SIZE    = %.1f KB\n",
              CryptoNote::parameters::MAX_TRANSACTION_SIZE_LIMIT / 1024.0);
  std::printf("\nReporting average over N iterations per config.\n"
              "Verify-only; excludes per-ring-member DB lookups (I/O cost).\n");

  std::string single;
  bool batchedGK = false;
  bool compareBoth = false;
  for (int i = 1; i < argc; ++i) {
    std::string a = argv[i];
    if (a == "--config" && i + 1 < argc)      { single = argv[i + 1]; ++i; }
    else if (a == "--batched-gk")             { batchedGK = true; }
    else if (a == "--compare-gk")             { compareBoth = true; }
  }

  for (size_t i = 0; i < kNumPresets; ++i) {
    if (!single.empty() && single != kPresets[i].name) continue;
    if (compareBoth) {
      runPreset(kPresets[i], false);   // baseline (per-output gk_verify)
      runPreset(kPresets[i], true);    // batched gk_verify_batch
    } else {
      runPreset(kPresets[i], batchedGK);
    }
  }

  std::printf("\nDone.\n");
  return 0;
}
