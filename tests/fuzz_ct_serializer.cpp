// Fuzz harness for the CT v2 transaction deserializer.
//
// Two build modes:
//
// 1. libFuzzer (clang -fsanitize=fuzzer): builds with the
//    LLVMFuzzerTestOneInput entrypoint. Run via the libFuzzer driver.
//    Recommended: build with ASan + UBSan for bug detection.
//
// 2. Standalone (MSVC or any C++17 compiler): builds with a main() that
//    seeds with hand-built CT-shaped corpus and does a simple PRNG-driven
//    mutation loop. Not coverage-guided, but exercises the parser with
//    millions of weird inputs and catches throws / hangs.
//
// Either mode focuses on Transaction's fromBinaryArray<> path with bytes
// crafted to look like CT v2 (version byte 2) so the CT-specific code
// paths get most of the exercise. The bounded-vector size checks in
// CryptoNoteSerialization.cpp are the primary target.

#include "CryptoNote.h"
#include "CryptoNoteConfig.h"
#include "CryptoNoteCore/CryptoNoteTools.h"
#include "CryptoNoteCore/CryptoNoteSerialization.h"
#include "Common/MemoryInputStream.h"
#include "Common/Varint.h"
#include "Serialization/BinaryInputStreamSerializer.h"

#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <exception>
#include <random>
#include <string>
#include <vector>

using namespace CryptoNote;

namespace {

void appendVarint(BinaryArray& blob, uint64_t value) {
  std::string encoded = Tools::get_varint_data(value);
  blob.insert(blob.end(), encoded.begin(), encoded.end());
}

void appendZeros(BinaryArray& blob, size_t count) {
  blob.insert(blob.end(), count, 0);
}

// Synthesize a small handful of CT-shaped blobs to seed the fuzzer. None
// of these need to verify or even fully parse; the point is to give the
// mutator a starting distribution skewed toward shapes the validator
// might actually see (CT v2 version byte, fee field, vin/vout markers,
// proof body markers).
std::vector<BinaryArray> buildSeedCorpus() {
  std::vector<BinaryArray> corpus;

  // Minimal CT-v2 transaction prefix shape.
  {
    BinaryArray b;
    appendVarint(b, TRANSACTION_VERSION_CT);  // version
    appendVarint(b, 0);                       // fee
    appendVarint(b, 0);                       // vin count
    appendVarint(b, 0);                       // vout count
    appendVarint(b, 0);                       // extra size
    appendVarint(b, 0);                       // ct_signatures count
    appendVarint(b, 0);                       // ct_proofs count
    appendZeros(b, 32);                       // kernel.excess
    appendZeros(b, 32);                       // kernel.sigE
    appendZeros(b, 32);                       // kernel.sigS
    corpus.push_back(std::move(b));
  }

  // Single CT input with a 1-member ring (v5+ coinbase carve-out) +
  // Triptych Schnorr proof body (n=0: 3 Q points + 3 scalars).
  {
    BinaryArray b;
    appendVarint(b, TRANSACTION_VERSION_CT);
    appendVarint(b, parameters::CT_MINIMUM_FEE);
    appendVarint(b, 1);                       // vin count
    b.push_back(0x04);                        // input tag = ConfidentialInput
    appendVarint(b, 1);                       // ring_members count
    appendVarint(b, 1);                       // member[0].amount
    appendVarint(b, 0);                       // member[0].outputIndex
    appendVarint(b, 1);                       // ring_pubkeys count
    appendZeros(b, 32);                       // pubkey
    appendVarint(b, 1);                       // ring_commits count
    appendZeros(b, 32);                       // commit
    appendZeros(b, 32);                       // pseudo_commit
    appendZeros(b, 32);                       // key image
    appendVarint(b, 0);                       // vout count
    appendVarint(b, 0);                       // extra size
    appendVarint(b, 1);                       // ct_signatures count
    b.push_back(0x00);                        // sig[0].n = 0 (Schnorr branch)
    appendZeros(b, 3 * 32);                   // Q_P[0], Q_M[0], Q_U[0]
    appendZeros(b, 3 * 32);                   // f_P, f_M, f_U
    appendVarint(b, 0);                       // ct_proofs count
    appendZeros(b, 96);                       // kernel (excess+sigE+sigS)
    corpus.push_back(std::move(b));
  }

  // Single CT input with a 4-member ring + corresponding Triptych proof
  // (n = log2(4) = 2: 6×2 = 12 points, 3×2 + 3 = 9 scalars in the proof).
  {
    BinaryArray b;
    appendVarint(b, TRANSACTION_VERSION_CT);
    appendVarint(b, parameters::CT_MINIMUM_FEE);
    appendVarint(b, 1);                       // vin count
    b.push_back(0x04);                        // input tag = ConfidentialInput
    appendVarint(b, 4);                       // ring_members count
    for (int k = 0; k < 4; ++k) {
      appendVarint(b, 1);                     // member[k].amount
      appendVarint(b, static_cast<uint64_t>(k));  // member[k].outputIndex
    }
    appendVarint(b, 4);                       // ring_pubkeys count
    appendZeros(b, 4 * 32);                   // 4 pubkeys
    appendVarint(b, 4);                       // ring_commits count
    appendZeros(b, 4 * 32);                   // 4 commits
    appendZeros(b, 32);                       // pseudo_commit
    appendZeros(b, 32);                       // key image
    appendVarint(b, 0);                       // vout count
    appendVarint(b, 0);                       // extra size
    appendVarint(b, 1);                       // ct_signatures count
    b.push_back(0x02);                        // sig[0].n = 2 (ring size 4)
    appendZeros(b, 6 * 2 * 32);               // 6n=12 points (I_bits/A/B/Q_P/Q_M/Q_U)
    appendZeros(b, (3 * 2 + 3) * 32);         // 3n+3=9 scalars (z/za/zb + f_P/f_M/f_U)
    appendVarint(b, 0);                       // ct_proofs count
    appendZeros(b, 96);                       // kernel (excess+sigE+sigS)
    corpus.push_back(std::move(b));
  }

  // Boundary blob: claims input count at exactly the cap.
  {
    BinaryArray b;
    appendVarint(b, TRANSACTION_VERSION_CT);
    appendVarint(b, 0);
    appendVarint(b, parameters::CT_MAX_INPUTS);  // boundary
    corpus.push_back(std::move(b));
  }

  // Boundary blob: claims input count one over the cap (should reject).
  {
    BinaryArray b;
    appendVarint(b, TRANSACTION_VERSION_CT);
    appendVarint(b, 0);
    appendVarint(b, parameters::CT_MAX_INPUTS + 1);
    corpus.push_back(std::move(b));
  }

  // Boundary blob: claims ring_members count one over the per-input cap.
  {
    BinaryArray b;
    appendVarint(b, TRANSACTION_VERSION_CT);
    appendVarint(b, 0);
    appendVarint(b, 1);
    b.push_back(0x04);
    appendVarint(b, parameters::CT_MAX_RING_SIZE + 1);
    corpus.push_back(std::move(b));
  }

  // Boundary blob: claims output count over cap.
  {
    BinaryArray b;
    appendVarint(b, TRANSACTION_VERSION_CT);
    appendVarint(b, 0);
    appendVarint(b, 0);
    appendVarint(b, parameters::CT_MAX_OUTPUTS + 1);
    corpus.push_back(std::move(b));
  }

  // Pure-noise small input.
  {
    BinaryArray b(64, 0xAA);
    b[0] = TRANSACTION_VERSION_CT;
    corpus.push_back(std::move(b));
  }

  return corpus;
}

// Parse one blob through the v2 deserializer. Catches std::exception (the
// expected failure mode for malformed input) and reports unknown exception
// types separately. Any unhandled non-C++ exception (e.g. an access
// violation on Windows or SIGSEGV elsewhere) bubbles up to the runtime —
// libFuzzer turns those into crash reports; the standalone driver dies.
enum class ParseOutcome { Ok, ThrewStd, ThrewUnknown };

ParseOutcome parseOne(const uint8_t* data, size_t size) {
  if (size > 2 * 1024 * 1024) return ParseOutcome::Ok;  // cap blob size
  Transaction tx;
  // Call the serializer directly (fromBinaryArray swallows std::exception
  // internally, which would mask all the bounded-vector rejections we want
  // the fuzzer to count). The crash-detection signal — surviving without
  // SIGSEGV / AV / hang — is the same either way; this gives us the
  // accurate throw distribution as a bonus.
  try {
    Common::MemoryInputStream stream(data, size);
    BinaryInputStreamSerializer serializer(stream);
    serialize(tx, serializer);
    return ParseOutcome::Ok;
  } catch (const std::exception&) {
    return ParseOutcome::ThrewStd;
  } catch (...) {
    return ParseOutcome::ThrewUnknown;
  }
}

}  // namespace

extern "C" int LLVMFuzzerTestOneInput(const uint8_t* data, size_t size) {
  parseOne(data, size);
  return 0;
}

// ─── Standalone driver (only when libFuzzer's main isn't linked) ───────
//
// The libFuzzer runtime provides main(); when we're not linked against it,
// we run our own dumb-fuzzing loop. Picked at link time via the
// CTFUZZER_STANDALONE define from CMake.

#ifdef CTFUZZER_STANDALONE

namespace {

struct Stats {
  uint64_t iterations = 0;
  uint64_t parsed = 0;
  uint64_t threwStd = 0;
  uint64_t threwUnknown = 0;
};

// Mutate `blob` in place using a simple PRNG. Picks one of a handful of
// strategies skewed to provoke length-related bugs (the bounded-vector
// resize logic is the highest-risk surface).
void mutate(std::mt19937& rng, BinaryArray& blob) {
  if (blob.empty()) {
    blob.push_back(static_cast<uint8_t>(rng() & 0xFF));
    return;
  }
  std::uniform_int_distribution<int> strat(0, 6);
  std::uniform_int_distribution<size_t> posDist(0, blob.size() - 1);
  switch (strat(rng)) {
    case 0: {  // bit flip
      size_t pos = posDist(rng);
      blob[pos] ^= 1u << (rng() & 7);
      break;
    }
    case 1: {  // byte replace
      size_t pos = posDist(rng);
      blob[pos] = static_cast<uint8_t>(rng() & 0xFF);
      break;
    }
    case 2: {  // insert random byte
      size_t pos = posDist(rng);
      blob.insert(blob.begin() + pos, static_cast<uint8_t>(rng() & 0xFF));
      break;
    }
    case 3: {  // delete byte
      size_t pos = posDist(rng);
      blob.erase(blob.begin() + pos);
      break;
    }
    case 4: {  // insert run of zeros
      size_t pos = posDist(rng);
      size_t n = 1 + (rng() % 32);
      blob.insert(blob.begin() + pos, n, 0);
      break;
    }
    case 5: {  // insert run of 0xFF (often big varint)
      size_t pos = posDist(rng);
      size_t n = 1 + (rng() % 16);
      blob.insert(blob.begin() + pos, n, 0xFF);
      break;
    }
    case 6: {  // truncate
      size_t cut = 1 + (rng() % blob.size());
      blob.resize(blob.size() - std::min(cut, blob.size() / 2 + 1));
      break;
    }
  }
}

}  // namespace

int main(int argc, char** argv) {
  uint64_t iterations = 200000;
  uint64_t seed = std::chrono::steady_clock::now().time_since_epoch().count();
  for (int i = 1; i + 1 < argc; ++i) {
    std::string a = argv[i];
    if (a == "--iterations") iterations = std::strtoull(argv[i + 1], nullptr, 10);
    else if (a == "--seed")   seed = std::strtoull(argv[i + 1], nullptr, 10);
  }

  std::printf("CT serializer fuzz harness (standalone PRNG-driven)\n");
  std::printf("====================================================\n");
  std::printf("iterations = %llu\n", static_cast<unsigned long long>(iterations));
  std::printf("seed       = %llu\n", static_cast<unsigned long long>(seed));
  std::printf("\nFor real coverage-guided fuzzing build with clang -fsanitize=fuzzer,address.\n");

  std::vector<BinaryArray> corpus = buildSeedCorpus();
  std::printf("seeded corpus = %zu blobs\n\n", corpus.size());

  std::mt19937 rng(static_cast<uint32_t>(seed));
  Stats stats;

  auto t0 = std::chrono::steady_clock::now();

  for (uint64_t it = 0; it < iterations; ++it) {
    // Pick a corpus seed and mutate a copy.
    BinaryArray blob = corpus[rng() % corpus.size()];

    // Do 1..8 rounds of mutation to walk further from the seed.
    int rounds = 1 + (rng() % 8);
    for (int r = 0; r < rounds; ++r) mutate(rng, blob);

    ParseOutcome r = parseOne(blob.data(), blob.size());
    stats.iterations++;
    switch (r) {
      case ParseOutcome::Ok:           stats.parsed++; break;
      case ParseOutcome::ThrewStd:     stats.threwStd++; break;
      case ParseOutcome::ThrewUnknown: stats.threwUnknown++; break;
    }

    // Promote any blob that parses cleanly into the corpus, capped so we
    // don't bloat memory. Gives subsequent mutations a chance to drill
    // into deeper parser states.
    if (r == ParseOutcome::Ok && corpus.size() < 256) {
      corpus.push_back(std::move(blob));
    }

    // Periodic progress.
    if ((it & 0x1FFF) == 0 && it > 0) {
      auto now = std::chrono::steady_clock::now();
      double sec = std::chrono::duration<double>(now - t0).count();
      std::printf("  %8llu iters  %.1fs  (%.0f/s)  parsed=%llu  threw(std)=%llu  threw(unknown)=%llu\n",
                  static_cast<unsigned long long>(it), sec, it / sec,
                  static_cast<unsigned long long>(stats.parsed),
                  static_cast<unsigned long long>(stats.threwStd),
                  static_cast<unsigned long long>(stats.threwUnknown));
      std::fflush(stdout);
    }
  }

  auto t1 = std::chrono::steady_clock::now();
  double sec = std::chrono::duration<double>(t1 - t0).count();

  std::printf("\n--- DONE ---\n");
  std::printf("iterations    = %llu  (%.0f / sec)\n",
              static_cast<unsigned long long>(stats.iterations), stats.iterations / sec);
  std::printf("parsed clean  = %llu\n", static_cast<unsigned long long>(stats.parsed));
  std::printf("std::exception= %llu\n", static_cast<unsigned long long>(stats.threwStd));
  std::printf("unknown throw = %llu\n", static_cast<unsigned long long>(stats.threwUnknown));
  std::printf("final corpus  = %zu\n", corpus.size());

  // No crashes means we got through `iterations` mutations without an
  // unhandled SEH/SIGSEGV. The harness can't catch those, but the process
  // dying would. Survival = baseline robustness.
  std::printf("\nNo crashes — parser survived %llu mutated inputs.\n",
              static_cast<unsigned long long>(stats.iterations));
  return 0;
}

#endif  // CTFUZZER_STANDALONE
