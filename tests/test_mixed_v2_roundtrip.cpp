// Roundtrip self-test for v2 mixed-input CT transactions.
//
// Builds a fake v2 Transaction with one KeyInput (transparent shielding) and
// one ConfidentialInput (CT spend), serializes it via toBinaryArray, parses
// it back via fromBinaryArray, and asserts the round-trip preserves every
// observable field.
//
// Catches the class of bug where the on-wire layout for mixed v2 txs goes
// out of sync between writer and reader — which is what would surface in
// production as "Failed to query blocks: Network error" once the stored
// blob can't be read back.

#include <cassert>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <vector>

#include "CryptoNote.h"
#include "CryptoNoteConfig.h"
#include "CryptoNoteCore/CryptoNoteSerialization.h"
#include "CryptoNoteCore/CryptoNoteTools.h"

using namespace CryptoNote;

namespace {

void fill(uint8_t* p, size_t n, uint8_t seed) {
  for (size_t i = 0; i < n; ++i) p[i] = static_cast<uint8_t>(seed + i);
}

template <class T>
T makePod(uint8_t seed) {
  T v;
  fill(reinterpret_cast<uint8_t*>(&v), sizeof(T), seed);
  return v;
}

Crypto::Signature makeSig(uint8_t seed) {
  Crypto::Signature s;
  fill(reinterpret_cast<uint8_t*>(&s), sizeof(s), seed);
  return s;
}

Transaction buildMixedV2() {
  Transaction tx;
  tx.version = TRANSACTION_VERSION_CT;
  tx.unlockTime = 0;
  tx.fee = 10000000000ULL;
  tx.extra = {0x01, 0xAA, 0xBB, 0xCC};

  // Input 0: KeyInput, ring size 3
  KeyInput ki;
  ki.amount = 100000000ULL;
  ki.outputIndexes = {7, 1, 4};  // relative offsets
  ki.keyImage = makePod<Crypto::KeyImage>(0x10);
  tx.inputs.push_back(ki);

  // Input 1: ConfidentialInput, ring size 4 (Triptych n=2)
  ConfidentialInput cin;
  for (size_t k = 0; k < 4; ++k) {
    cin.ringMembers.push_back(RingMemberRef{
        100000000ULL + k * 10, static_cast<uint32_t>(k)});
    cin.ringPubkeys.push_back(makePod<Crypto::PublicKey>(0x20 + k));
    cin.ringCommitments.push_back(makePod<Crypto::EllipticCurvePoint>(0x30 + k));
  }
  cin.pseudoCommitment = makePod<Crypto::EllipticCurvePoint>(0x40);
  cin.keyImage = makePod<Crypto::KeyImage>(0x50);
  tx.inputs.push_back(cin);

  // Output: one ConfidentialOutput
  ConfidentialOutput cout;
  cout.targetKey = makePod<Crypto::PublicKey>(0x60);
  cout.commitment = makePod<Crypto::EllipticCurvePoint>(0x70);
  std::memset(cout.maskedAmount.data(), 0x80, 8);
  TransactionOutput out;
  out.amount = 0;
  out.target = cout;
  tx.outputs.push_back(out);

  // Per-input signatures: 3 ring sigs for the KeyInput, empty for the CI.
  tx.signatures.resize(2);
  for (size_t i = 0; i < 3; ++i) {
    tx.signatures[0].push_back(makeSig(0x90 + i));
  }
  // tx.signatures[1] stays empty.

  // CT signatures: empty slot for KeyInput, full Triptych for CI.
  tx.ctSignatures.resize(2);
  // [0] stays default-constructed (all empty vectors) → serialized as n=0xFF.
  CTInputSignature& cs = tx.ctSignatures[1];
  cs.I_bits.resize(2);
  cs.A.resize(2);
  cs.B.resize(2);
  cs.Q_P.resize(2);
  cs.Q_M.resize(2);
  cs.Q_U.resize(2);
  cs.z.resize(2);
  cs.za.resize(2);
  cs.zb.resize(2);
  for (size_t j = 0; j < 2; ++j) {
    cs.I_bits[j] = makePod<Crypto::EllipticCurvePoint>(0xA0 + j);
    cs.A[j]      = makePod<Crypto::EllipticCurvePoint>(0xA2 + j);
    cs.B[j]      = makePod<Crypto::EllipticCurvePoint>(0xA4 + j);
    cs.Q_P[j]    = makePod<Crypto::EllipticCurvePoint>(0xA6 + j);
    cs.Q_M[j]    = makePod<Crypto::EllipticCurvePoint>(0xA8 + j);
    cs.Q_U[j]    = makePod<Crypto::EllipticCurvePoint>(0xAA + j);
    cs.z[j]      = makePod<Crypto::EllipticCurveScalar>(0xAC + j);
    cs.za[j]     = makePod<Crypto::EllipticCurveScalar>(0xAE + j);
    cs.zb[j]     = makePod<Crypto::EllipticCurveScalar>(0xB0 + j);
  }
  cs.f_P = makePod<Crypto::EllipticCurveScalar>(0xB2);
  cs.f_M = makePod<Crypto::EllipticCurveScalar>(0xB3);
  cs.f_U = makePod<Crypto::EllipticCurveScalar>(0xB4);

  // CT output proof: dummy values.
  tx.ctProofs.resize(1);
  CTOutputProof& proof = tx.ctProofs[0];
  for (size_t i = 0; i < 6; ++i) {
    proof.I[i]  = makePod<Crypto::EllipticCurvePoint>(0xC0 + i);
    proof.A[i]  = makePod<Crypto::EllipticCurvePoint>(0xC6 + i);
    proof.B[i]  = makePod<Crypto::EllipticCurvePoint>(0xCC + i);
    proof.Q[i]  = makePod<Crypto::EllipticCurvePoint>(0xD2 + i);
    proof.z[i]  = makePod<Crypto::EllipticCurveScalar>(0xD8 + i);
    proof.za[i] = makePod<Crypto::EllipticCurveScalar>(0xDE + i);
    proof.zb[i] = makePod<Crypto::EllipticCurveScalar>(0xE4 + i);
  }
  proof.f = makePod<Crypto::EllipticCurveScalar>(0xEA);

  tx.kernel.excessCommitment = makePod<Crypto::EllipticCurvePoint>(0xF0);
  tx.kernel.sigE = makePod<Crypto::EllipticCurveScalar>(0xF1);
  tx.kernel.sigS = makePod<Crypto::EllipticCurveScalar>(0xF2);

  return tx;
}

bool memEq(const void* a, const void* b, size_t n) {
  return std::memcmp(a, b, n) == 0;
}

#define CHECK(cond)                                                            \
  do {                                                                         \
    if (!(cond)) {                                                             \
      std::fprintf(stderr, "FAIL @ %s:%d: %s\n", __FILE__, __LINE__, #cond);   \
      return 1;                                                                \
    }                                                                          \
  } while (0)

int run() {
  Transaction src = buildMixedV2();

  BinaryArray ba;
  if (!toBinaryArray(src, ba)) {
    std::fprintf(stderr, "toBinaryArray failed\n");
    return 1;
  }
  std::printf("Serialized %zu bytes\n", ba.size());

  Transaction dst;
  if (!fromBinaryArray(dst, ba)) {
    std::fprintf(stderr, "fromBinaryArray failed (stream did not reach end?)\n");
    return 1;
  }

  CHECK(dst.version == src.version);
  CHECK(dst.fee == src.fee);
  CHECK(dst.unlockTime == src.unlockTime);
  CHECK(dst.extra == src.extra);

  // Inputs
  CHECK(dst.inputs.size() == src.inputs.size());
  // Input 0: KeyInput
  CHECK(dst.inputs[0].type() == typeid(KeyInput));
  const auto& ki_src = boost::get<KeyInput>(src.inputs[0]);
  const auto& ki_dst = boost::get<KeyInput>(dst.inputs[0]);
  CHECK(ki_src.amount == ki_dst.amount);
  CHECK(ki_src.outputIndexes == ki_dst.outputIndexes);
  CHECK(memEq(&ki_src.keyImage, &ki_dst.keyImage, sizeof(ki_src.keyImage)));

  // Input 1: ConfidentialInput
  CHECK(dst.inputs[1].type() == typeid(ConfidentialInput));
  const auto& cin_src = boost::get<ConfidentialInput>(src.inputs[1]);
  const auto& cin_dst = boost::get<ConfidentialInput>(dst.inputs[1]);
  CHECK(cin_src.ringMembers.size() == cin_dst.ringMembers.size());
  for (size_t k = 0; k < cin_src.ringMembers.size(); ++k) {
    CHECK(cin_src.ringMembers[k].amount == cin_dst.ringMembers[k].amount);
    CHECK(cin_src.ringMembers[k].outputIndex == cin_dst.ringMembers[k].outputIndex);
    CHECK(memEq(&cin_src.ringPubkeys[k], &cin_dst.ringPubkeys[k], 32));
    CHECK(memEq(&cin_src.ringCommitments[k], &cin_dst.ringCommitments[k], 32));
  }
  CHECK(memEq(&cin_src.pseudoCommitment, &cin_dst.pseudoCommitment, 32));
  CHECK(memEq(&cin_src.keyImage, &cin_dst.keyImage, 32));

  // Signatures
  CHECK(dst.signatures.size() == src.signatures.size());
  CHECK(dst.signatures[0].size() == 3);
  CHECK(dst.signatures[1].size() == 0);
  for (size_t i = 0; i < 3; ++i) {
    CHECK(memEq(&dst.signatures[0][i], &src.signatures[0][i],
                sizeof(Crypto::Signature)));
  }

  // CT signatures: empty slot + full Triptych
  CHECK(dst.ctSignatures.size() == 2);
  CHECK(dst.ctSignatures[0].I_bits.empty());
  CHECK(dst.ctSignatures[0].A.empty());
  CHECK(dst.ctSignatures[0].Q_P.empty());
  CHECK(dst.ctSignatures[1].I_bits.size() == 2);
  CHECK(dst.ctSignatures[1].Q_P.size() == 2);
  for (size_t j = 0; j < 2; ++j) {
    CHECK(memEq(&dst.ctSignatures[1].I_bits[j],
                &src.ctSignatures[1].I_bits[j], 32));
    CHECK(memEq(&dst.ctSignatures[1].Q_P[j],
                &src.ctSignatures[1].Q_P[j], 32));
    CHECK(memEq(&dst.ctSignatures[1].z[j],
                &src.ctSignatures[1].z[j], 32));
  }
  CHECK(memEq(&dst.ctSignatures[1].f_P, &src.ctSignatures[1].f_P, 32));

  // Kernel
  CHECK(memEq(&dst.kernel.excessCommitment, &src.kernel.excessCommitment, 32));
  CHECK(memEq(&dst.kernel.sigE, &src.kernel.sigE, 32));
  CHECK(memEq(&dst.kernel.sigS, &src.kernel.sigS, 32));

  // ctProofs
  CHECK(dst.ctProofs.size() == 1);
  for (size_t i = 0; i < 6; ++i) {
    CHECK(memEq(&dst.ctProofs[0].I[i], &src.ctProofs[0].I[i], 32));
  }
  CHECK(memEq(&dst.ctProofs[0].f, &src.ctProofs[0].f, 32));

  std::printf("OK: v2 mixed-input round-trip preserved every field\n");
  return 0;
}

}  // namespace

int main() {
  return run();
}
