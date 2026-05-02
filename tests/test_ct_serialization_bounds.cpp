#include <cstdint>
#include <iostream>

#include "Common/BinaryArray.hpp"
#include "CryptoNoteConfig.h"
#include "CryptoNoteCore/CryptoNoteTools.h"

namespace {

void appendVarint(CryptoNote::BinaryArray& blob, uint64_t value) {
  while (value >= 0x80) {
    blob.push_back(static_cast<uint8_t>((value & 0x7f) | 0x80));
    value >>= 7;
  }
  blob.push_back(static_cast<uint8_t>(value));
}

void appendZeros(CryptoNote::BinaryArray& blob, size_t count) {
  blob.insert(blob.end(), count, 0);
}

void appendCtHeader(CryptoNote::BinaryArray& blob) {
  appendVarint(blob, CryptoNote::TRANSACTION_VERSION_CT);
  appendVarint(blob, 0); // fee
}

void appendCtInput(CryptoNote::BinaryArray& blob, size_t ringPubkeyCount, size_t ringCommitmentCount) {
  blob.push_back(0x04); // ConfidentialInput variant tag
  appendVarint(blob, CryptoNote::parameters::CT_CONFIDENTIAL_OUTPUT_AMOUNT);
  appendVarint(blob, ringPubkeyCount);
  appendZeros(blob, ringPubkeyCount * sizeof(Crypto::PublicKey));
  appendVarint(blob, ringCommitmentCount);
  appendZeros(blob, ringCommitmentCount * sizeof(Crypto::EllipticCurvePoint));
  appendZeros(blob, sizeof(Crypto::EllipticCurvePoint)); // pseudoCommitment
  appendZeros(blob, sizeof(Crypto::KeyImage));
}

void appendCtOutput(CryptoNote::BinaryArray& blob) {
  appendVarint(blob, 0); // hidden amount field
  blob.push_back(0x04);  // ConfidentialOutput variant tag
  appendZeros(blob, sizeof(Crypto::PublicKey));
  appendZeros(blob, sizeof(Crypto::EllipticCurvePoint));
  appendZeros(blob, 8);
}

void appendValidCtPrefix(CryptoNote::BinaryArray& blob) {
  appendCtHeader(blob);
  appendVarint(blob, 1); // vin
  appendCtInput(blob, 1, 1);
  appendVarint(blob, 1); // vout
  appendCtOutput(blob);
  appendVarint(blob, 0); // extra
}

void appendCtSignature(CryptoNote::BinaryArray& blob, size_t ssCount) {
  appendZeros(blob, sizeof(Crypto::EllipticCurveScalar)); // c0
  appendVarint(blob, ssCount);
  for (size_t i = 0; i < ssCount; ++i) {
    appendZeros(blob, 2 * sizeof(Crypto::EllipticCurveScalar));
  }
}

void appendCtProof(CryptoNote::BinaryArray& blob) {
  static_assert(sizeof(CryptoNote::CTOutputProof) == 1376, "Unexpected CTOutputProof size");
  appendZeros(blob, sizeof(CryptoNote::CTOutputProof));
}

void appendKernel(CryptoNote::BinaryArray& blob) {
  appendZeros(blob, sizeof(CryptoNote::TransactionKernel));
}

CryptoNote::BinaryArray makeValidCtTransactionBlob() {
  CryptoNote::BinaryArray blob;
  appendValidCtPrefix(blob);
  appendVarint(blob, 1); // ct_signatures
  appendCtSignature(blob, 1);
  appendVarint(blob, 1); // ct_proofs
  appendCtProof(blob);
  appendKernel(blob);
  return blob;
}

bool parses(const CryptoNote::BinaryArray& blob) {
  CryptoNote::Transaction tx;
  return CryptoNote::fromBinaryArray(tx, blob);
}

bool expectParseResult(const char* name, const CryptoNote::BinaryArray& blob, bool expected) {
  const bool actual = parses(blob);
  if (actual != expected) {
    std::cerr << name << ": expected parse result " << expected << ", got " << actual << std::endl;
    return false;
  }
  return true;
}

} // namespace

int main() {
  int failures = 0;

  if (!expectParseResult("valid minimal CT serialization", makeValidCtTransactionBlob(), true)) {
    ++failures;
  }

  {
    CryptoNote::BinaryArray blob;
    appendCtHeader(blob);
    appendVarint(blob, CryptoNote::parameters::CT_MAX_INPUTS + 1);
    if (!expectParseResult("reject oversized CT input count", blob, false)) {
      ++failures;
    }
  }

  {
    CryptoNote::BinaryArray blob;
    appendCtHeader(blob);
    appendVarint(blob, 1);
    blob.push_back(0x04);
    appendVarint(blob, CryptoNote::parameters::CT_CONFIDENTIAL_OUTPUT_AMOUNT);
    appendVarint(blob, CryptoNote::parameters::CT_MAX_RING_SIZE + 1);
    if (!expectParseResult("reject oversized CT ring pubkey count", blob, false)) {
      ++failures;
    }
  }

  {
    CryptoNote::BinaryArray blob;
    appendCtHeader(blob);
    appendVarint(blob, 1);
    appendCtInput(blob, 1, 2);
    if (!expectParseResult("reject mismatched CT ring commitments", blob, false)) {
      ++failures;
    }
  }

  {
    CryptoNote::BinaryArray blob;
    appendValidCtPrefix(blob);
    appendVarint(blob, CryptoNote::parameters::CT_MAX_INPUTS + 1);
    if (!expectParseResult("reject oversized CT signature count", blob, false)) {
      ++failures;
    }
  }

  {
    CryptoNote::BinaryArray blob;
    appendValidCtPrefix(blob);
    appendVarint(blob, 1);
    appendZeros(blob, sizeof(Crypto::EllipticCurveScalar));
    appendVarint(blob, CryptoNote::parameters::CT_MAX_RING_SIZE + 1);
    if (!expectParseResult("reject oversized CT MLSAG ss count", blob, false)) {
      ++failures;
    }
  }

  {
    CryptoNote::BinaryArray blob;
    appendValidCtPrefix(blob);
    appendVarint(blob, 1);
    appendCtSignature(blob, 1);
    appendVarint(blob, CryptoNote::parameters::CT_MAX_OUTPUTS + 1);
    if (!expectParseResult("reject oversized CT proof count", blob, false)) {
      ++failures;
    }
  }

  if (failures != 0) {
    return 1;
  }

  std::cout << "CT serialization bounds tests passed" << std::endl;
  return 0;
}
