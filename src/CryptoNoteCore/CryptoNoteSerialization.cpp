// Copyright (c) 2012-2016, The CryptoNote developers, The Bytecoin developers
//
// This file is part of Karbo.
//
// Karbo is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Karbo is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Karbo.  If not, see <http://www.gnu.org/licenses/>.

#include "CryptoNoteSerialization.h"

#include <algorithm>
#include <sstream>
#include <stdexcept>

#include <boost/variant/static_visitor.hpp>
#include <boost/variant/apply_visitor.hpp>

#include "Serialization/ISerializer.h"
#include "Serialization/SerializationOverloads.h"
#include "Serialization/BinaryInputStreamSerializer.h"
#include "Serialization/BinaryOutputStreamSerializer.h"
#include "Serialization/JsonOutputStreamSerializer.h"

#include "Common/StringOutputStream.h"
#include "crypto/crypto.h"

#include "Account.h"
#include "CryptoNoteConfig.h"
#include "CryptoNoteFormatUtils.h"
#include "CryptoNoteTools.h"
#include "TransactionExtra.h"

using namespace Common;

namespace {

using namespace CryptoNote;
using namespace Common;

size_t getSignaturesCount(const TransactionInput& input) {
  struct txin_signature_size_visitor : public boost::static_visitor < size_t > {
    size_t operator()(const BaseInput& txin) const { return 0; }
    size_t operator()(const KeyInput& txin) const { return txin.outputIndexes.size(); }
    size_t operator()(const ConfidentialInput& txin) const { return 0; } // Triptych proof lives in tx body
  };

  return boost::apply_visitor(txin_signature_size_visitor(), input);
}

struct BinaryVariantTagGetter: boost::static_visitor<uint8_t> {
  uint8_t operator()(const CryptoNote::BaseInput) { return  0xff; }
  uint8_t operator()(const CryptoNote::KeyInput) { return  0x2; }
  // Tag 0x3 reserverd for deprecated CryptoNote::MultisignatureInput
  uint8_t operator()(const CryptoNote::ConfidentialInput) { return  0x4; }
  uint8_t operator()(const CryptoNote::KeyOutput) { return  0x2; }
  // Tag 0x3 reserverd for deprecated CryptoNote::MultisignatureOutput
  uint8_t operator()(const CryptoNote::ConfidentialOutput) { return  0x4; }
  uint8_t operator()(const CryptoNote::Transaction) { return  0xcc; }
  uint8_t operator()(const CryptoNote::Block) { return  0xbb; }
};

struct VariantSerializer : boost::static_visitor<> {
  VariantSerializer(CryptoNote::ISerializer& serializer, const std::string& name) : s(serializer), name(name) {}

  template <typename T>
  void operator() (T& param) { s(param, name); }

  CryptoNote::ISerializer& s;
  std::string name;
};

void getVariantValue(CryptoNote::ISerializer& serializer, uint8_t tag, CryptoNote::TransactionInput& in) {
  switch(tag) {
  case 0xff: {
    CryptoNote::BaseInput v;
    serializer(v, "value");
    in = v;
    break;
  }
  case 0x2: {
    CryptoNote::KeyInput v;
    serializer(v, "value");
    in = v;
    break;
  }
  case 0x4: {
    CryptoNote::ConfidentialInput v;
    serializer(v, "value");
    in = v;
    break;
  }
  default:
    throw std::runtime_error("Unknown variant tag");
  }
}

void getVariantValue(CryptoNote::ISerializer& serializer, uint8_t tag, CryptoNote::TransactionOutputTarget& out) {
  switch(tag) {
  case 0x2: {
    CryptoNote::KeyOutput v;
    serializer(v, "data");
    out = v;
    break;
  }
  case 0x4: {
    CryptoNote::ConfidentialOutput v;
    serializer(v, "data");
    out = v;
    break;
  }
  default:
    throw std::runtime_error("Unknown variant tag");
  }
}

template <typename T>
bool serializePod(T& v, Common::StringView name, CryptoNote::ISerializer& serializer) {
  return serializer.binary(&v, sizeof(v), name);
}

bool serializeVarintVector(std::vector<uint32_t>& vector, CryptoNote::ISerializer& serializer, Common::StringView name) {
  size_t size = vector.size();
  
  if (!serializer.beginArray(size, name)) {
    vector.clear();
    return false;
  }

  vector.resize(size);

  for (size_t i = 0; i < size; ++i) {
    serializer(vector[i], "");
  }

  serializer.endArray();
  return true;
}

void throwIfArrayTooLarge(size_t size, size_t maxSize, const char* fieldName) {
  if (size > maxSize) {
    std::ostringstream oss;
    oss << "Serialization error: array '" << fieldName << "' size " << size
        << " exceeds limit " << maxSize;
    throw std::runtime_error(oss.str());
  }
}

bool serializeBoundedVarintVector(std::vector<uint32_t>& vector, CryptoNote::ISerializer& serializer,
                                  Common::StringView name, size_t maxSize, const char* fieldName) {
  size_t size = vector.size();

  if (serializer.type() == CryptoNote::ISerializer::OUTPUT) {
    throwIfArrayTooLarge(size, maxSize, fieldName);
  }

  if (!serializer.beginArray(size, name)) {
    if (serializer.type() == CryptoNote::ISerializer::INPUT) {
      vector.clear();
    }
    return false;
  }

  if (serializer.type() == CryptoNote::ISerializer::INPUT) {
    throwIfArrayTooLarge(size, maxSize, fieldName);
    vector.resize(size);
  }

  for (size_t i = 0; i < size; ++i) {
    serializer(vector[i], "");
  }

  serializer.endArray();
  return true;
}

template <typename T, typename SerializeElement>
bool serializeBoundedVector(std::vector<T>& vector, CryptoNote::ISerializer& serializer, Common::StringView name,
                            size_t maxSize, const char* fieldName, SerializeElement serializeElement) {
  size_t size = vector.size();

  if (serializer.type() == CryptoNote::ISerializer::OUTPUT) {
    throwIfArrayTooLarge(size, maxSize, fieldName);
  }

  if (!serializer.beginArray(size, name)) {
    if (serializer.type() == CryptoNote::ISerializer::INPUT) {
      vector.clear();
    }
    return false;
  }

  if (serializer.type() == CryptoNote::ISerializer::INPUT) {
    throwIfArrayTooLarge(size, maxSize, fieldName);
    vector.resize(size);
  }

  for (size_t i = 0; i < size; ++i) {
    serializeElement(vector[i]);
  }

  serializer.endArray();
  return true;
}

}

namespace Crypto {

bool serialize(PublicKey& pubKey, Common::StringView name, CryptoNote::ISerializer& serializer) {
  return serializePod(pubKey, name, serializer);
}

bool serialize(SecretKey& secKey, Common::StringView name, CryptoNote::ISerializer& serializer) {
  return serializePod(secKey, name, serializer);
}

bool serialize(Hash& h, Common::StringView name, CryptoNote::ISerializer& serializer) {
  return serializePod(h, name, serializer);
}

bool serialize(KeyImage& keyImage, Common::StringView name, CryptoNote::ISerializer& serializer) {
  return serializePod(keyImage, name, serializer);
}

bool serialize(chacha8_iv& chacha, Common::StringView name, CryptoNote::ISerializer& serializer) {
  return serializePod(chacha, name, serializer);
}

bool serialize(Signature& sig, Common::StringView name, CryptoNote::ISerializer& serializer) {
  return serializePod(sig, name, serializer);
}

bool serialize(EllipticCurveScalar& ecScalar, Common::StringView name, CryptoNote::ISerializer& serializer) {
  return serializePod(ecScalar, name, serializer);
}

bool serialize(EllipticCurvePoint& ecPoint, Common::StringView name, CryptoNote::ISerializer& serializer) {
  return serializePod(ecPoint, name, serializer);
}

}

namespace CryptoNote {

void serialize(TransactionPrefix& txP, ISerializer& serializer) {
  serializer(txP.version, "version");

  if (txP.version > CURRENT_TRANSACTION_VERSION && txP.version != TRANSACTION_VERSION_CT) {
    throw std::runtime_error("Wrong transaction version");
  }

  if (txP.version == TRANSACTION_VERSION_CT) {
    // Version 4 (CT): fee is plaintext, unlockTime must be 0
    txP.unlockTime = 0;
    serializer(txP.fee, "fee");
    serializeBoundedVector(txP.inputs, serializer, "vin", CryptoNote::parameters::CT_MAX_INPUTS, "vin",
      [&serializer](TransactionInput& input) { serializer(input, ""); });
    serializeBoundedVector(txP.outputs, serializer, "vout", CryptoNote::parameters::CT_MAX_OUTPUTS, "vout",
      [&serializer](TransactionOutput& output) { serializer(output, ""); });
    serializeAsBinary(txP.extra, "extra", serializer);
  } else {
    // Version 1 (transparent)
    serializer(txP.unlockTime, "unlock_time");
    serializer(txP.inputs, "vin");
    serializer(txP.outputs, "vout");
    serializeAsBinary(txP.extra, "extra", serializer);
  }
}

void serialize(Transaction& tx, ISerializer& serializer) {
  serialize(static_cast<TransactionPrefix&>(tx), serializer);

  // Per-input ring signatures (transparent KeyInput slots only). v2 mixed
  // CT txs use this section for KeyInput shielding signatures alongside
  // ConfidentialInput Triptych proofs in ct_signatures. ConfidentialInput
  // slots write zero bytes here (getSignaturesCount returns 0), so the
  // on-wire byte stream for a pure-CT v2 tx (all ConfidentialInputs) is
  // identical with or without this section running.
  {
    size_t sigSize = tx.inputs.size();

    // ignore base transaction
    if (serializer.type() == ISerializer::INPUT && !(sigSize == 1 && tx.inputs[0].type() == typeid(BaseInput))) {
      tx.signatures.resize(sigSize);
    }

    bool signaturesNotExpected = tx.signatures.empty();
    if (!signaturesNotExpected && tx.inputs.size() != tx.signatures.size()) {
      throw std::runtime_error("Serialization error: unexpected signatures size");
    }

    for (size_t i = 0; i < tx.inputs.size(); ++i) {
      size_t signatureSize = getSignaturesCount(tx.inputs[i]);
      if (signaturesNotExpected) {
        if (signatureSize == 0) {
          continue;
        } else {
          throw std::runtime_error("Serialization error: signatures are not expected");
        }
      }

      if (serializer.type() == ISerializer::OUTPUT) {
        if (signatureSize != tx.signatures[i].size()) {
          throw std::runtime_error("Serialization error: unexpected signatures size");
        }

        for (Crypto::Signature& sig : tx.signatures[i]) {
          serializePod(sig, "", serializer);
        }

      } else {
        std::vector<Crypto::Signature> signatures(signatureSize);
        for (Crypto::Signature& sig : signatures) {
          serializePod(sig, "", serializer);
        }

        tx.signatures[i] = std::move(signatures);
      }
    }
  }

  if (tx.version == TRANSACTION_VERSION_CT) {
    // Version 2 (CT): proof body — Triptych spend proofs, GK denomination proofs, kernel.
    // These are in Transaction body (not prefix) so getTransactionPrefixHash() excludes them.

    // Per-input Triptych spend proofs
    size_t sigCount = tx.ctSignatures.size();
    if (serializer.type() == ISerializer::OUTPUT) {
      throwIfArrayTooLarge(sigCount, CryptoNote::parameters::CT_MAX_INPUTS, "ct_signatures");
    }
    serializer.beginArray(sigCount, "ct_signatures");
    if (serializer.type() == ISerializer::INPUT) {
      throwIfArrayTooLarge(sigCount, CryptoNote::parameters::CT_MAX_INPUTS, "ct_signatures");
      tx.ctSignatures.resize(sigCount);
    }
    for (size_t i = 0; i < sigCount; ++i) {
      serialize(tx.ctSignatures[i], serializer);
    }
    serializer.endArray();

    // Per-output GK denomination proofs
    size_t proofCount = tx.ctProofs.size();
    if (serializer.type() == ISerializer::OUTPUT) {
      throwIfArrayTooLarge(proofCount, CryptoNote::parameters::CT_MAX_OUTPUTS, "ct_proofs");
    }
    serializer.beginArray(proofCount, "ct_proofs");
    if (serializer.type() == ISerializer::INPUT) {
      throwIfArrayTooLarge(proofCount, CryptoNote::parameters::CT_MAX_OUTPUTS, "ct_proofs");
      tx.ctProofs.resize(proofCount);
    }
    for (size_t i = 0; i < proofCount; ++i) {
      serialize(tx.ctProofs[i], serializer);
    }
    serializer.endArray();

    // Kernel (balance proof)
    serialize(tx.kernel, serializer);
  }
}

void serialize(TransactionInput& in, ISerializer& serializer) {
  if (serializer.type() == ISerializer::OUTPUT) {
    BinaryVariantTagGetter tagGetter;
    uint8_t tag = boost::apply_visitor(tagGetter, in);
    serializer.binary(&tag, sizeof(tag), "type");

    VariantSerializer visitor(serializer, "value");
    boost::apply_visitor(visitor, in);
  } else {
    uint8_t tag;
    serializer.binary(&tag, sizeof(tag), "type");

    getVariantValue(serializer, tag, in);
  }
}

void serialize(BaseInput& gen, ISerializer& serializer) {
  serializer(gen.blockIndex, "height");
}

void serialize(KeyInput& key, ISerializer& serializer) {
  serializer(key.amount, "amount");
  serializeVarintVector(key.outputIndexes, serializer, "key_offsets");
  serializer(key.keyImage, "k_image");
}

void serialize(TransactionInputs & inputs, ISerializer & serializer) {
  serializer(inputs, "vin");
}

void serialize(TransactionOutput& output, ISerializer& serializer) {
  serializer(output.amount, "amount");
  serializer(output.target, "target");
}

void serialize(TransactionOutputTarget& output, ISerializer& serializer) {
  if (serializer.type() == ISerializer::OUTPUT) {
    BinaryVariantTagGetter tagGetter;
    uint8_t tag = boost::apply_visitor(tagGetter, output);
    serializer.binary(&tag, sizeof(tag), "type");

    VariantSerializer visitor(serializer, "data");
    boost::apply_visitor(visitor, output);
  } else {
    uint8_t tag;
    serializer.binary(&tag, sizeof(tag), "type");

    getVariantValue(serializer, tag, output);
  }
}

void serialize(KeyOutput& key, ISerializer& serializer) {
  serializer(key.key, "key");
}

void serialize(ConfidentialInput& input, ISerializer& serializer) {
  // Per-member ring references: amount + absolute outputIndex per ring slot.
  // The single ringAmount/ringOutputIndexes from the previous schema is gone
  // so CT inputs can mix transparent and confidential ring members.
  size_t ringSize = input.ringMembers.size();
  if (serializer.type() == ISerializer::OUTPUT) {
    throwIfArrayTooLarge(ringSize, CryptoNote::parameters::CT_MAX_RING_SIZE, "ring_members");
  }
  serializer.beginArray(ringSize, "ring_members");
  if (serializer.type() == ISerializer::INPUT) {
    throwIfArrayTooLarge(ringSize, CryptoNote::parameters::CT_MAX_RING_SIZE, "ring_members");
    input.ringMembers.resize(ringSize);
  }
  for (size_t i = 0; i < ringSize; ++i) {
    // Each element is a (amount, offset) struct with two differently-typed
    // fields. The KV-binary array protocol pins one element type per array
    // from the first write, so the only safe shape for heterogeneous
    // elements is OBJECT — wrap each member in an explicit object scope.
    // beginObject/endObject are no-ops on the binary stream serializer
    // (LMDB / on-chain), so the wire bytes for the stored tx blob are
    // unchanged; this only fixes KV-binary RPC round-trip.
    serializer.beginObject("");
    serializer(input.ringMembers[i].amount, "amount");
    serializer(input.ringMembers[i].outputIndex, "offset");
    serializer.endObject();
  }
  serializer.endArray();

  // Ring public keys (variable length, must match ringMembers count)
  size_t ringPubkeySize = input.ringPubkeys.size();
  if (serializer.type() == ISerializer::OUTPUT) {
    throwIfArrayTooLarge(ringPubkeySize, CryptoNote::parameters::CT_MAX_RING_SIZE, "ring_pubkeys");
    if (ringPubkeySize != ringSize) {
      throw std::runtime_error("Serialization error: ring_pubkeys size does not match ring_members size");
    }
  }
  serializer.beginArray(ringPubkeySize, "ring_pubkeys");
  if (serializer.type() == ISerializer::INPUT) {
    throwIfArrayTooLarge(ringPubkeySize, CryptoNote::parameters::CT_MAX_RING_SIZE, "ring_pubkeys");
    if (ringPubkeySize != ringSize) {
      throw std::runtime_error("Serialization error: ring_pubkeys size does not match ring_members size");
    }
    input.ringPubkeys.resize(ringPubkeySize);
  }
  for (size_t i = 0; i < ringPubkeySize; ++i) {
    serializer(input.ringPubkeys[i], "");
  }
  serializer.endArray();

  // Ring commitments (must match ringMembers count)
  size_t ringCommitmentSize = input.ringCommitments.size();
  if (serializer.type() == ISerializer::OUTPUT) {
    throwIfArrayTooLarge(ringCommitmentSize, CryptoNote::parameters::CT_MAX_RING_SIZE, "ring_commits");
    if (ringCommitmentSize != ringSize) {
      throw std::runtime_error("Serialization error: ring_commits size does not match ring_members size");
    }
  }
  serializer.beginArray(ringCommitmentSize, "ring_commits");
  if (serializer.type() == ISerializer::INPUT) {
    throwIfArrayTooLarge(ringCommitmentSize, CryptoNote::parameters::CT_MAX_RING_SIZE, "ring_commits");
    if (ringCommitmentSize != ringSize) {
      throw std::runtime_error("Serialization error: ring_commits size does not match ring_members size");
    }
    input.ringCommitments.resize(ringCommitmentSize);
  }
  for (size_t i = 0; i < ringCommitmentSize; ++i) {
    serializePod(input.ringCommitments[i], "", serializer);
  }
  serializer.endArray();

  // Pseudo-output commitment
  serializePod(input.pseudoCommitment, "pseudo_commit", serializer);

  // Key image
  serializer(input.keyImage, "k_image");
}

void serialize(ConfidentialOutput& output, ISerializer& serializer) {
  // One-time stealth address (32 bytes)
  serializer(output.targetKey, "target_key");

  // Pedersen commitment (32 bytes)
  serializePod(output.commitment, "commitment", serializer);

  // Masked amount (8 bytes)
  serializer.binary(output.maskedAmount.data(), 8, "masked_amount");
}

void serialize(CTInputSignature& sig, ISerializer& serializer) {
  // Triptych signature shape, controlled by a single header byte n:
  //
  //   n = 0xFF (empty slot — matching tx.inputs[i] is a v2 KeyInput, whose
  //            authorization lives in tx.signatures[i] as a legacy ring sig).
  //     All vectors empty, no body bytes after the header.
  //     wire: 1 byte
  //
  //   n = 0  (ring_size = 1, Schnorr branch — v5+ coinbase carve-out)
  //     I_bits / A / B / z / za / zb : empty
  //     Q_P / Q_M / Q_U              : 1 entry each (Schnorr nonce commits)
  //     f_P, f_M, f_U                : 3 scalars
  //     wire: 1 + 3×32 + 3×32 = 193 bytes
  //
  //   n ∈ {2, 3, 4}  (ring_size ∈ {4, 8, 16}, full Triptych)
  //     I_bits / A / B               : n entries each
  //     Q_P / Q_M / Q_U              : n entries each
  //     z / za / zb                  : n entries each
  //     f_P, f_M, f_U                : 3 scalars
  //     wire: 1 + 6n×32 + (3n+3)×32 bytes
  //
  // n = 1 is intentionally reserved as invalid: a 2-member ring would
  // pin the spend to one of two candidates with one decoy, which is
  // weaker than either the Schnorr branch (no privacy claim) or the
  // full Triptych floor at n=2 (3 decoys). Pinning n away from 1 keeps
  // wallet behavior consistent with consensus.
  //
  // Cross-validation that n matches the matching ConfidentialInput's
  // ring_size, and that empty (n=0xFF) slots align with KeyInputs,
  // lives in Blockchain::checkConfidentialTransaction and the semantic
  // check in Core.cpp; this serializer enforces only the on-wire shape.

  constexpr uint8_t kEmptySlot = 0xFF;

  auto n_bits_for = [](uint8_t n) -> size_t {
    // I_bits/A/B/z/za/zb length for a given header byte.
    return (n == 0) ? 0 : static_cast<size_t>(n);
  };
  auto n_q_for = [](uint8_t n) -> size_t {
    // Q_P/Q_M/Q_U length.
    return (n == 0) ? 1 : static_cast<size_t>(n);
  };

  if (dynamic_cast<JsonOutputStreamSerializer*>(&serializer) != nullptr) {
    // JSON: emit n explicitly, named arrays for the rest. The decoder
    // path uses the binary one; JSON is read-only by explorer / RPC.
    //   n = 0xFF → empty slot (matching tx.inputs[i] is a v2 KeyInput)
    //   n = 0    → Schnorr branch (ring size 1, q_len=1, bits_len=0)
    //   n ∈ {2,3,4} → full Triptych (ring size 4/8/16)
    uint8_t n_json;
    if (sig.I_bits.empty() && sig.Q_P.empty()) {
      n_json = kEmptySlot;
    } else if (sig.I_bits.empty()) {
      n_json = 0;
    } else {
      n_json = static_cast<uint8_t>(sig.I_bits.size());
    }
    serializer(n_json, "n");

    auto emitPointArray = [&](std::vector<Crypto::EllipticCurvePoint>& arr,
                              Common::StringView name) {
      size_t size = arr.size();
      serializer.beginArray(size, name);
      for (auto& p : arr) serializePod(p, "", serializer);
      serializer.endArray();
    };
    auto emitScalarArray = [&](std::vector<Crypto::EllipticCurveScalar>& arr,
                               Common::StringView name) {
      size_t size = arr.size();
      serializer.beginArray(size, name);
      for (auto& s : arr) serializePod(s, "", serializer);
      serializer.endArray();
    };

    emitPointArray(sig.I_bits, "I_bits");
    emitPointArray(sig.A,      "A");
    emitPointArray(sig.B,      "B");
    emitPointArray(sig.Q_P,    "Q_P");
    emitPointArray(sig.Q_M,    "Q_M");
    emitPointArray(sig.Q_U,    "Q_U");
    emitScalarArray(sig.z,  "z");
    emitScalarArray(sig.za, "za");
    emitScalarArray(sig.zb, "zb");
    serializePod(sig.f_P, "f_P", serializer);
    serializePod(sig.f_M, "f_M", serializer);
    serializePod(sig.f_U, "f_U", serializer);
    return;
  }

  uint8_t n = 0;
  if (serializer.type() == ISerializer::OUTPUT) {
    // Determine n from the proof shape. A fully-empty CTInputSignature means
    // the matching tx.inputs[i] is a v2 KeyInput; we emit only the sentinel
    // header (1 byte), no body.
    const size_t bits_len = sig.I_bits.size();
    const size_t q_len    = sig.Q_P.size();
    if (bits_len == 0 && q_len == 0 &&
        sig.A.empty()  && sig.B.empty()  &&
        sig.Q_M.empty() && sig.Q_U.empty() &&
        sig.z.empty()  && sig.za.empty() && sig.zb.empty()) {
      n = kEmptySlot;
    } else if (bits_len == 0 && q_len == 1) {
      n = 0;
    } else if ((bits_len == 2 || bits_len == 3 || bits_len == 4) && q_len == bits_len) {
      n = static_cast<uint8_t>(bits_len);
    } else {
      throw std::runtime_error("CTInputSignature: invalid shape on serialize");
    }
    if (n != kEmptySlot) {
      const size_t expected_bits = n_bits_for(n);
      const size_t expected_q    = n_q_for(n);
      if (sig.A.size()   != expected_bits || sig.B.size()   != expected_bits ||
          sig.Q_P.size() != expected_q    || sig.Q_M.size() != expected_q    || sig.Q_U.size() != expected_q ||
          sig.z.size()   != expected_bits || sig.za.size()  != expected_bits || sig.zb.size()  != expected_bits) {
        throw std::runtime_error("CTInputSignature: vector length mismatch on serialize");
      }
    }
  }
  serializer.binary(&n, sizeof(n), "n");
  if (serializer.type() == ISerializer::INPUT) {
    if (n == kEmptySlot) {
      sig.I_bits.clear();
      sig.A.clear(); sig.B.clear();
      sig.Q_P.clear(); sig.Q_M.clear(); sig.Q_U.clear();
      sig.z.clear(); sig.za.clear(); sig.zb.clear();
      std::memset(&sig.f_P, 0, sizeof(sig.f_P));
      std::memset(&sig.f_M, 0, sizeof(sig.f_M));
      std::memset(&sig.f_U, 0, sizeof(sig.f_U));
      return;
    }
    if (n != 0 && n != 2 && n != 3 && n != 4) {
      throw std::runtime_error("CTInputSignature: unsupported proof size on deserialize");
    }
    const size_t bits_len = n_bits_for(n);
    const size_t q_len    = n_q_for(n);
    sig.I_bits.resize(bits_len);
    sig.A.resize(bits_len);
    sig.B.resize(bits_len);
    sig.Q_P.resize(q_len);
    sig.Q_M.resize(q_len);
    sig.Q_U.resize(q_len);
    sig.z.resize(bits_len);
    sig.za.resize(bits_len);
    sig.zb.resize(bits_len);
  }

  if (n == kEmptySlot) {
    return; // OUTPUT path for an empty slot: header only, no body bytes.
  }

  const size_t bits_len = n_bits_for(n);
  const size_t q_len    = n_q_for(n);

  for (size_t i = 0; i < bits_len; ++i) serializePod(sig.I_bits[i], "", serializer);
  for (size_t i = 0; i < bits_len; ++i) serializePod(sig.A[i],      "", serializer);
  for (size_t i = 0; i < bits_len; ++i) serializePod(sig.B[i],      "", serializer);
  for (size_t i = 0; i < q_len;    ++i) serializePod(sig.Q_P[i],    "", serializer);
  for (size_t i = 0; i < q_len;    ++i) serializePod(sig.Q_M[i],    "", serializer);
  for (size_t i = 0; i < q_len;    ++i) serializePod(sig.Q_U[i],    "", serializer);
  for (size_t i = 0; i < bits_len; ++i) serializePod(sig.z[i],      "", serializer);
  for (size_t i = 0; i < bits_len; ++i) serializePod(sig.za[i],     "", serializer);
  for (size_t i = 0; i < bits_len; ++i) serializePod(sig.zb[i],     "", serializer);
  serializePod(sig.f_P, "", serializer);
  serializePod(sig.f_M, "", serializer);
  serializePod(sig.f_U, "", serializer);
}

void serialize(CTOutputProof& proof, ISerializer& serializer) {
  // The proof body is consensus-critical: the binary form is fixed at 6 raw
  // points/scalars per field with no length prefix, so any signed/hashed bytes
  // line up with what verifiers reconstruct. JSON output, however, needs named
  // arrays so RPC consumers (block explorer) can read the fields. Branch on
  // serializer type rather than retrofitting beginArray() into the binary path,
  // which would change the on-disk layout.
  if (dynamic_cast<JsonOutputStreamSerializer*>(&serializer) != nullptr) {
    auto emitPointArray = [&](Crypto::EllipticCurvePoint (&arr)[6], Common::StringView name) {
      size_t size = 6;
      serializer.beginArray(size, name);
      for (size_t i = 0; i < 6; ++i) serializePod(arr[i], "", serializer);
      serializer.endArray();
    };
    auto emitScalarArray = [&](Crypto::EllipticCurveScalar (&arr)[6], Common::StringView name) {
      size_t size = 6;
      serializer.beginArray(size, name);
      for (size_t i = 0; i < 6; ++i) serializePod(arr[i], "", serializer);
      serializer.endArray();
    };
    emitPointArray(proof.I, "I");
    emitPointArray(proof.A, "A");
    emitPointArray(proof.B, "B");
    emitPointArray(proof.Q, "Q");
    emitScalarArray(proof.z, "z");
    emitScalarArray(proof.za, "za");
    emitScalarArray(proof.zb, "zb");
    serializePod(proof.f, "f", serializer);
    return;
  }

  for (size_t i = 0; i < 6; ++i) {
    serializePod(proof.I[i], "", serializer);
  }
  for (size_t i = 0; i < 6; ++i) {
    serializePod(proof.A[i], "", serializer);
  }
  for (size_t i = 0; i < 6; ++i) {
    serializePod(proof.B[i], "", serializer);
  }
  for (size_t i = 0; i < 6; ++i) {
    serializePod(proof.Q[i], "", serializer);
  }
  for (size_t i = 0; i < 6; ++i) {
    serializePod(proof.z[i], "", serializer);
  }
  for (size_t i = 0; i < 6; ++i) {
    serializePod(proof.za[i], "", serializer);
  }
  for (size_t i = 0; i < 6; ++i) {
    serializePod(proof.zb[i], "", serializer);
  }
  serializePod(proof.f, "", serializer);
}

void serialize(TransactionKernel& kernel, ISerializer& serializer) {
  serializePod(kernel.excessCommitment, "excess", serializer);
  serializePod(kernel.sigE, "sig_e", serializer);
  serializePod(kernel.sigS, "sig_s", serializer);
}

void serialize(ParentBlockSerializer& pbs, ISerializer& serializer) {
  serializer(pbs.m_parentBlock.majorVersion, "majorVersion");

  serializer(pbs.m_parentBlock.minorVersion, "minorVersion");
  serializer(pbs.m_timestamp, "timestamp");
  serializer(pbs.m_parentBlock.previousBlockHash, "prevId");
  serializer.binary(&pbs.m_nonce, sizeof(pbs.m_nonce), "nonce");

  if (pbs.m_hashingSerialization) {
    Crypto::Hash minerTxHash;
    if (!getObjectHash(pbs.m_parentBlock.baseTransaction, minerTxHash)) {
      throw std::runtime_error("Get transaction hash error");
    }

    Crypto::Hash merkleRoot;
    Crypto::tree_hash_from_branch(pbs.m_parentBlock.baseTransactionBranch.data(), pbs.m_parentBlock.baseTransactionBranch.size(), minerTxHash, 0, merkleRoot);

    serializer(merkleRoot, "merkleRoot");
  }

  uint64_t txNum = static_cast<uint64_t>(pbs.m_parentBlock.transactionCount);
  serializer(txNum, "numberOfTransactions");
  pbs.m_parentBlock.transactionCount = static_cast<uint16_t>(txNum);
  if (pbs.m_parentBlock.transactionCount < 1) {
    throw std::runtime_error("Wrong transactions number");
  }

  if (pbs.m_headerOnly) {
    return;
  }

  size_t branchSize = Crypto::tree_depth(pbs.m_parentBlock.transactionCount);
  if (serializer.type() == ISerializer::OUTPUT) {
    if (pbs.m_parentBlock.baseTransactionBranch.size() != branchSize) {
      throw std::runtime_error("Wrong miner transaction branch size");
    }
  } else {
    pbs.m_parentBlock.baseTransactionBranch.resize(branchSize);
  }

//  serializer(m_parentBlock.baseTransactionBranch, "baseTransactionBranch");
  //TODO: Make arrays with computable size! This code won't work with json serialization!
  for (Crypto::Hash& hash: pbs.m_parentBlock.baseTransactionBranch) {
    serializer(hash, "");
  }

  serializer(pbs.m_parentBlock.baseTransaction, "minerTx");

  TransactionExtraMergeMiningTag mmTag;
  if (!getMergeMiningTagFromExtra(pbs.m_parentBlock.baseTransaction.extra, mmTag)) {
    throw std::runtime_error("Can't get extra merge mining tag");
  }

  if (mmTag.depth > 8 * sizeof(Crypto::Hash)) {
    throw std::runtime_error("Wrong merge mining tag depth");
  }

  if (serializer.type() == ISerializer::OUTPUT) {
    if (mmTag.depth != pbs.m_parentBlock.blockchainBranch.size()) {
      throw std::runtime_error("Blockchain branch size must be equal to merge mining tag depth");
    }
  } else {
    pbs.m_parentBlock.blockchainBranch.resize(mmTag.depth);
  }

//  serializer(m_parentBlock.blockchainBranch, "blockchainBranch");
  //TODO: Make arrays with computable size! This code won't work with json serialization!
  for (Crypto::Hash& hash: pbs.m_parentBlock.blockchainBranch) {
    serializer(hash, "");
  }
}

void serializeBlockHeader(BlockHeader& header, ISerializer& serializer) {
  serializer(header.majorVersion, "major_version");
  if (header.majorVersion > BLOCK_MAJOR_VERSION_7) {
    throw std::runtime_error("Wrong major version");
  }

  serializer(header.minorVersion, "minor_version");
  
  if (header.majorVersion == BLOCK_MAJOR_VERSION_2 || header.majorVersion == BLOCK_MAJOR_VERSION_3) {
    serializer(header.previousBlockHash, "prev_id");
  } else if (header.majorVersion == BLOCK_MAJOR_VERSION_1 || header.majorVersion >= BLOCK_MAJOR_VERSION_4) {
    serializer(header.timestamp, "timestamp");
    serializer(header.previousBlockHash, "prev_id");
    serializer.binary(&header.nonce, sizeof(header.nonce), "nonce");
  }
  else {
    throw std::runtime_error("Wrong major version");
  }
}

void serialize(BlockHeader& header, ISerializer& serializer) {
  serializeBlockHeader(header, serializer);
}

void serialize(Block& block, ISerializer& serializer) {
  serializeBlockHeader(block, serializer);

  if (block.majorVersion >= BLOCK_MAJOR_VERSION_5) {
    serializer(block.signature, "signature");
  }

  if (block.majorVersion == BLOCK_MAJOR_VERSION_2 || block.majorVersion == BLOCK_MAJOR_VERSION_3) {
    auto parentBlockSerializer = makeParentBlockSerializer(block, false, false);
    serializer(parentBlockSerializer, "parent_block");
  }

  serializer(block.baseTransaction, "miner_tx");
  serializer(block.transactionHashes, "tx_hashes");
}

void serialize(AccountPublicAddress& address, ISerializer& serializer) {
  serializer(address.spendPublicKey, "m_spend_public_key");
  serializer(address.viewPublicKey, "m_view_public_key");
}

void serialize(AccountKeys& keys, ISerializer& s) {
  s(keys.address, "m_account_address");
  s(keys.spendSecretKey, "m_spend_secret_key");
  s(keys.viewSecretKey, "m_view_secret_key");
}

void doSerialize(TransactionExtraMergeMiningTag& tag, ISerializer& serializer) {
  uint64_t depth = static_cast<uint64_t>(tag.depth);
  serializer(depth, "depth");
  tag.depth = static_cast<size_t>(depth);
  serializer(tag.merkleRoot, "merkle_root");
}

void serialize(TransactionExtraMergeMiningTag& tag, ISerializer& serializer) {
  if (serializer.type() == ISerializer::OUTPUT) {
    std::string field;
    StringOutputStream os(field);
    BinaryOutputStreamSerializer output(os);
    doSerialize(tag, output);
    serializer(field, "");
  } else {
    std::string field;
    serializer(field, "");
    MemoryInputStream stream(field.data(), field.size());
    BinaryInputStreamSerializer input(stream);
    doSerialize(tag, input);
  }
}

void serialize(KeyPair& keyPair, ISerializer& serializer) {
  serializer(keyPair.secretKey, "secret_key");
  serializer(keyPair.publicKey, "public_key");
}


} //namespace CryptoNote
