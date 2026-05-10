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
    size_t operator()(const ConfidentialInput& txin) const { return 0; } // MLSAG is in tx body
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

  if (tx.version == TRANSACTION_VERSION_CT) {
    // Version 4 (CT): proof body — MLSAG signatures, GK proofs, kernel.
    // These are in Transaction body (not prefix) so getTransactionPrefixHash() excludes them.

    // Per-input MLSAG signatures
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
    return;
  }

  // Version 1 (transparent): per-input ring signatures
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
  serializer(input.ringAmount, "ring_amount");
  serializeBoundedVarintVector(input.ringOutputIndexes, serializer, "ring_offsets",
    CryptoNote::parameters::CT_MAX_RING_SIZE, "ring_offsets");

  // Ring public keys (variable length)
  size_t ringSize = input.ringPubkeys.size();
  if (serializer.type() == ISerializer::OUTPUT) {
    throwIfArrayTooLarge(ringSize, CryptoNote::parameters::CT_MAX_RING_SIZE, "ring_pubkeys");
  }
  serializer.beginArray(ringSize, "ring_pubkeys");
  if (serializer.type() == ISerializer::INPUT) {
    throwIfArrayTooLarge(ringSize, CryptoNote::parameters::CT_MAX_RING_SIZE, "ring_pubkeys");
    input.ringPubkeys.resize(ringSize);
  }
  for (size_t i = 0; i < ringSize; ++i) {
    serializer(input.ringPubkeys[i], "");
  }
  serializer.endArray();

  // Ring commitments
  size_t ringCommitmentSize = input.ringCommitments.size();
  if (serializer.type() == ISerializer::OUTPUT) {
    throwIfArrayTooLarge(ringCommitmentSize, CryptoNote::parameters::CT_MAX_RING_SIZE, "ring_commits");
    if (ringCommitmentSize != ringSize) {
      throw std::runtime_error("Serialization error: ring_commits size does not match ring_pubkeys size");
    }
  }
  serializer.beginArray(ringCommitmentSize, "ring_commits");
  if (serializer.type() == ISerializer::INPUT) {
    throwIfArrayTooLarge(ringCommitmentSize, CryptoNote::parameters::CT_MAX_RING_SIZE, "ring_commits");
    if (ringCommitmentSize != ringSize) {
      throw std::runtime_error("Serialization error: ring_commits size does not match ring_pubkeys size");
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
  serializePod(sig.c0, "c0", serializer);
  size_t ringSize = sig.ss.size();
  if (serializer.type() == ISerializer::OUTPUT) {
    throwIfArrayTooLarge(ringSize, CryptoNote::parameters::CT_MAX_RING_SIZE, "ss");
  }
  serializer.beginArray(ringSize, "ss");
  if (serializer.type() == ISerializer::INPUT) {
    throwIfArrayTooLarge(ringSize, CryptoNote::parameters::CT_MAX_RING_SIZE, "ss");
    sig.ss.resize(ringSize);
  }
  for (size_t i = 0; i < ringSize; ++i) {
    serializePod(sig.ss[i][0], "", serializer);
    serializePod(sig.ss[i][1], "", serializer);
  }
  serializer.endArray();
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
  if (header.majorVersion > BLOCK_MAJOR_VERSION_6) {
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
