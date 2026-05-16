// Copyright (c) 2012-2016, The CryptoNote developers, The Bytecoin developers
// Copyright (c) 2016-2020, The Karbo developers
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

#pragma once

#include <array>
#include <string>
#include <vector>

#include "CryptoTypes.h"
#include "CryptoNote.h"
#include "BlockchainExplorerData.h"
#include <boost/variant.hpp>

namespace CryptoNote {

enum class TransactionRemoveReason : uint8_t 
{ 
  INCLUDED_IN_BLOCK = 0, 
  TIMEOUT = 1
};

struct TransactionOutputToKeyDetails {
  Crypto::PublicKey txOutKey;
};

struct TransactionOutputDetails {
  uint64_t amount;
  uint32_t globalIndex;

  boost::variant<TransactionOutputToKeyDetails> output;
};

struct TransactionOutputReferenceDetails {
  Crypto::Hash transactionHash;
  size_t number;
};

struct TransactionInputGenerateDetails {
  uint32_t height;
};

struct TransactionInputToKeyDetails {
  std::vector<uint32_t> outputIndexes;
  Crypto::KeyImage keyImage;
  uint64_t mixin;
  std::vector<TransactionOutputReferenceDetails> outputs;
};

struct TransactionInputMultisignatureDetails {
  uint32_t signatures;
  TransactionOutputReferenceDetails output;
};

struct TransactionInputDetails {
  uint64_t amount;

  boost::variant<
    TransactionInputGenerateDetails,
    TransactionInputToKeyDetails> input;
};

struct TransactionExtraDetails {
  std::vector<size_t> padding;
  std::vector<Crypto::PublicKey> publicKey; 
  std::vector<std::string> nonce;
  std::vector<uint8_t> raw;
};

struct transactionOutputDetails2 {
  TransactionOutput output;
  uint64_t globalIndex;
};

struct BaseInputDetails {
  BaseInput input;
  uint64_t amount;
};

struct KeyInputDetails {
  KeyInput input;
  uint64_t mixin;
  std::vector<TransactionOutputReferenceDetails> outputs;
};

// Confidential (CT) input detail: amount is hidden, but the ring layout,
// pseudo-commitment and key image are public and useful for explorers.
// ringMembers carries the per-member (amount, outputIndex) tuples needed
// to render mixed transparent/confidential rings; outputs[] holds the
// matching resolved (txHash, outputIndex) pairs in the same order.
struct ConfidentialInputDetails {
  Crypto::KeyImage keyImage;
  Crypto::EllipticCurvePoint pseudoCommitment;
  uint64_t mixin;
  std::vector<RingMemberRef> ringMembers;
  std::vector<TransactionOutputReferenceDetails> outputs;
};

typedef boost::variant<BaseInputDetails, KeyInputDetails, ConfidentialInputDetails> transactionInputDetails2;

struct TransactionExtraDetails2 {
  std::vector<size_t> padding;
  Crypto::PublicKey publicKey;
  BinaryArray nonce;
  BinaryArray raw;
  size_t size = 0;
};

struct TransactionDetails {
  Crypto::Hash hash;
  uint64_t size = 0;
  uint64_t fee = 0;
  uint64_t totalInputsAmount = 0;
  uint64_t totalOutputsAmount = 0;
  uint64_t mixin = 0;     // max ring size across inputs (legacy field)
  uint64_t minMixin = 0;  // min ring size across inputs (CT rings must be a supported power of two: 4, 8, or 16)
  uint64_t unlockTime = 0;
  uint64_t timestamp = 0;
  uint8_t version = 0;
  Crypto::Hash paymentId;
  bool hasPaymentId = false;
  bool inBlockchain = false;
  Crypto::Hash blockHash;
  uint32_t blockHeight = 0;
  TransactionExtraDetails2 extra;
  std::vector<std::vector<Crypto::Signature>> signatures;
  std::vector<transactionInputDetails2> inputs;
  std::vector<transactionOutputDetails2> outputs;

  // CT (v2) proof body. Empty / value-initialized for non-CT transactions.
  std::vector<CTInputSignature> ctSignatures; // per-input Triptych spend proof
  std::vector<CTOutputProof>    ctProofs;     // per-output GK denomination membership
  TransactionKernel             kernel;       // balance-equation excess + Schnorr
};

struct BlockDetails {
  uint8_t majorVersion = 0;
  uint8_t minorVersion = 0;
  uint64_t timestamp = 0;
  Crypto::Hash prevBlockHash;
  Crypto::Hash proofOfWork;
  uint32_t nonce = 0;
  bool isOrphaned = false;
  uint32_t height = 0;
  uint32_t depth = 0;
  Crypto::Hash hash;
  uint64_t difficulty = 0;
  uint64_t cumulativeDifficulty = 0;
  uint64_t reward = 0;
  uint64_t baseReward = 0;
  uint64_t blockSize = 0;
  uint64_t transactionsCumulativeSize = 0;
  uint64_t alreadyGeneratedCoins = 0;
  // Consensus-tracked: total visible value currently locked inside the ECC CT
  // pool at this block height. See Blockchain::getConfidentialSupply.
  uint64_t confidentialSupply = 0;
  // Consensus-tracked: total visible value held by PQ-owned plain outputs.
  // Stubbed at 0 today; will become non-zero once PQ-plain activates.
  uint64_t pqPlainSupply = 0;
  uint64_t alreadyGeneratedTransactions = 0;
  uint64_t sizeMedian = 0;
  uint64_t effectiveSizeMedian = 0;
  double penalty = 0.0;
  uint64_t totalFeeAmount = 0;
  Crypto::Signature minerSignature;
  std::vector<TransactionDetails> transactions;
};

}
