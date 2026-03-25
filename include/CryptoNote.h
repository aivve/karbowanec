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

#pragma once

#include <array>
#include <vector>
#include <boost/variant.hpp>
#include "android.h"
#include "CryptoTypes.h"

namespace CryptoNote {

// ---------------------------------------------------------------------------
// Inputs
// ---------------------------------------------------------------------------

struct BaseInput {
  uint32_t blockIndex;
};

struct KeyInput {
  uint64_t amount;
  std::vector<uint32_t> outputIndexes;
  Crypto::KeyImage keyImage;
};

// Confidential transaction input (version 4).
// Contains the ring of public keys and commitments, a pseudo-output commitment,
// key image, and a two-row MLSAG ring signature.
struct ConfidentialInput {
  std::vector<Crypto::PublicKey>           ringPubkeys;   // one-time public keys of ring members
  std::vector<Crypto::EllipticCurvePoint>  ringCommitments; // Pedersen commitments of ring members
  Crypto::EllipticCurvePoint               pseudoCommitment; // C' = v*H + r'*G
  Crypto::KeyImage                         keyImage;       // I = x * Hp(P)

  // MLSAG signature
  Crypto::EllipticCurveScalar              mlsagC0;        // initial challenge
  std::vector<std::array<Crypto::EllipticCurveScalar, 2>> mlsagSS; // ss[ring_member][row]
};

typedef boost::variant<BaseInput, KeyInput, ConfidentialInput> TransactionInput;

// ---------------------------------------------------------------------------
// Outputs
// ---------------------------------------------------------------------------

struct KeyOutput {
  Crypto::PublicKey key;
};

// Confidential transaction output (version 4).
// Contains a Pedersen commitment, masked amount, and GK membership proof
// proving the committed value is one of the 64 canonical denominations.
//
// Wire size: 32 (commitment) + 8 (masked_amount) + 768 (GK proof) = 808 bytes.
struct ConfidentialOutput {
  Crypto::EllipticCurvePoint commitment;      // C = v*H + r*G  (32 bytes)
  std::array<uint8_t, 8>    maskedAmount;     // ECDH-masked denomination (8 bytes)

  // GK proof fields (768 bytes total):
  //   6 points A + 6 points B + 6 points Q = 18 * 32 = 576 bytes
  //   6 scalars z + 1 scalar f = 7 * 32 = 224 bytes
  //   Total = 800 bytes
  // Note: spec says 768 bytes with Q[5] (m=1..5), but our implementation uses
  // Q[6] (m=0..5) for 800 bytes. This matches the actual gk_proof.h.
  Crypto::EllipticCurvePoint  gkA[6];  // bit randomness commitments
  Crypto::EllipticCurvePoint  gkB[6];  // bit value commitments
  Crypto::EllipticCurvePoint  gkQ[6];  // polynomial coefficient commitments
  Crypto::EllipticCurveScalar gkZ[6];  // per-bit response scalars
  Crypto::EllipticCurveScalar gkF;     // final evaluation scalar
};

typedef boost::variant<KeyOutput, ConfidentialOutput> TransactionOutputTarget;

struct TransactionOutput {
  uint64_t amount;
  TransactionOutputTarget target;
};

using TransactionInputs = std::vector<TransactionInput>;

// ---------------------------------------------------------------------------
// Transaction kernel (version 4 only)
// ---------------------------------------------------------------------------

// Proves the balance equation: sum(C_in) - sum(C_out) - fee*H = excess*G
// Stored in Transaction (not TransactionPrefix) alongside signatures.
struct TransactionKernel {
  Crypto::EllipticCurvePoint excessCommitment; // excess * G
  Crypto::EllipticCurveScalar sigE;            // Schnorr signature e
  Crypto::EllipticCurveScalar sigS;            // Schnorr signature s
};

// ---------------------------------------------------------------------------
// TransactionPrefix / Transaction
// ---------------------------------------------------------------------------

struct TransactionPrefix {
  uint8_t version;
  uint64_t unlockTime;  // v1-v3: unlock time; v4: must be 0
  TransactionInputs inputs;
  std::vector<TransactionOutput> outputs;
  std::vector<uint8_t> extra;
  uint64_t fee;         // v4 only: plaintext fee in new atomic units
};

struct Transaction : public TransactionPrefix {
  // v1-v3: per-input ring signatures
  std::vector<std::vector<Crypto::Signature>> signatures;
  // v4: transaction kernel (balance proof)
  TransactionKernel kernel;
};

struct AccountPublicAddress {
  Crypto::PublicKey spendPublicKey;
  Crypto::PublicKey viewPublicKey;
};

struct ParentBlock {
  uint8_t majorVersion;
  uint8_t minorVersion;
  Crypto::Hash previousBlockHash;
  uint16_t transactionCount;
  std::vector<Crypto::Hash> baseTransactionBranch;
  Transaction baseTransaction;
  std::vector<Crypto::Hash> blockchainBranch;
};

struct BlockHeader {
  uint8_t majorVersion;
  uint8_t minorVersion;
  uint32_t nonce;
  uint64_t timestamp;
  Crypto::Hash previousBlockHash;
};

struct Block : public BlockHeader {
  ParentBlock parentBlock;
  Transaction baseTransaction;
  Crypto::Signature signature;
  std::vector<Crypto::Hash> transactionHashes;
};

struct AccountKeys {
  AccountPublicAddress address;
  Crypto::SecretKey spendSecretKey;
  Crypto::SecretKey viewSecretKey;
};

struct KeyPair {
  Crypto::PublicKey publicKey;
  Crypto::SecretKey secretKey;
};

using BinaryArray = std::vector<uint8_t>;

}
