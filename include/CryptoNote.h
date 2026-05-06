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

// Confidential transaction input (version 2) — prefix portion only.
// Contains the ring of public keys and commitments, a pseudo-output commitment,
// and key image. MLSAG signatures are stored separately in Transaction body.
struct ConfidentialInput {
  uint64_t                                 ringAmount;        // transparent amount bucket of referenced ring members
  std::vector<uint32_t>                    ringOutputIndexes; // relative offsets in the ringAmount bucket
  std::vector<Crypto::PublicKey>           ringPubkeys;       // one-time public keys of ring members
  std::vector<Crypto::EllipticCurvePoint>  ringCommitments;   // Pedersen commitments of ring members
  Crypto::EllipticCurvePoint               pseudoCommitment;  // C' = v*H + r'*G
  Crypto::KeyImage                         keyImage;          // I = x * Hp(P)
};

typedef boost::variant<BaseInput, KeyInput, ConfidentialInput> TransactionInput;

// ---------------------------------------------------------------------------
// Outputs
// ---------------------------------------------------------------------------

struct KeyOutput {
  Crypto::PublicKey key;
};

// Confidential transaction output (version 2) — prefix portion only.
// Contains a Pedersen commitment and masked amount. GK denomination proofs
// are stored separately in Transaction body.
struct ConfidentialOutput {
  Crypto::PublicKey          targetKey;        // One-time stealth address P = Hs(8aR||idx)*G + B  (32 bytes)
  Crypto::EllipticCurvePoint commitment;       // Pedersen commitment C = v*H + r*G  (32 bytes)
  std::array<uint8_t, 8>     maskedAmount;     // ECDH-masked denomination (8 bytes)
};

typedef boost::variant<KeyOutput, ConfidentialOutput> TransactionOutputTarget;

struct TransactionOutput {
  uint64_t amount;
  TransactionOutputTarget target;
};

using TransactionInputs = std::vector<TransactionInput>;

// ---------------------------------------------------------------------------
// CT proof body types (version 2 only — stored in Transaction, not prefix)
// ---------------------------------------------------------------------------

// Per-input MLSAG ring signature (two-row: spend key + commitment difference).
struct CTInputSignature {
  Crypto::EllipticCurveScalar              c0;   // initial challenge
  std::vector<std::array<Crypto::EllipticCurveScalar, 2>> ss; // ss[ring_member][row]
};

// Per-output GK denomination membership proof.
// Proves the committed value is one of the 64 canonical denominations.
//   6 points I + 6 points A + 6 points B + 6 points Q = 768 bytes
//   6 scalars z + 6 scalars za + 6 scalars zb + 1 scalar f = 608 bytes
//   Total = 1376 bytes.
struct CTOutputProof {
  Crypto::EllipticCurvePoint  I[6];  // commitments to secret index bits
  Crypto::EllipticCurvePoint  A[6];  // bit randomness commitments
  Crypto::EllipticCurvePoint  B[6];  // bit value commitments
  Crypto::EllipticCurvePoint  Q[6];  // polynomial coefficient commitments
  Crypto::EllipticCurveScalar z[6];  // per-bit response scalars
  Crypto::EllipticCurveScalar za[6]; // opening responses for I^x * A
  Crypto::EllipticCurveScalar zb[6]; // opening responses for I^(x-z) * B
  Crypto::EllipticCurveScalar f;     // final evaluation scalar
};

// Proves the balance equation: sum(C_in) - sum(C_out) - fee*H = excess*G
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

  // v4 (CT): proof body — separate from prefix so getTransactionPrefixHash() excludes them
  std::vector<CTInputSignature> ctSignatures;  // per-input MLSAG signatures
  std::vector<CTOutputProof>    ctProofs;      // per-output GK denomination proofs
  TransactionKernel             kernel;        // balance proof + Schnorr signature
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
