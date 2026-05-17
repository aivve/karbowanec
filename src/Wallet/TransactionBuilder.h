// Copyright (c) 2012-2016, The CryptoNote developers, The Bytecoin developers
// Copyright (c) 2016-2026, Karbo developers
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

#include <memory>
#include <string>
#include <vector>

#include "CryptoNote.h"
#include "ITransaction.h"
#include "CryptoNoteCore/Account.h"

namespace CryptoNote {

// ── Transparent (v1) transaction builder types ──────────────────────────────

// Input descriptor: holds resolved mixin outputs + sender keys needed to generate key image and sign.
// ephKeys is populated by buildTransaction() during addInput.
struct TxBuildInput {
  TransactionTypes::InputKeyInfo keyInfo;
  AccountKeys senderKeys;
  KeyPair ephKeys;
};

// Output descriptor: a single decomposed amount to a destination address.
// Callers decompose amounts (digitSplitStrategy) before calling buildTransaction().
struct TxBuildOutput {
  AccountPublicAddress destination;
  uint64_t amount;
};

// Build and sign a complete transparent (v1) transaction using the ITransaction interface.
std::unique_ptr<ITransaction> buildTransaction(
    std::vector<TxBuildInput>& inputs,
    std::vector<TxBuildOutput>& outputs,
    const Crypto::SecretKey& viewSecretKey,
    const std::string& extra,
    uint64_t unlockTimestamp,
    uint64_t sizeLimit,
    Crypto::SecretKey& txSecretKey);

// ── Confidential (v2 CT) transaction builder types ──────────────────────────

// One ring member descriptor used during transaction construction.
// Each member is self-describing: its amount bucket and absolute offset are
// independent of other members, allowing mixed transparent/confidential rings.
// For a transparent KeyOutput member, amount is the real on-chain amount; for
// a ConfidentialOutput member, amount is CT_CONFIDENTIAL_OUTPUT_AMOUNT.
struct CTBuildRingMember {
  uint64_t                       amount;       // bucket: real amount or CT sentinel
  uint32_t                       outputIndex;  // absolute index in that bucket
  Crypto::PublicKey              pubkey;       // one-time public key
  Crypto::EllipticCurvePoint     commitment;   // Pedersen commitment (or amount*H for transparent)
};

// CT input: a spent output with its ring members and the sender's secret keys.
//
// `isTransparent` decides the on-wire input shape:
//   true  → emit a KeyInput (legacy ring sig in tx.signatures). Used for
//           transparent shielding into the CT pool. Ring must be all-KeyOutput
//           and all members must share the same amount bucket as `amount`.
//           realBlinding is ignored (always 0 for transparent outputs).
//   false → emit a ConfidentialInput (Triptych in tx.ctSignatures). Used for
//           CT-to-CT or transparent-decoy spend. Ring may be mixed-bucket.
//
// Ring members are NOT required to be sorted on entry — buildConfidentialTransaction
// canonicalises them by (amount, outputIndex) before signing and updates
// realIndex to the corresponding post-sort slot.
struct CTBuildInput {
  std::vector<CTBuildRingMember> ringMembers;  // all members of the ring (one is real)
  size_t                         realIndex;    // index of the real spend in ringMembers (pre-sort)
  Crypto::SecretKey              spendPrivkey; // ephemeral spend key for real input: P_real = x*G
  Crypto::EllipticCurveScalar    realBlinding; // blinding of the real input's commitment (0 for transparent)
  uint64_t                       amount;       // plaintext amount of the real input in atomic units
  bool                           isTransparent = false;
};

// CT output: a single canonical denomination going to a destination address.
struct CTBuildOutput {
  AccountPublicAddress destination;
  uint64_t amount;  // must be a canonical denomination (from DENOMINATIONS[0..63])
};

// Build a complete confidential (v2 CT) transaction.
//
// Construction order:
//   1. Deterministic tx key from viewSecretKey + inputs hash
//   2. For each output: derive blinding factor, compute Pedersen commitment, ECDH-mask amount
//   3. For each input: choose random pseudo-blinding factor, compute pseudo-commitment
//   4. Compute CT signing hash (excludes proof response fields)
//   5. Generate GK denomination proofs for each output
//   6. Generate Triptych spend proofs for each input
//   7. Compute excess scalar, sign kernel Schnorr signature
//
// viewSecretKey:  wallet view secret key for deterministic tx key derivation.
// fee:           plaintext fee in new atomic units.
// extra:         extra data (payment ID, etc.) — encoded in tx.extra.
// txSecretKey:   [out] the deterministic tx secret key for sending-proof.
//
// Returns the fully constructed and signed CryptoNote::Transaction.
// Throws on invalid input (non-canonical denomination, ring size mismatch, etc.).
Transaction buildConfidentialTransaction(
    std::vector<CTBuildInput>& inputs,
    std::vector<CTBuildOutput>& outputs,
    const Crypto::SecretKey& viewSecretKey,
    uint64_t fee,
    const std::string& extra,
    Crypto::SecretKey& txSecretKey);

} // namespace CryptoNote
