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

#include "TransactionBuilder.h"

#include <algorithm>
#include <cstring>
#include <stdexcept>

#include <Common/BinaryArray.hpp>
#include <Common/StringTools.h>
#include "crypto/crypto.h"
#include "crypto/random.h"
#include "crypto/ct_ecdh.h"
#include "crypto/gk_proof.h"
#include "crypto/mlsag.h"
#include "crypto/transaction_balance.h"
#include "CryptoNoteConfig.h"

extern "C" {
#include "crypto/crypto-ops.h"
#include "crypto/crypto-util.h"
}
#include "CryptoNoteCore/CryptoNoteBasic.h"
#include "CryptoNoteCore/CryptoNoteFormatUtils.h"
#include "CryptoNoteCore/CryptoNoteTools.h"
#include "CryptoNoteCore/TransactionApi.h"
#include "Denominations.h"
#include "Wallet/WalletErrors.h"

namespace CryptoNote {

// ── Transparent (v1) transaction builder ─────────────────────────────────────

std::unique_ptr<ITransaction> buildTransaction(
    std::vector<TxBuildInput>& inputs,
    std::vector<TxBuildOutput>& outputs,
    const Crypto::SecretKey& viewSecretKey,
    const std::string& extra,
    uint64_t unlockTimestamp,
    uint64_t sizeLimit,
    Crypto::SecretKey& txSecretKey) {

  std::unique_ptr<ITransaction> tx = createTransaction();

  // 1. Set unlock time
  tx->setUnlockTime(unlockTimestamp);

  // 2. Add inputs — this generates key images and populates inputs[i].ephKeys
  for (auto& input : inputs) {
    tx->addInput(input.senderKeys, input.keyInfo, input.ephKeys);
  }

  // 3. Generate deterministic transaction keys (must come after inputs, before outputs).
  //    r = Hs(viewSecretKey || inputsHash). Enables tx key recovery for sending proofs.
  //    If viewSecretKey is null, keep the random keypair — original CryptoNote protocol, safe.
  if (viewSecretKey != NULL_SECRET_KEY) {
    tx->generateDeterministicTransactionKeys(viewSecretKey);
  }
  // else: random tx keypair from TransactionImpl() constructor is used (safe, original protocol)

  // 4. Sort outputs ascending by amount (shuffle first for privacy, then stable sort)
  std::shuffle(outputs.begin(), outputs.end(), Random::generator());
  std::sort(outputs.begin(), outputs.end(), [](const TxBuildOutput& a, const TxBuildOutput& b) {
    return a.amount < b.amount;
  });

  for (const auto& out : outputs) {
    tx->addOutput(out.amount, out.destination);
  }

  // 5. Append extra data (payment ID, etc.)
  if (!extra.empty()) {
    tx->appendExtra(Common::asBinaryArray(extra));
  }

  // 6. Sign each input
  size_t i = 0;
  for (const auto& input : inputs) {
    tx->signInputKey(i++, input.keyInfo, input.ephKeys);
  }

  // 7. Enforce size limit if requested
  if (sizeLimit > 0) {
    size_t txSize = tx->getTransactionData().size();
    if (txSize >= sizeLimit) {
      throw std::system_error(make_error_code(error::TRANSACTION_SIZE_TOO_BIG));
    }
  }

  // 8. Extract and return the deterministic secret key
  tx->getTransactionSecretKey(txSecretKey);

  return tx;
}

// ── Confidential (v2 CT) transaction builder ─────────────────────────────────

// computeCTInputsHash is no longer needed — we use getObjectHash(tx.inputs)
// after fully populating the ConfidentialInput structs, which matches the
// hash used by isOurOutgoingTransaction() in TransfersConsumer.cpp.

namespace {

// RAII guard that securely wipes all temporary CT-build secrets when it goes
// out of scope, on both the success and the exception path. Wallet hardening
// only — does not affect consensus serialization or transaction validity.
struct CTBuildSecretCleanup {
  std::vector<Crypto::EllipticCurveScalar>& outputBlindings;
  std::vector<Crypto::EllipticCurveScalar>& pseudoBlindings;
  std::vector<CTBuildInput>& inputs;
  Crypto::EllipticCurveScalar* excessScalar = nullptr;
  KeyPair* txKeyPair = nullptr;

  ~CTBuildSecretCleanup() {
    if (!outputBlindings.empty()) {
      sodium_memzero(outputBlindings.data(),
                     outputBlindings.size() * sizeof(Crypto::EllipticCurveScalar));
    }
    if (!pseudoBlindings.empty()) {
      sodium_memzero(pseudoBlindings.data(),
                     pseudoBlindings.size() * sizeof(Crypto::EllipticCurveScalar));
    }
    for (auto& input : inputs) {
      sodium_memzero(&input.realBlinding, sizeof(input.realBlinding));
      sodium_memzero(&input.spendPrivkey, sizeof(input.spendPrivkey));
    }
    if (excessScalar != nullptr) {
      sodium_memzero(excessScalar, sizeof(*excessScalar));
    }
    if (txKeyPair != nullptr) {
      sodium_memzero(&txKeyPair->secretKey, sizeof(txKeyPair->secretKey));
    }
  }
};

} // namespace

// NOTE: buildConfidentialTransaction() consumes a *temporary* set of CTBuildInput
// descriptors copied from wallet state. The function securely wipes the secret
// fields (realBlinding, spendPrivkey) of each input on both success and
// exception paths via CTBuildSecretCleanup. Callers must not pass references to
// persistent wallet storage — only short-lived build descriptors.
Transaction buildConfidentialTransaction(
    std::vector<CTBuildInput>& inputs,
    std::vector<CTBuildOutput>& outputs,
    const Crypto::SecretKey& viewSecretKey,
    uint64_t fee,
    const std::string& extra,
    Crypto::SecretKey& txSecretKey) {

  if (inputs.empty()) {
    throw std::invalid_argument("CT transaction must have at least one input");
  }
  if (outputs.empty()) {
    throw std::invalid_argument("CT transaction must have at least one output");
  }

  // Validate all output amounts are canonical denominations
  for (const auto& out : outputs) {
    if (denominationIndex(out.amount) < 0) {
      throw std::invalid_argument("CT output amount is not a canonical denomination: "
                                  + std::to_string(out.amount));
    }
  }

  // Validate CT ring metadata consistency.
  for (size_t i = 0; i < inputs.size(); ++i) {
    if (inputs[i].ringPubkeys.empty()) {
      throw std::invalid_argument("CT input has empty ring at index " + std::to_string(i));
    }
    if (inputs[i].ringPubkeys.size() != inputs[i].ringCommitments.size()) {
      throw std::invalid_argument("CT input ring pubkeys/commitments size mismatch at index " + std::to_string(i));
    }
    if (inputs[i].ringPubkeys.size() != inputs[i].ringOutputIndexes.size()) {
      throw std::invalid_argument("CT input ring pubkeys/indexes size mismatch at index " + std::to_string(i));
    }
    if (inputs[i].realIndex >= inputs[i].ringPubkeys.size()) {
      throw std::invalid_argument("CT input real index out of range at index " + std::to_string(i));
    }
  }

  // ── Step 1: Prepare inputs — key images, pseudo-commitments ─────────────────
  std::vector<Crypto::EllipticCurveScalar> pseudoBlindings(inputs.size());
  std::vector<Crypto::EllipticCurvePoint> pseudoCommitments(inputs.size());
  std::vector<Crypto::KeyImage> keyImages(inputs.size());

  // Pre-size outputBlindings so its storage is registered with the cleanup
  // guard before any code path that can throw populates secrets.
  std::vector<Crypto::EllipticCurveScalar> outputBlindings(outputs.size());

  // Install RAII cleanup as early as possible so secrets are wiped on every
  // exit path (success or exception). excessScalar / txKeyPair pointers are
  // set immediately after those locals are declared further down.
  CTBuildSecretCleanup cleanup{outputBlindings, pseudoBlindings, inputs};

  for (size_t i = 0; i < inputs.size(); ++i) {
    // Generate random pseudo-blinding factor
    Crypto::EllipticCurveScalar r_pseudo;
    Random::randomBytes(32, r_pseudo.data);
    sc_reduce32(r_pseudo.data);
    pseudoBlindings[i] = r_pseudo;

    // Compute pseudo-commitment: C' = amount*H + r'*G
    Crypto::PublicKey pseudo_pk;
    bool pcOk = Crypto::pedersen_commit(inputs[i].amount, r_pseudo, pseudo_pk);
    sodium_memzero(&r_pseudo, sizeof(r_pseudo));
    if (!pcOk) {
      throw std::runtime_error("Failed to compute pseudo-commitment for input " + std::to_string(i));
    }
    std::memcpy(&pseudoCommitments[i], &pseudo_pk, 32);

    // Compute key image: I = x * Hp(P_real)
    Crypto::PublicKey realPubkey = inputs[i].ringPubkeys[inputs[i].realIndex];
    Crypto::generate_key_image(realPubkey, inputs[i].spendPrivkey, keyImages[i]);
  }

  // ── Step 2: Assemble inputs into transaction prefix ────────────────────────
  // We must build the full ConfidentialInput structs BEFORE computing the
  // deterministic tx key, so that getObjectHash(tx.inputs) matches the hash
  // used by isOurOutgoingTransaction() in TransfersConsumer.cpp.
  Transaction tx;
  tx.version = TRANSACTION_VERSION_CT;
  tx.unlockTime = 0;  // CT transactions: unlockTime must be 0
  tx.fee = fee;

  tx.inputs.resize(inputs.size());
  for (size_t i = 0; i < inputs.size(); ++i) {
    ConfidentialInput cin;
    cin.ringAmount = inputs[i].ringAmount;
    cin.ringOutputIndexes = absolute_output_offsets_to_relative(inputs[i].ringOutputIndexes);
    cin.ringPubkeys = inputs[i].ringPubkeys;
    cin.ringCommitments = inputs[i].ringCommitments;
    cin.pseudoCommitment = pseudoCommitments[i];
    cin.keyImage = keyImages[i];
    tx.inputs[i] = std::move(cin);
  }

  // ── Step 3: Deterministic tx key from view secret key + inputs hash ────────
  // Uses getObjectHash(tx.inputs) which is the canonical serialization of the
  // fully populated ConfidentialInput structs — identical to what
  // ITransactionReader::getTransactionInputsHash() returns.
  Crypto::Hash inputsHash;
  getObjectHash(tx.inputs, inputsHash);
  KeyPair txKeyPair;
  cleanup.txKeyPair = &txKeyPair;
  if (!generateDeterministicTransactionKeys(inputsHash, viewSecretKey, txKeyPair)) {
    throw std::runtime_error("Failed to generate deterministic transaction keys");
  }
  txSecretKey = txKeyPair.secretKey;

  // Extra: add tx public key + any user extra data
  {
    tx.extra.push_back(0x01);  // TX_EXTRA_TAG_PUBKEY
    const uint8_t* pk_data = reinterpret_cast<const uint8_t*>(&txKeyPair.publicKey);
    tx.extra.insert(tx.extra.end(), pk_data, pk_data + 32);

    if (!extra.empty()) {
      const uint8_t* extra_data = reinterpret_cast<const uint8_t*>(extra.data());
      tx.extra.insert(tx.extra.end(), extra_data, extra_data + extra.size());
    }
  }

  // ── Step 4: Prepare outputs ────────────────────────────────────────────────
  // Shuffle outputs for privacy, then sort by amount (ascending) for determinism
  std::shuffle(outputs.begin(), outputs.end(), Random::generator());
  std::sort(outputs.begin(), outputs.end(), [](const CTBuildOutput& a, const CTBuildOutput& b) {
    return a.amount < b.amount;
  });

  // Per-output state: commitments, stealth keys, denomination indices.
  // outputBlindings was pre-sized above and is owned by the cleanup guard.
  std::vector<Crypto::PublicKey> outputCommitments(outputs.size());
  std::vector<Crypto::PublicKey> outputTargetKeys(outputs.size());
  std::vector<std::array<uint8_t, 8>> outputMaskedAmounts(outputs.size());
  std::vector<int> outputDenomIndices(outputs.size());

  // RAII helper: wipe the per-iteration ECDH shared secret on every exit path.
  struct ScopedKeyDerivationWipe {
    Crypto::KeyDerivation* p;
    ~ScopedKeyDerivationWipe() { if (p) sodium_memzero(p, sizeof(*p)); }
  };

  for (size_t i = 0; i < outputs.size(); ++i) {
    // Derive ECDH shared secret with recipient
    Crypto::KeyDerivation sharedSecret;
    ScopedKeyDerivationWipe sharedSecretWipe{&sharedSecret};

    if (!Crypto::generate_key_derivation(outputs[i].destination.viewPublicKey,
                                          txKeyPair.secretKey, sharedSecret)) {
      throw std::runtime_error("Failed to generate key derivation for output " + std::to_string(i));
    }

    // Derive blinding factor: r = Hs(shared_secret || output_index)
    Crypto::derive_blinding_factor(sharedSecret, i, outputBlindings[i]);

    // Compute Pedersen commitment: C = amount*H + r*G
    if (!Crypto::pedersen_commit(outputs[i].amount, outputBlindings[i], outputCommitments[i])) {
      throw std::runtime_error("Failed to compute Pedersen commitment for output " + std::to_string(i));
    }

    // Derive one-time stealth address: P = Hs(shared_secret || idx)*G + B_spend
    if (!Crypto::derive_public_key(sharedSecret, i, outputs[i].destination.spendPublicKey,
                                    outputTargetKeys[i])) {
      throw std::runtime_error("Failed to derive one-time output key for output " + std::to_string(i));
    }

    // ECDH-mask the amount
    Crypto::MaskedAmount masked;
    Crypto::mask_amount(sharedSecret, i, outputs[i].amount, masked);
    std::memcpy(outputMaskedAmounts[i].data(), masked.data, 8);

    outputDenomIndices[i] = denominationIndex(outputs[i].amount);
  }

  // Build outputs (prefix only — GK proofs go in tx.ctProofs)
  tx.outputs.resize(outputs.size());
  for (size_t i = 0; i < outputs.size(); ++i) {
    ConfidentialOutput cout;
    cout.targetKey = outputTargetKeys[i];
    std::memcpy(&cout.commitment, &outputCommitments[i], 32);
    cout.maskedAmount = outputMaskedAmounts[i];

    TransactionOutput txout;
    txout.amount = 0;  // CT outputs: amount field is 0 (real amount is in commitment)
    txout.target = std::move(cout);
    tx.outputs[i] = std::move(txout);
  }

  // ── Step 4b: Compute prefix hash for Fiat-Shamir binding ──────────────────
  // Proof response fields are in Transaction body (not prefix), so the prefix
  // hash naturally excludes them — no custom hash function needed.
  Crypto::Hash signingHash = getObjectHash(*static_cast<const TransactionPrefix*>(&tx));

  // ── Step 5: Generate GK denomination proofs for each output ────────────────
  tx.ctProofs.resize(outputs.size());
  for (size_t i = 0; i < outputs.size(); ++i) {
    Crypto::GKProof proof;
    Crypto::EllipticCurvePoint commitPoint;
    std::memcpy(&commitPoint, &outputCommitments[i], 32);

    if (!Crypto::gk_prove(commitPoint, outputs[i].amount, outputBlindings[i],
                          static_cast<size_t>(outputDenomIndices[i]),
                          signingHash, proof)) {
      throw std::runtime_error("GK prove failed for output " + std::to_string(i));
    }

    // Copy proof into transaction body
    auto& gkp = tx.ctProofs[i];
    for (size_t j = 0; j < 6; ++j) {
      ge_p3_tobytes(reinterpret_cast<unsigned char*>(&gkp.I[j]), &proof.I[j]);
      ge_p3_tobytes(reinterpret_cast<unsigned char*>(&gkp.A[j]), &proof.A[j]);
      ge_p3_tobytes(reinterpret_cast<unsigned char*>(&gkp.B[j]), &proof.B[j]);
      ge_p3_tobytes(reinterpret_cast<unsigned char*>(&gkp.Q[j]), &proof.Q[j]);
      gkp.z[j] = proof.z[j];
      gkp.za[j] = proof.za[j];
      gkp.zb[j] = proof.zb[j];
    }
    gkp.f = proof.f;
  }

  // ── Step 6: Generate MLSAG ring signatures for each input ──────────────────
  tx.ctSignatures.resize(inputs.size());
  for (size_t i = 0; i < inputs.size(); ++i) {
    Crypto::MLSAGSignature mlsag;
    Crypto::KeyImage ki;

    if (!Crypto::mlsag_sign(
        signingHash,
        inputs[i].ringPubkeys.data(),
        inputs[i].ringCommitments.data(),
        pseudoCommitments[i],
        inputs[i].ringPubkeys.size(),
        inputs[i].realIndex,
        inputs[i].spendPrivkey,
        inputs[i].realBlinding,
        pseudoBlindings[i],
        ki,
        mlsag)) {
      throw std::runtime_error("MLSAG sign failed for input " + std::to_string(i));
    }

    // Update key image in the prefix input
    auto& cin = boost::get<ConfidentialInput>(tx.inputs[i]);
    cin.keyImage = ki;

    // Copy MLSAG signature into transaction body
    tx.ctSignatures[i].c0 = mlsag.c0;
    tx.ctSignatures[i].ss = std::move(mlsag.ss);
  }

  // ── Step 7: Compute excess and sign kernel ─────────────────────────────────
  // excess = sum(pseudo_blindings) - sum(output_blindings)
  // For transparent pre-fork inputs, realBlinding is zero, so pseudo_blinding
  // contributes fully. The balance equation:
  //   sum(C'_in) - sum(C_out) - fee*H = excess*G
  Crypto::EllipticCurveScalar excessScalar;
  cleanup.excessScalar = &excessScalar;
  Crypto::compute_excess_scalar(
      pseudoBlindings.data(), pseudoBlindings.size(),
      outputBlindings.data(), outputBlindings.size(),
      excessScalar);

  Crypto::TransactionKernel cryptoKernel;
  if (!Crypto::sign_transaction_kernel(excessScalar, signingHash, cryptoKernel)) {
    throw std::runtime_error("Failed to sign transaction kernel");
  }

  // Copy to CryptoNote kernel
  tx.kernel.excessCommitment = cryptoKernel.excess;
  std::memcpy(&tx.kernel.sigE, &cryptoKernel.signature, 32);
  std::memcpy(&tx.kernel.sigS, reinterpret_cast<const char*>(&cryptoKernel.signature) + 32, 32);

  // All temporary CT-build secrets (outputBlindings, pseudoBlindings, per-input
  // realBlinding/spendPrivkey, excessScalar, txKeyPair.secretKey) are wiped by
  // CTBuildSecretCleanup's destructor on the way out — including any early
  // exception path above. Wallet hardening only; tx bytes are unchanged.
  return tx;
}

} // namespace CryptoNote
