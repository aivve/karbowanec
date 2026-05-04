// Copyright (c) 2016-2026, The Karbo developers
//
// Transaction balance equation and kernel signature for confidential transactions.
//
// Balance equation (spec Section 12):
//   sum(C_in) - sum(C_out) - fee*H = excess*G
//
// The kernel stores the excess commitment (excess*G) and a Schnorr signature
// proving knowledge of the excess scalar over the transaction hash.
//
// Mixed transparent/CT balance (spec Section 3.3):
//   For pre-fork transparent inputs spent into CT outputs, transparent amounts
//   are converted to implicit commitments
//   amount*H (with zero blinding factor).

#pragma once

#include <cstdint>
#include <vector>
#include <CryptoTypes.h>

namespace Crypto {

// Transaction kernel: proves balance without revealing amounts.
// Stored in the transaction alongside CT signatures.
#pragma pack(push, 1)
struct TransactionKernel {
  EllipticCurvePoint excess;   // excess_commitment = excess * G
  Signature          signature; // Schnorr signature over tx_hash with excess scalar
};
#pragma pack(pop)

// Compute the excess commitment from the balance equation.
//
// Given input commitments, output commitments, and a plaintext fee:
//   result = sum(input_commitments) - sum(output_commitments) - fee*H
//
// For a valid transaction this equals excess*G where excess = sum(r_in) - sum(r_out).
//
// input_commitments:  Pedersen commitments of inputs (C_i = v_i*H + r_i*G)
// output_commitments: Pedersen commitments of outputs (C_j = v_j*H + r_j*G)
// fee:                plaintext fee in atomic units
// result:             [output] the computed excess commitment point
//
// Returns false if any point fails to decode.
bool compute_excess_commitment(
  const EllipticCurvePoint input_commitments[],
  size_t num_inputs,
  const EllipticCurvePoint output_commitments[],
  size_t num_outputs,
  uint64_t fee,
  EllipticCurvePoint& result);

// Verify the transaction balance equation.
//
// Checks that:
//   sum(input_commitments) - sum(output_commitments) - fee*H == kernel.excess
//
// Then verifies the kernel Schnorr signature over tx_hash against the excess point.
//
// Returns true if and only if the balance holds AND the signature is valid.
bool verify_transaction_balance(
  const EllipticCurvePoint input_commitments[],
  size_t num_inputs,
  const EllipticCurvePoint output_commitments[],
  size_t num_outputs,
  uint64_t fee,
  const Hash& tx_hash,
  const TransactionKernel& kernel);

// Sign a transaction kernel.
//
// The sender computes: excess = sum(r_in) - sum(r_out) (private scalar)
// and signs tx_hash with a standard Schnorr signature using excess as the secret key.
// The corresponding public key is excess*G (the excess commitment).
//
// excess_scalar: the private excess value
// tx_hash:       hash of the transaction prefix
// kernel:        [output] filled with excess commitment and signature
//
// Returns false on scalar validation failure.
bool sign_transaction_kernel(
  const EllipticCurveScalar& excess_scalar,
  const Hash& tx_hash,
  TransactionKernel& kernel);

// Build an implicit commitment for a transparent (pre-fork) amount.
//
// For transparent inputs with known plaintext amount, the implicit commitment
// is amount*H + 0*G = amount*H (zero blinding factor).
// The amount must be a non-zero value in atomic units.
//
// Returns false if amount is 0 (would yield identity, rejected downstream)
// or if the point computation fails.
bool transparent_amount_to_commitment(
  uint64_t amount,
  EllipticCurvePoint& commitment);

// Compute the excess scalar from input and output blinding factors.
//
// excess = sum(input_blindings) - sum(output_blindings)
//
// All arithmetic is mod l (the Ed25519 group order).
void compute_excess_scalar(
  const EllipticCurveScalar input_blindings[],
  size_t num_inputs,
  const EllipticCurveScalar output_blindings[],
  size_t num_outputs,
  EllipticCurveScalar& excess);

} // namespace Crypto
