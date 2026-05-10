// Copyright (c) 2016-2026, The Karbo developers
//
// Transaction balance equation and kernel signature for confidential transactions.

#include "transaction_balance.h"
#include "pedersen.h"
#include "crypto.h"
#include "crypto-ops.h"
#include "hash.h"

#include <cstring>

namespace Crypto {

// ── Helper: encode uint64 as a reduced scalar (little-endian) ────────

static void uint64_to_scalar(uint64_t val, unsigned char out[32]) {
  memset(out, 0, 32);
  for (int i = 0; i < 8; ++i)
    out[i] = static_cast<unsigned char>(val >> (8 * i));
}

// ── Helper: point addition / subtraction using ge_* primitives ───────

// Decode compressed point into ge_p3. Returns false on invalid encoding.
static bool decode_point(const EllipticCurvePoint& P, ge_p3& result) {
  return ge_frombytes_vartime(&result,
    reinterpret_cast<const unsigned char*>(&P)) == 0;
}

// Encode ge_p3 to compressed point.
static void encode_point_p3(const ge_p3& p, EllipticCurvePoint& out) {
  ge_p3_tobytes(reinterpret_cast<unsigned char*>(&out), &p);
}

// Encode ge_p2 to compressed point.
static void encode_point_p2(const ge_p2& p, EllipticCurvePoint& out) {
  ge_tobytes(reinterpret_cast<unsigned char*>(&out), &p);
}

// Add two ge_p3 points: result = A + B (returned as ge_p3)
static void point_add(const ge_p3& A, const ge_p3& B, ge_p3& result) {
  ge_cached B_cached;
  ge_p3_to_cached(&B_cached, &B);
  ge_p1p1 sum;
  ge_add(&sum, &A, &B_cached);
  ge_p1p1_to_p3(&result, &sum);
}

// Subtract: result = A - B (returned as ge_p3)
static void point_sub(const ge_p3& A, const ge_p3& B, ge_p3& result) {
  ge_cached B_cached;
  ge_p3_to_cached(&B_cached, &B);
  ge_p1p1 diff;
  ge_sub(&diff, &A, &B_cached);
  ge_p1p1_to_p3(&result, &diff);
}

// Identity point in ge_p3 representation. The neutral element encodes as
// (x=0, y=1) -> bytes 0x01, 0x00..0x00, which always decodes successfully;
// the return is checked anyway so a future change to ge_frombytes_vartime
// can never silently leave `result` uninitialized.
static bool identity_p3(ge_p3& result) {
  static const unsigned char identity_bytes[32] = {
    0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00
  };
  return ge_frombytes_vartime(&result, identity_bytes) == 0;
}

// ── compute_excess_commitment ────────────────────────────────────────

bool compute_excess_commitment(
  const EllipticCurvePoint input_commitments[],
  size_t num_inputs,
  const EllipticCurvePoint output_commitments[],
  size_t num_outputs,
  uint64_t fee,
  EllipticCurvePoint& result)
{
  // Start with identity
  ge_p3 accumulator;
  if (!identity_p3(accumulator))
    return false;

  // Add all input commitments
  for (size_t i = 0; i < num_inputs; ++i) {
    ge_p3 point;
    if (!decode_point(input_commitments[i], point))
      return false;
    point_add(accumulator, point, accumulator);
  }

  // Subtract all output commitments
  for (size_t i = 0; i < num_outputs; ++i) {
    ge_p3 point;
    if (!decode_point(output_commitments[i], point))
      return false;
    point_sub(accumulator, point, accumulator);
  }

  // Subtract fee*H
  if (fee != 0) {
    // Compute fee*H
    const EllipticCurvePoint& H = pedersen_get_H();
    ge_p3 H_p3;
    if (!decode_point(H, H_p3))
      return false;

    unsigned char fee_scalar[32];
    uint64_to_scalar(fee, fee_scalar);

    // fee*H via ge_scalarmult
    ge_p2 feeH_p2;
    ge_scalarmult(&feeH_p2, fee_scalar, &H_p3);

    // Convert p2 -> compressed -> p3 (round-trip to change representation)
    unsigned char feeH_bytes[32];
    ge_tobytes(feeH_bytes, &feeH_p2);
    ge_p3 feeH_p3;
    if (ge_frombytes_vartime(&feeH_p3, feeH_bytes) != 0)
      return false;

    point_sub(accumulator, feeH_p3, accumulator);
  }

  encode_point_p3(accumulator, result);
  return true;
}

// ── verify_transaction_balance ───────────────────────────────────────

bool verify_transaction_balance(
  const EllipticCurvePoint input_commitments[],
  size_t num_inputs,
  const EllipticCurvePoint output_commitments[],
  size_t num_outputs,
  uint64_t fee,
  const Hash& tx_hash,
  const TransactionKernel& kernel)
{
  // Step 1: Compute expected excess from the balance equation
  EllipticCurvePoint computed_excess;
  if (!compute_excess_commitment(
      input_commitments, num_inputs,
      output_commitments, num_outputs,
      fee, computed_excess))
    return false;

  // Step 2: Check that computed excess matches the kernel's excess commitment
  if (memcmp(&computed_excess, &kernel.excess, 32) != 0)
    return false;

  // Step 3: Verify Schnorr signature over tx_hash using excess as public key
  // The existing check_signature expects a PublicKey, which is the same 32 bytes
  // as EllipticCurvePoint.
  const PublicKey& excess_pubkey =
    reinterpret_cast<const PublicKey&>(kernel.excess);

  if (!ct_public_key_valid(excess_pubkey))
    return false;

  return check_signature(tx_hash, excess_pubkey, kernel.signature);
}

// ── sign_transaction_kernel ──────────────────────────────────────────

bool sign_transaction_kernel(
  const EllipticCurveScalar& excess_scalar,
  const Hash& tx_hash,
  TransactionKernel& kernel)
{
  // Validate the excess scalar is in range
  if (sc_check(reinterpret_cast<const unsigned char*>(&excess_scalar)) != 0)
    return false;

  // Compute excess commitment = excess * G
  ge_p3 excess_point;
  ge_scalarmult_base(&excess_point,
    reinterpret_cast<const unsigned char*>(&excess_scalar));
  encode_point_p3(excess_point, kernel.excess);

  // Sign tx_hash with the excess scalar using the standard Schnorr signature.
  // generate_signature(hash, pub, sec, sig) does:
  //   e = H(hash || pub || k*G), s = k - e*sec
  const PublicKey& excess_pubkey =
    reinterpret_cast<const PublicKey&>(kernel.excess);
  const SecretKey& excess_seckey =
    reinterpret_cast<const SecretKey&>(excess_scalar);

  generate_signature(tx_hash, excess_pubkey, excess_seckey, kernel.signature);
  return true;
}

// ── transparent_amount_to_commitment ─────────────────────────────────

bool transparent_amount_to_commitment(
  uint64_t amount,
  EllipticCurvePoint& commitment)
{
  // amount=0 would yield 0*H = identity. The identity point is rejected by
  // point_valid_for_pedersen() everywhere it is consumed (input commitments,
  // ring commitments, GK proof inputs), so producing it here is never useful
  // and only invites accidental coupling. Consensus also requires KeyOutput
  // amounts to be non-zero, so a transparent ring member with amount=0 cannot
  // exist on chain. Fail fast at the source.
  if (amount == 0) {
    return false;
  }

  // Compute amount*H (blinding factor = 0, so C = amount*H + 0*G = amount*H)
  const EllipticCurvePoint& H = pedersen_get_H();
  ge_p3 H_p3;
  if (ge_frombytes_vartime(&H_p3,
      reinterpret_cast<const unsigned char*>(&H)) != 0)
    return false;

  unsigned char amount_scalar[32];
  uint64_to_scalar(amount, amount_scalar);

  ge_p2 result_p2;
  ge_scalarmult(&result_p2, amount_scalar, &H_p3);
  ge_tobytes(reinterpret_cast<unsigned char*>(&commitment), &result_p2);

  return true;
}

// ── compute_excess_scalar ────────────────────────────────────────────

void compute_excess_scalar(
  const EllipticCurveScalar input_blindings[],
  size_t num_inputs,
  const EllipticCurveScalar output_blindings[],
  size_t num_outputs,
  EllipticCurveScalar& excess)
{
  // excess = sum(input_blindings) - sum(output_blindings)
  // All arithmetic mod l (Ed25519 group order).

  unsigned char sum_in[32], sum_out[32];
  memset(sum_in, 0, 32);
  memset(sum_out, 0, 32);

  for (size_t i = 0; i < num_inputs; ++i) {
    sc_add(sum_in, sum_in,
      reinterpret_cast<const unsigned char*>(&input_blindings[i]));
  }

  for (size_t i = 0; i < num_outputs; ++i) {
    sc_add(sum_out, sum_out,
      reinterpret_cast<const unsigned char*>(&output_blindings[i]));
  }

  sc_sub(reinterpret_cast<unsigned char*>(&excess), sum_in, sum_out);
}

} // namespace Crypto
