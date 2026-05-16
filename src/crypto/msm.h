// Copyright (c) 2016-2026, The Karbo developers
//
// Multi-scalar multiplication (MSM) primitives shared by the CT batched
// verifiers (Groth-Kohlweiss denomination proofs and Triptych spend
// proofs).
//
// The verifier collapses each per-proof "list of linear-combination
// equations equal identity" into a single point-sum check by sampling
// fresh random α coefficients (one per equation) and summing α-scaled
// terms. If every individual equation holds, the random-α sum is
// identity; if any single equation is broken, the random-α sum is
// non-identity with overwhelming probability (~2⁻²⁵²).
//
// The MSM dispatch then evaluates Σ s_i · P_i either:
//   - naïvely (sequential ge_scalarmult, one per term — used as the
//     reference path and for small batches),
//   - or via windowed Pippenger (c=4, 16 buckets per window × 64
//     windows; ~3-5× faster at the few-hundreds-to-thousands batch
//     sizes the validator actually sees in production).
//
// The fixed-base scalars on G and H are passed separately because:
//   * ge_scalarmult_base on G uses ref10's precomputed table and is much
//     faster than treating G as a generic term in the MSM.
//   * H is shared across the entire batch, so consolidating its
//     coefficient saves one scalarmult per proof.
// (Pippenger absorbs both into its term list internally — see the
//  implementation in msm.cpp.)

#pragma once

#include <cstddef>
#include <vector>
#include <CryptoTypes.h>

extern "C" {
#include "crypto-ops.h"
}

namespace Crypto {

// One term in a linear combination: scalar * point. The point is kept
// as ge_p3 so the MSM loop doesn't pay a per-term decode cost.
struct MSMTerm {
  EllipticCurveScalar scalar;
  ge_p3 point;
};

// Check whether (Σ_i terms[i].scalar · terms[i].point) + extraG·G + extraH·H
// equals the identity point.
//
// extraG / extraH must be valid 32-byte scalars in canonical range. The
// caller passes H_p3 (the decoded ge_p3 form of the Pedersen H generator)
// so this function does not have to look it up itself.
//
// Returns false only for structural failures (e.g. a term's point fails
// to convert) or when the sum is non-identity. Returns true iff the sum
// is identity.

// Reference path: sequential s·P additions; consolidated G and H last.
// Faster than Pippenger only for very small batches (<~20 terms).
bool msm_naive_sum_is_identity(const std::vector<MSMTerm>& terms,
                               const unsigned char extraG[32],
                               const unsigned char extraH[32],
                               const ge_p3& H_p3);

// Production path: Pippenger windowed-bucket MSM, c=4.
// Faster than naïve at any batch size ≳ 10 terms; the breakeven point is
// well below every realistic CT-tx shape.
bool msm_pippenger_sum_is_identity(const std::vector<MSMTerm>& terms,
                                   const unsigned char extraG[32],
                                   const unsigned char extraH[32],
                                   const ge_p3& H_p3);

} // namespace Crypto
