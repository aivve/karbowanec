// Copyright (c) 2016-2026, The Karbo developers
//
// Groth-Kohlweiss one-of-many membership proof for confidential transactions.
// Proves knowledge of the opening of one commitment in a ring of N=64
// denomination commitments using a binary decomposition (n=6 bits).
//
// Proof size: 24 points x 32 + 19 scalars x 32 = 1376 bytes.
// I commits to the secret index bits, A/B are the bitness proof commitments,
// and Q commits to the non-leading polynomial coefficients. The degree-6
// leading coefficient (= D[l] = r*G) is absorbed into scalar f.
//
// Reference: Groth & Kohlweiss, "One-out-of-Many Proofs", EUROCRYPT 2015.

#pragma once

#include <cstddef>
#include <cstdint>
#include <CryptoTypes.h>
#include "crypto-ops.h"

namespace Crypto {

static const size_t GK_N = 64;  // ring size (number of denominations)
static const size_t GK_n = 6;   // bit decomposition width (2^6 = 64)

struct GKProof {
  ge_p3 I[6];               // commitments to secret index bits l_j
  ge_p3 A[6];               // bit randomness commitments
  ge_p3 B[6];               // bit value commitments
  ge_p3 Q[6];               // polynomial coefficient commitments (m=0..5)
  EllipticCurveScalar z[6]; // per-bit response scalars
  EllipticCurveScalar za[6];// opening responses for I^x * A
  EllipticCurveScalar zb[6];// opening responses for I^(x-z) * B
  EllipticCurveScalar f;    // final evaluation scalar
};

// Prove that commitment C opens to one of the 64 canonical denominations.
//
// C:                  the Pedersen commitment (v*H + r*G)
// v:                  the committed denomination value (atomic units)
// r:                  the blinding factor
// denomination_index: index in DENOMINATIONS[] (0..63)
// tx_hash:            transaction hash for Fiat-Shamir binding
// proof:              output proof
//
// Returns true on success.
bool gk_prove(const EllipticCurvePoint& C,
              uint64_t v,
              const EllipticCurveScalar& r,
              size_t denomination_index,
              const Hash& tx_hash,
              GKProof& proof);

// Verify a GK one-of-many membership proof.
//
// C:       the Pedersen commitment being proven
// proof:   the GK proof
// tx_hash: transaction hash (must match what the prover used)
//
// Returns true if the proof is valid.
bool gk_verify(const EllipticCurvePoint& C,
               const GKProof& proof,
               const Hash& tx_hash);

} // namespace Crypto
