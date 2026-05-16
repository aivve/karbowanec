// Copyright (c) 2016-2026, The Karbo developers
//
// Triptych-style linkable one-out-of-many spend proof for confidential
// transaction inputs. Replaces the linear-size MLSAG with a logarithmic
// (in ring size) Groth-Kohlweiss-descended construction.
//
// Algorithmic references (the C++ implementation here is original; the
// underlying protocol is academic prior art):
//   - Jens Groth and Markulf Kohlweiss, "One-out-of-Many Proofs:
//     Or How to Leak a Secret and Spend a Coin," EUROCRYPT 2015.
//     ePrint 2014/764. Source for the bit-decomposition selector
//     polynomial p_k(X), the Q-polynomial track, and the bit-commitment
//     proof (the I_bits/A/B + z/za/zb response structure).
//   - Sarang Noether, Brandon Goodell, Surae Noether, "Triptych:
//     logarithmic-sized linkable ring signatures with applications,"
//     2020. ePrint 2020/018. Source for the linkable-tag construction
//     and the I-base blinding trick (Q_U[m] = σ_U[m]·I + ψ_U[m]) that
//     lets the response scalar f_U carry x⁻¹ without ever exposing it.
//
// Public statement, per CT input:
//   Ring (P_k, C_k) for k = 0..N-1 with N = 2^n in {4, 8, 16}.
//   Pseudo-output commitment C'.
//   Linking tag (CryptoNote key image) I.
//
//   The prover knows (l, x, z) with:
//     P_l   = x * G                                 (spend authorization)
//     C_l   = C' + z * G                            (commitment balance,
//                                                    z = r_real - r_pseudo)
//     I     = x * Hp(P_l)                           (CryptoNote key image)
//
// The proof reveals nothing about l beyond the ring membership. The key
// image I is consensus-checked elsewhere against the global spent set; the
// proof binds I to the unknown index l through the U-ring (U_k = Hp(P_k))
// using Triptych's blinding-base trick: the U-track's polynomial
// commitments are blinded with I rather than G, which lets the response
// scalar carry (1/x) without ever exposing it or 1/x to the verifier.
//
// Algebra (per ring R ∈ {P, M, U} where M_k := C_k - C'):
//
//   Σ_k p_k(X) · R_k = R_l · X^n + Σ_{m<n} X^m · ψ_R[m]
//
// where p_k is the GK selector polynomial (degree n, leading coefficient
// 1 iff k = l). The prover commits to ψ_R[m] (for m < n) blinded as:
//
//   Q_P[m] = ρ_P[m] · G + ψ_P[m]      (P-ring, G-base — standard GK)
//   Q_M[m] = ρ_M[m] · G + ψ_M[m]      (M-ring, G-base — standard GK)
//   Q_U[m] = σ_U[m] · I + ψ_U[m]      (U-ring, I-base — Triptych trick)
//
// After Fiat-Shamir challenge x_chal, prover responds:
//
//   f_P = x · x_chal^n  −  Σ_m ρ_P[m] · x_chal^m
//   f_M = z · x_chal^n  −  Σ_m ρ_M[m] · x_chal^m
//   f_U = x⁻¹ · x_chal^n − Σ_m σ_U[m] · x_chal^m
//
// Verifier checks three ring identities:
//
//   Σ_k p_k(x_chal) · P_k = f_P · G + Σ_m x_chal^m · Q_P[m]
//   Σ_k p_k(x_chal) · M_k = f_M · G + Σ_m x_chal^m · Q_M[m]
//   Σ_k p_k(x_chal) · U_k = f_U · I + Σ_m x_chal^m · Q_U[m]
//
// The third identity holds iff I = x · Hp(P_l), forcing the linking-tag
// witness x_image and the spend witness x_spend (extracted from f_P) to
// match at index l. Different witnesses would force ψ_U[n] = U_l to a
// different point, contradicting the public Hp(P_l) value at that index.
//
// Soundness sketch (informal — full proof mirrors Groth-Kohlweiss with the
// extra Triptych row): the GK extractor pulls (l, x, z) from any prover
// that satisfies all three equations with non-negligible probability over
// the Fiat-Shamir challenge. Hidden inflation would require z s.t. M_l
// commits to a non-zero value mod L, which would break the M-ring
// extractor — equivalent to breaking the discrete log on G.
//
// Fiat-Shamir transcript (one canonical serialization, NO ad-hoc hashing
// elsewhere). The challenge commits to:
//   - domain separator "Triptych-KarboCT-v1"   (separation from GK/MLSAG)
//   - tx prefix hash (message)                 (binds proof to the tx)
//   - n as one byte                            (ring-size domain split)
//   - every ring pubkey P_k in order
//   - every ring commitment C_k in order
//   - C' (pseudo-output commitment)
//   - I (key image / linking tag)
//   - I_bits[j], A[j], B[j]  for j = 0..n−1   (bit-decomposition commits)
//   - Q_P[m], Q_M[m], Q_U[m] for m = 0..n−1   (poly-coefficient commits)
//
// Independence from the GK denomination proof: separate domain separator,
// separate transcript, separate challenges. The only shared input is the
// tx prefix hash, which both proofs commit to as their tx-context binding.

#pragma once

#include <cstddef>
#include <vector>
#include <CryptoTypes.h>

namespace Crypto {

// Per-input Triptych signature. Vector lengths follow this rule:
//
//   ring_size = 1  (n_bits = 0, Schnorr branch — see below)
//     I_bits, A, B, z, za, zb : empty
//     Q_P, Q_M, Q_U           : exactly 1 entry each (Schnorr nonce commits)
//
//   ring_size ∈ {4, 8, 16}  (n_bits = log2(ring_size); full Triptych)
//     I_bits, A, B, z, za, zb : exactly n_bits entries
//     Q_P, Q_M, Q_U           : exactly n_bits entries
//
// f_P, f_M, f_U are always present (three response scalars).
//
// Ring-size-1 (Schnorr) branch:
//   The polynomial selector degenerates at n=0, so we cannot use the
//   one-out-of-many algebra. Instead the proof reduces to three
//   independent Schnorr proofs (one per relation) sharing the same
//   Fiat-Shamir challenge:
//     P_0 = x·G        — Schnorr over G              ↔  f_P, Q_P[0]
//     M_0 = z·G        — Schnorr over G              ↔  f_M, Q_M[0]
//     I   = x·Hp(P_0)  — Schnorr over Hp(P_0)        ↔  f_U, Q_U[0]
//   The "same x" binding for equations 1 and 3 is implicit: a prover who
//   could pass both with different x's would solve dlog of I in base
//   Hp(P_0). The carve-out exists so v5+ coinbase outputs (which have no
//   privacy expectation since they're publicly mined) can be spent
//   without forcing the wallet to request decoys.
//
// Full Triptych branch:
//   Standard logarithmic linkable one-out-of-many proof; see the
//   protocol comment at the top of this file.
struct TriptychSignature {
  std::vector<EllipticCurvePoint>  I_bits;   // commitments to secret index bits l_j
  std::vector<EllipticCurvePoint>  A;        // bitness aux commitments
  std::vector<EllipticCurvePoint>  B;        // bitness aux commitments
  std::vector<EllipticCurvePoint>  Q_P;      // P-ring polynomial coefficients (G-base)
  std::vector<EllipticCurvePoint>  Q_M;      // M-ring polynomial coefficients (G-base)
  std::vector<EllipticCurvePoint>  Q_U;      // U-ring polynomial coefficients (I-base)
  std::vector<EllipticCurveScalar> z;        // bit-commitment responses
  std::vector<EllipticCurveScalar> za;       // opening responses for x·I_bits + A
  std::vector<EllipticCurveScalar> zb;       // opening responses for (x−z)·I_bits + B
  EllipticCurveScalar              f_P;      // spend witness response
  EllipticCurveScalar              f_M;      // balance witness response
  EllipticCurveScalar              f_U;      // image witness response
};

// Generate a Triptych spend proof for a CT input.
//
// message:         transaction prefix hash (Fiat-Shamir context binding)
// ring_pubkeys:    one-time public keys P_k of ring members (size = ring_size)
// ring_commits:    Pedersen commitments C_k of ring members (size = ring_size)
// pseudo_commit:   pseudo-output commitment C' = v·H + r'·G
// ring_size:       number of ring members; MUST be in {1, 4, 8, 16}.
//                  ring_size = 1 reduces to a Schnorr-style proof (used for
//                  v5+ coinbase carve-out — see TriptychSignature comment).
// true_index:      index l of the real input within the ring
// spend_privkey:   secret spend key x such that P_l = x·G
// real_blinding:   blinding factor r of the real input's commitment C_l
// pseudo_blinding: blinding factor r' of the pseudo-output commitment C'
// key_image:       [out] linking tag I = x · Hp(P_l)
// sig:             [out] Triptych signature (variable arrays sized to log2(ring_size))
//
// Returns false on bad inputs (bad points, index out of range, ring_size not
// a supported power of two, spend_privkey not in [1, L)).
bool triptych_sign(
  const Hash& message,
  const PublicKey ring_pubkeys[],
  const EllipticCurvePoint ring_commits[],
  const EllipticCurvePoint& pseudo_commit,
  size_t ring_size,
  size_t true_index,
  const SecretKey& spend_privkey,
  const EllipticCurveScalar& real_blinding,
  const EllipticCurveScalar& pseudo_blinding,
  KeyImage& key_image,
  TriptychSignature& sig);

// Verify a Triptych spend proof.
//
// Returns true iff:
//   - ring_size is one of the supported powers of two
//   - every response scalar is in canonical range [0, L)
//   - every committed point is in the prime-order subgroup
//   - the three ring identities hold under the Fiat-Shamir challenge
//
// Does NOT check the global spent-key set — that is consensus state and
// stays with the caller (Blockchain validation).
bool triptych_verify(
  const Hash& message,
  const PublicKey ring_pubkeys[],
  const EllipticCurvePoint ring_commits[],
  const EllipticCurvePoint& pseudo_commit,
  size_t ring_size,
  const KeyImage& key_image,
  const TriptychSignature& sig);

// Returns true iff ring_size is supported: 1 (Schnorr branch — see the
// TriptychSignature comment) or 4 / 8 / 16 (full Triptych).
// Exposed so callers (TransactionBuilder, validation) can reject early
// without ever entering the proof path on a malformed shape.
bool triptych_ring_size_supported(size_t ring_size);

// Batched verification of `count` Triptych spend proofs that all share a
// single Fiat-Shamir message (typically the tx prefix hash of one
// transaction). For each proof i, parallel arrays describe its inputs:
//   ring_pubkeys[i]   — pointer to the ring of size ring_sizes[i]
//   ring_commits[i]   — parallel ring of commitment points
//   pseudo_commits[i] — pseudo-output commitment for input i
//   ring_sizes[i]     — must be 1, 4, 8, or 16
//   key_images[i]     — linking tag for input i
//   sigs[i]           — Triptych proof struct
//
// Soundness: per-equation random α coefficients are sampled fresh on
// every call (one per equation per proof), so a prover cannot pre-bias
// the combined point sum. Any single broken equation flips the combined
// sum to non-identity with overwhelming probability (~2⁻²⁵²).
//
// Returns true iff every proof verifies. On false, the caller is
// expected to fall back to per-input triptych_verify() to pinpoint
// which proof failed — the batched path has no per-proof diagnostic.
//
// On invalid structural input (bad ring size, malformed proof shape,
// scalars out of range, points off the prime-order subgroup) the
// function fails fast and returns false without proceeding to the MSM.
bool triptych_verify_batch(
  const Hash& message,
  const PublicKey* const* ring_pubkeys,
  const EllipticCurvePoint* const* ring_commits,
  const EllipticCurvePoint* pseudo_commits,
  const size_t* ring_sizes,
  const KeyImage* key_images,
  const TriptychSignature* sigs,
  size_t count);

} // namespace Crypto
