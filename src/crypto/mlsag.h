// Copyright (c) 2016-2026, The Karbo developers
//
// MLSAG (Multi-Layered Linkable Spontaneous Anonymous Group) ring signatures
// for confidential transaction inputs.
//
// Two-row MLSAG:
//   Row 1: one-time public keys {P_k}, with key image I = x * Hp(P_l)
//   Row 2: commitment differences {C_k - C'}, where C' is pseudo-output commitment
//
// The signature proves knowledge of (x, z) such that:
//   P_l = x*G           (spend authorization)
//   C_l - C' = z*G      (amounts balance, z = r_real - r_pseudo)
// without revealing the true index l.

#pragma once

#include <cstddef>
#include <vector>
#include <array>
#include <CryptoTypes.h>

namespace Crypto {

struct MLSAGSignature {
  EllipticCurveScalar c0;                              // initial challenge
  std::vector<std::array<EllipticCurveScalar, 2>> ss;  // ss[ring_member][row]
};

// Generate MLSAG ring signature for a CT input.
//
// message:         transaction prefix hash
// ring_pubkeys:    one-time public keys of ring members (size = ring_size)
// ring_commits:    Pedersen commitments of ring members (size = ring_size)
// pseudo_commit:   pseudo-output commitment C' = v*H + r'*G
// ring_size:       number of members in the ring
// true_index:      index of the real input in the ring
// spend_privkey:   secret spend key x such that P_{true_index} = x*G
// real_blinding:   blinding factor r of the real input's commitment
// pseudo_blinding: blinding factor r' of the pseudo-output commitment
// key_image:       [output] key image I = x * Hp(P)
// sig:             [output] MLSAG signature {c_0, ss[ring_size][2]}
//
// Returns false on invalid inputs (bad points, index out of range).
bool mlsag_sign(
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
  MLSAGSignature& sig);

// Verify MLSAG ring signature for a CT input.
//
// Returns true if the signature is valid: the ring closes (c_n == c_0)
// and all scalars are in range.
bool mlsag_verify(
  const Hash& message,
  const PublicKey ring_pubkeys[],
  const EllipticCurvePoint ring_commits[],
  const EllipticCurvePoint& pseudo_commit,
  size_t ring_size,
  const KeyImage& key_image,
  const MLSAGSignature& sig);

} // namespace Crypto
