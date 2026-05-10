// Copyright (c) 2016-2026, The Karbo developers
//
// Pedersen commitment primitives for confidential transactions.
// Provides: independent generator H, Pedersen commit, subgroup validation.

#pragma once

#include <cstddef>
#include <cstdint>
#include <CryptoTypes.h>

namespace Crypto {

// Independent generator H for Pedersen commitments.
// H = hash_to_point("CN-amount-generator"), precomputed.
// H is NOT the basepoint G; it is chosen so that log_G(H) is unknown.
const EllipticCurvePoint& pedersen_get_H();

// Pedersen commitment: C = v*H + r*G
// v: value scalar (amount encoded as 32-byte little-endian scalar)
// r: blinding factor (random scalar)
// C: output commitment point
// Returns false if the resulting point fails subgroup validation.
bool pedersen_commit(const EllipticCurveScalar& v,
                     const EllipticCurveScalar& r,
                     EllipticCurvePoint& C);

// Subgroup validation: returns true if P is a valid non-identity
// point on the Ed25519 curve in the prime-order subgroup.
bool point_valid_for_pedersen(const EllipticCurvePoint& P);

// CT public keys are used inside subgroup-sensitive MLSAG and output checks.
bool ct_public_key_valid(const PublicKey& key);

} // namespace Crypto
