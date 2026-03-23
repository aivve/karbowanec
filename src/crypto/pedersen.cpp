// Copyright (c) 2016-2026, The Karbo developers
//
// Pedersen commitment primitives for confidential transactions.

#include "pedersen.h"
#include "crypto-ops.h"
#include "hash.h"

#include <cstring>
#include <cassert>

namespace Crypto {

// ── Independent generator H ──────────────────────────────────────────
//
// H = hash_to_point("CN-amount-generator")
// Uses the same hash-to-curve method as hash_data_to_ec in crypto.cpp:
//   1. cn_fast_hash(data) → 32-byte hash
//   2. ge_fromfe_frombytes_vartime → ge_p2
//   3. ge_mul8 (cofactor clearing) → ge_p1p1 → ge_p2
//   4. ge_tobytes → compressed 32-byte point
//
// Precomputed once at startup via static initialization.

static EllipticCurvePoint compute_H() {
  static const char domain[] = "CN-amount-generator";
  Hash h;
  ge_p2 point;
  ge_p1p1 point2;
  EllipticCurvePoint result;

  cn_fast_hash(domain, sizeof(domain) - 1, h);
  ge_fromfe_frombytes_vartime(&point,
    reinterpret_cast<const unsigned char*>(&h));
  ge_mul8(&point2, &point);
  ge_p1p1_to_p2(&point, &point2);
  ge_tobytes(reinterpret_cast<unsigned char*>(&result), &point);
  return result;
}

static const EllipticCurvePoint H_point = compute_H();

const EllipticCurvePoint& pedersen_get_H() {
  return H_point;
}

// ── Subgroup validation ──────────────────────────────────────────────
//
// A point P is valid for Pedersen commitments iff:
//   1. It decodes to a valid curve point (ge_frombytes_vartime succeeds)
//   2. P is not the identity (all-zero encoding, or the canonical
//      encoding of the neutral element)
//   3. 8*P is not the identity (P is in the prime-order subgroup)

static bool is_identity(const unsigned char* bytes) {
  // The identity point in Ed25519 encodes as (x=0, y=1):
  // little-endian: 01 00 00 ... 00
  static const unsigned char identity[32] = {
    0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00
  };
  return memcmp(bytes, identity, 32) == 0;
}

bool point_valid_for_pedersen(const EllipticCurvePoint& P) {
  const unsigned char* p_bytes =
    reinterpret_cast<const unsigned char*>(&P);

  // Must decode to a valid curve point
  ge_p3 point;
  if (ge_frombytes_vartime(&point, p_bytes) != 0) {
    return false;
  }

  // P must not be the identity
  if (is_identity(p_bytes)) {
    return false;
  }

  // 8*P must not be the identity (ensures prime-order subgroup)
  ge_p2 p2;
  ge_p1p1 p1p1;
  ge_p3_to_p2(&p2, &point);
  ge_mul8(&p1p1, &p2);
  ge_p1p1_to_p2(&p2, &p1p1);

  unsigned char eightP[32];
  ge_tobytes(eightP, &p2);
  if (is_identity(eightP)) {
    return false;
  }

  return true;
}

// ── Pedersen commitment ──────────────────────────────────────────────
//
// C = v*H + r*G
//
// ge_scalarmult_base computes r*G → ge_p3
// ge_scalarmult computes v*H → ge_p2
// We then add the two points.

bool pedersen_commit(const EllipticCurveScalar& v,
                     const EllipticCurveScalar& r,
                     EllipticCurvePoint& C) {
  const unsigned char* v_bytes =
    reinterpret_cast<const unsigned char*>(&v);
  const unsigned char* r_bytes =
    reinterpret_cast<const unsigned char*>(&r);

  // Decode H into ge_p3 for ge_scalarmult
  ge_p3 H_p3;
  if (ge_frombytes_vartime(&H_p3,
      reinterpret_cast<const unsigned char*>(&H_point)) != 0) {
    return false;
  }

  // v*H → ge_p2
  ge_p2 vH_p2;
  ge_scalarmult(&vH_p2, v_bytes, &H_p3);

  // r*G → ge_p3
  ge_p3 rG_p3;
  ge_scalarmult_base(&rG_p3, r_bytes);

  // Add: vH + rG
  // Convert vH from ge_p2 to ge_p3 (via tobytes + frombytes round-trip)
  unsigned char vH_bytes[32];
  ge_tobytes(vH_bytes, &vH_p2);
  ge_p3 vH_p3;
  if (ge_frombytes_vartime(&vH_p3, vH_bytes) != 0) {
    return false;
  }

  ge_cached rG_cached;
  ge_p3_to_cached(&rG_cached, &rG_p3);

  ge_p1p1 sum_p1p1;
  ge_add(&sum_p1p1, &vH_p3, &rG_cached);

  ge_p2 sum_p2;
  ge_p1p1_to_p2(&sum_p2, &sum_p1p1);

  unsigned char* C_bytes = reinterpret_cast<unsigned char*>(&C);
  ge_tobytes(C_bytes, &sum_p2);

  // Subgroup validation on the result
  if (!point_valid_for_pedersen(C)) {
    return false;
  }

  return true;
}

} // namespace Crypto
