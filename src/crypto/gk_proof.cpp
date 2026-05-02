// Copyright (c) 2016-2026, The Karbo developers
//
// Groth-Kohlweiss one-of-many membership proof implementation.
//
// Protocol overview (Fiat-Shamir non-interactive variant):
//
// Public:  Ring of derived commitments D[k] = C - V[k]*H for k=0..63
// Secret:  Index l and blinding r such that D[l] = r*G
//
// Round 1: Prover commits to bit decomposition of l and polynomial coefficients
// Round 2: Fiat-Shamir challenge x = H(domain || tx_hash || D[] || I[] || A[] || B[] || Q[])
// Round 3: Prover computes response scalars z[j], za[j], zb[j], and f
//
// Selector polynomial for index k:
//   p_k(x) = product_{j=0}^{5} (bit_j(k)==1 ? z[j] : x-z[j])
//
// Key property: sum_k p_{k,6}*D[k] = D[l] = r*G (only the secret index
// contributes to the leading coefficient).
//
// Q[m] commits to sum_k p_{k,m}*D[k] for m=0..5 with fresh rho[m] blindings.
// The degree-6 coefficient is handled by the scalar f.
//
// Verification: sum_k p_k(x)*D[k] == f*G + sum_{m=0}^{5} x^m * Q[m]

#include "gk_proof.h"
#include "pedersen.h"
#include "hash.h"
#include "random.h"
#include "Denominations.h"

#include <cstring>
#include <cassert>
#include <vector>

extern "C" {
#include "crypto-ops.h"
}

namespace Crypto {

// ── Helpers ─────────────────────────────────────────────────────────────

static void random_scalar(EllipticCurveScalar& res) {
  unsigned char tmp[64];
  Random::randomBytes(64, tmp);
  sc_reduce(tmp);
  memcpy(&res, tmp, 32);
  sodium_memzero(tmp, 64);
}

static void uint64_to_scalar(uint64_t val, EllipticCurveScalar& s) {
  memset(s.data, 0, 32);
  for (int i = 0; i < 8; ++i) {
    s.data[i] = static_cast<uint8_t>(val >> (8 * i));
  }
}

static void p3_to_bytes(unsigned char out[32], const ge_p3* p) {
  ge_p3_tobytes(out, p);
}

static bool subgroup_check_p3(const ge_p3& p) {
  EllipticCurvePoint point;
  ge_p3_tobytes(reinterpret_cast<unsigned char*>(&point), &p);
  return point_valid_for_pedersen(point);
}

static bool p2_to_p3(ge_p3* out, const ge_p2* in) {
  unsigned char bytes[32];
  ge_tobytes(bytes, in);
  return ge_frombytes_vartime(out, bytes) == 0;
}

static void point_add(ge_p3* out, const ge_p3* a, const ge_p3* b) {
  ge_cached b_cached;
  ge_p3_to_cached(&b_cached, b);
  ge_p1p1 r;
  ge_add(&r, a, &b_cached);
  ge_p1p1_to_p3(out, &r);
}

static void point_sub(ge_p3* out, const ge_p3* a, const ge_p3* b) {
  ge_cached b_cached;
  ge_p3_to_cached(&b_cached, b);
  ge_p1p1 r;
  ge_sub(&r, a, &b_cached);
  ge_p1p1_to_p3(out, &r);
}

static bool point_equal(const ge_p3& a, const ge_p3& b) {
  unsigned char a_bytes[32], b_bytes[32];
  p3_to_bytes(a_bytes, &a);
  p3_to_bytes(b_bytes, &b);
  return memcmp(a_bytes, b_bytes, 32) == 0;
}

static bool scalarmult(ge_p3* out, const unsigned char s[32], const ge_p3* P) {
  ge_p2 r;
  ge_scalarmult(&r, s, P);
  return p2_to_p3(out, &r);
}

static void point_identity(ge_p3* out) {
  static const unsigned char identity_bytes[32] = {
    0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00
  };
  ge_frombytes_vartime(out, identity_bytes);
}

// Compute D[k] = C - V[k]*H for all k.
static bool compute_derived_ring(const EllipticCurvePoint& C,
                                 ge_p3 D[GK_N],
                                 ge_p3& C_p3) {
  if (ge_frombytes_vartime(&C_p3,
      reinterpret_cast<const unsigned char*>(&C)) != 0) {
    return false;
  }

  ge_p3 H_p3;
  const EllipticCurvePoint& H = pedersen_get_H();
  if (ge_frombytes_vartime(&H_p3,
      reinterpret_cast<const unsigned char*>(&H)) != 0) {
    return false;
  }

  for (size_t k = 0; k < GK_N; ++k) {
    EllipticCurveScalar vk_scalar;
    uint64_to_scalar(CryptoNote::DENOMINATIONS[k], vk_scalar);

    ge_p3 vkH;
    if (!scalarmult(&vkH, vk_scalar.data, &H_p3)) {
      return false;
    }

    point_sub(&D[k], &C_p3, &vkH);
  }

  return true;
}

// ── Fiat-Shamir challenge ───────────────────────────────────────────────
//
// x = Keccak(domain || D[0..63] || A[0..5] || B[0..5] || Q[0..5] || tx_hash)

static void compute_challenge(const Hash& tx_hash,
                              const ge_p3 D[GK_N],
                              const ge_p3 I[GK_n],
                              const ge_p3 A[GK_n],
                              const ge_p3 B[GK_n],
                              const ge_p3 Q[GK_n],
                              EllipticCurveScalar& x) {
  static const char domain[] = "GK-KarboCT-v2";
  const size_t domain_len = sizeof(domain) - 1;

  // 64 + 6 + 6 + 6 + 6 = 88 points
  const size_t n_points = GK_N + GK_n + GK_n + GK_n + GK_n;
  const size_t buf_size = domain_len + 32 + n_points * 32;

  std::vector<unsigned char> buf(buf_size);
  unsigned char* ptr = buf.data();

  memcpy(ptr, domain, domain_len);
  ptr += domain_len;

  for (size_t k = 0; k < GK_N; ++k) {
    p3_to_bytes(ptr, &D[k]);
    ptr += 32;
  }

  for (size_t j = 0; j < GK_n; ++j) {
    p3_to_bytes(ptr, &I[j]);
    ptr += 32;
  }

  for (size_t j = 0; j < GK_n; ++j) {
    p3_to_bytes(ptr, &A[j]);
    ptr += 32;
  }

  for (size_t j = 0; j < GK_n; ++j) {
    p3_to_bytes(ptr, &B[j]);
    ptr += 32;
  }

  for (size_t m = 0; m < GK_n; ++m) {
    p3_to_bytes(ptr, &Q[m]);
    ptr += 32;
  }

  memcpy(ptr, tx_hash.data, 32);
  ptr += 32;

  assert(ptr == buf.data() + buf_size);

  cn_fast_hash(buf.data(), buf_size, reinterpret_cast<Hash&>(x));
  sc_reduce32(x.data);
}

// ── Polynomial computation ──────────────────────────────────────────────
//
// p_k(x) = product_{j=0}^{5} f_{j, bit_j(k)}(x)
//
// f_{j,1}(x) = l_j*x + a_j       (constant=a_j,  linear=l_j)
// f_{j,0}(x) = (1-l_j)*x - a_j   (constant=-a_j, linear=1-l_j)
//
// For k=l: all factors have leading coefficient 1 → p_l has degree 6
// For k≠l: at least one factor is constant → degree < 6

static void compute_poly_coeffs(const int bits[GK_n],
                                const EllipticCurveScalar a[GK_n],
                                EllipticCurveScalar poly_coeffs[GK_N][GK_n + 1]) {
  unsigned char zero[32];
  sc_0(zero);

  for (size_t k = 0; k < GK_N; ++k) {
    unsigned char poly[GK_n + 1][32];
    memset(poly, 0, sizeof(poly));

    unsigned char one[32];
    memset(one, 0, 32);
    one[0] = 1;
    memcpy(poly[0], one, 32);

    size_t current_degree = 0;

    for (size_t j = 0; j < GK_n; ++j) {
      int k_j = (k >> j) & 1;
      int l_j = bits[j];

      unsigned char factor_const[32];
      unsigned char factor_linear[32];

      if (k_j == 1) {
        memcpy(factor_const, a[j].data, 32);
        memset(factor_linear, 0, 32);
        if (l_j) factor_linear[0] = 1;
      } else {
        sc_sub(factor_const, zero, a[j].data);
        memset(factor_linear, 0, 32);
        if (!l_j) factor_linear[0] = 1;
      }

      unsigned char new_poly[GK_n + 1][32];
      memset(new_poly, 0, sizeof(new_poly));

      for (size_t i = 0; i <= current_degree + 1; ++i) {
        unsigned char term1[32], term2[32];

        sc_mul(term1, factor_const, poly[i]);

        if (i > 0) {
          sc_mul(term2, factor_linear, poly[i - 1]);
          sc_add(new_poly[i], term1, term2);
        } else {
          memcpy(new_poly[i], term1, 32);
        }
      }

      current_degree++;
      memcpy(poly, new_poly, sizeof(poly));
    }

    for (size_t i = 0; i <= GK_n; ++i) {
      memcpy(poly_coeffs[k][i].data, poly[i], 32);
    }
  }
}

// ── Prover ──────────────────────────────────────────────────────────────

bool gk_prove(const EllipticCurvePoint& C,
              uint64_t v,
              const EllipticCurveScalar& r,
              size_t denomination_index,
              const Hash& tx_hash,
              GKProof& proof) {
  if (denomination_index >= GK_N) return false;
  if (CryptoNote::DENOMINATIONS[denomination_index] != v) return false;

  // Step 0: Compute derived ring D[k] = C - V[k]*H
  ge_p3 D[GK_N];
  ge_p3 C_p3;
  if (!compute_derived_ring(C, D, C_p3)) return false;
  if (!point_valid_for_pedersen(C)) return false;
  if (!subgroup_check_p3(C_p3)) return false;
  for (size_t k = 0; k < GK_N; ++k) {
    if (!subgroup_check_p3(D[k])) return false;
  }

  // Step 1: Decompose denomination_index into 6 bits
  int bits[GK_n];
  for (size_t j = 0; j < GK_n; ++j) {
    bits[j] = (denomination_index >> j) & 1;
  }

  // Generate random blinding scalars for bit commitments
  EllipticCurveScalar rj[GK_n], a[GK_n], s[GK_n], t[GK_n];
  for (size_t j = 0; j < GK_n; ++j) {
    random_scalar(rj[j]);
    random_scalar(a[j]);
    random_scalar(s[j]);
    random_scalar(t[j]);
  }

  // Compute I[j] = rj[j]*G + l_j*H,
  // A[j] = s[j]*G + a[j]*H,
  // B[j] = t[j]*G + (l_j*a[j])*H.
  ge_p3 H_p3;
  if (ge_frombytes_vartime(&H_p3,
      reinterpret_cast<const unsigned char*>(&pedersen_get_H())) != 0) {
    return false;
  }

  for (size_t j = 0; j < GK_n; ++j) {
    ge_p3 rG;
    ge_scalarmult_base(&rG, rj[j].data);
    if (bits[j]) {
      point_add(&proof.I[j], &rG, &H_p3);
    } else {
      proof.I[j] = rG;
    }

    ge_p3 sG, aH;
    ge_scalarmult_base(&sG, s[j].data);
    if (!scalarmult(&aH, a[j].data, &H_p3)) return false;
    point_add(&proof.A[j], &sG, &aH);

    ge_p3 tG;
    ge_scalarmult_base(&tG, t[j].data);

    if (bits[j]) {
      point_add(&proof.B[j], &tG, &aH);
    } else {
      proof.B[j] = tG;
    }
  }

  // Step 2: Compute selector polynomial coefficients
  EllipticCurveScalar poly_coeffs[GK_N][GK_n + 1];
  compute_poly_coeffs(bits, a, poly_coeffs);

  // Step 3: Q[m] = rho[m]*G + sum_k p_{k,m}*D[k] for m=0..5
  EllipticCurveScalar rho[GK_n];
  for (size_t m = 0; m < GK_n; ++m) {
    random_scalar(rho[m]);

    ge_p3 sum;
    ge_scalarmult_base(&sum, rho[m].data);

    for (size_t k = 0; k < GK_N; ++k) {
      if (!sc_isnonzero(poly_coeffs[k][m].data)) continue;

      ge_p3 term;
      if (!scalarmult(&term, poly_coeffs[k][m].data, &D[k])) return false;
      point_add(&sum, &sum, &term);
    }

    proof.Q[m] = sum;
  }

  // Step 4: Fiat-Shamir challenge
  EllipticCurveScalar x;
  compute_challenge(tx_hash, D, proof.I, proof.A, proof.B, proof.Q, x);

  // Step 5: Response scalars
  // z[j] = l_j * x + a[j]
  for (size_t j = 0; j < GK_n; ++j) {
    if (bits[j]) {
      sc_add(proof.z[j].data, x.data, a[j].data);
    } else {
      memcpy(proof.z[j].data, a[j].data, 32);
    }

    // za[j] = rj[j] * x + s[j]
    unsigned char term[32];
    sc_mul(term, rj[j].data, x.data);
    sc_add(proof.za[j].data, term, s[j].data);

    // zb[j] = rj[j] * (x - z[j]) + t[j]
    unsigned char x_minus_z[32];
    sc_sub(x_minus_z, x.data, proof.z[j].data);
    sc_mul(term, rj[j].data, x_minus_z);
    sc_add(proof.zb[j].data, term, t[j].data);
  }

  // f = r * x^6 - sum_{m=0}^{5} rho[m] * x^m
  unsigned char x_pow[GK_n + 1][32];
  memset(x_pow[0], 0, 32);
  x_pow[0][0] = 1;
  memcpy(x_pow[1], x.data, 32);
  for (size_t i = 2; i <= GK_n; ++i) {
    sc_mul(x_pow[i], x_pow[i - 1], x.data);
  }

  sc_mul(proof.f.data, r.data, x_pow[GK_n]);

  for (size_t m = 0; m < GK_n; ++m) {
    unsigned char term[32];
    sc_mul(term, rho[m].data, x_pow[m]);
    sc_sub(proof.f.data, proof.f.data, term);
  }

  // Secure cleanup
  sodium_memzero(rj, sizeof(rj));
  sodium_memzero(a, sizeof(a));
  sodium_memzero(s, sizeof(s));
  sodium_memzero(t, sizeof(t));
  sodium_memzero(rho, sizeof(rho));

  return true;
}

// ── Verifier ────────────────────────────────────────────────────────────

bool gk_verify(const EllipticCurvePoint& C,
               const GKProof& proof,
               const Hash& tx_hash) {
  if (sc_check(proof.f.data) != 0) {
    return false;
  }
  for (size_t j = 0; j < GK_n; ++j) {
    if (sc_check(proof.z[j].data) != 0) {
      return false;
    }
    if (sc_check(proof.za[j].data) != 0) {
      return false;
    }
    if (sc_check(proof.zb[j].data) != 0) {
      return false;
    }
    if (!subgroup_check_p3(proof.I[j])) {
      return false;
    }
    if (!subgroup_check_p3(proof.A[j])) {
      return false;
    }
    if (!subgroup_check_p3(proof.B[j])) {
      return false;
    }
    if (!subgroup_check_p3(proof.Q[j])) {
      return false;
    }
  }

  // Step 0: Compute derived ring
  ge_p3 D[GK_N];
  ge_p3 C_p3;
  if (!compute_derived_ring(C, D, C_p3)) return false;
  for (size_t k = 0; k < GK_N; ++k) {
    if (!subgroup_check_p3(D[k])) {
      return false;
    }
  }

  // Step 1: Recompute challenge
  EllipticCurveScalar x;
  compute_challenge(tx_hash, D, proof.I, proof.A, proof.B, proof.Q, x);

  ge_p3 H_p3;
  if (ge_frombytes_vartime(&H_p3,
      reinterpret_cast<const unsigned char*>(&pedersen_get_H())) != 0) {
    return false;
  }

  // Step 2: Verify bit-commitment response equations.
  //
  // I[j] = rj*G + l_j*H
  // A[j] = s_j*G + a_j*H
  // B[j] = t_j*G + l_j*a_j*H
  // z[j]  = l_j*x + a_j
  // za[j] = rj*x + s_j
  // zb[j] = rj*(x-z[j]) + t_j
  //
  // x*I[j] + A[j]       == za[j]*G + z[j]*H
  // (x-z[j])*I[j] + B[j] == zb[j]*G
  for (size_t j = 0; j < GK_n; ++j) {
    ge_p3 lhs, rhs, term;

    if (!scalarmult(&lhs, x.data, &proof.I[j])) return false;
    point_add(&lhs, &lhs, &proof.A[j]);

    ge_scalarmult_base(&rhs, proof.za[j].data);
    if (!scalarmult(&term, proof.z[j].data, &H_p3)) return false;
    point_add(&rhs, &rhs, &term);
    if (!point_equal(lhs, rhs)) {
      return false;
    }

    unsigned char x_minus_z[32];
    sc_sub(x_minus_z, x.data, proof.z[j].data);
    if (!scalarmult(&lhs, x_minus_z, &proof.I[j])) return false;
    point_add(&lhs, &lhs, &proof.B[j]);

    ge_scalarmult_base(&rhs, proof.zb[j].data);
    if (!point_equal(lhs, rhs)) {
      return false;
    }
  }

  // Step 3: Compute p_k(x) for each k
  // p_k(x) = product_{j=0}^{5} (bit_j(k)==1 ? z[j] : x-z[j])
  EllipticCurveScalar pk[GK_N];
  for (size_t k = 0; k < GK_N; ++k) {
    unsigned char product[32];
    memset(product, 0, 32);
    product[0] = 1;

    for (size_t j = 0; j < GK_n; ++j) {
      int k_j = (k >> j) & 1;
      unsigned char factor[32];

      if (k_j == 1) {
        memcpy(factor, proof.z[j].data, 32);
      } else {
        sc_sub(factor, x.data, proof.z[j].data);
      }

      sc_mul(product, product, factor);
    }

    memcpy(pk[k].data, product, 32);
  }

  // Step 4: LHS = sum_{k=0}^{63} p_k(x) * D[k]
  ge_p3 lhs;
  point_identity(&lhs);

  for (size_t k = 0; k < GK_N; ++k) {
    if (!sc_isnonzero(pk[k].data)) continue;

    ge_p3 term;
    if (!scalarmult(&term, pk[k].data, &D[k])) return false;
    point_add(&lhs, &lhs, &term);
  }

  // Step 5: RHS = f*G + sum_{m=0}^{5} x^m * Q[m]
  ge_p3 rhs;
  ge_scalarmult_base(&rhs, proof.f.data);

  unsigned char x_pow[GK_n][32]; // x^0 .. x^5
  memset(x_pow[0], 0, 32);
  x_pow[0][0] = 1; // x^0 = 1
  if (GK_n > 1) memcpy(x_pow[1], x.data, 32);
  for (size_t i = 2; i < GK_n; ++i) {
    sc_mul(x_pow[i], x_pow[i - 1], x.data);
  }

  for (size_t m = 0; m < GK_n; ++m) {
    ge_p3 term;
    if (!scalarmult(&term, x_pow[m], &proof.Q[m])) return false;
    point_add(&rhs, &rhs, &term);
  }

  // Step 6: Check LHS == RHS
  return point_equal(lhs, rhs);
}

} // namespace Crypto
