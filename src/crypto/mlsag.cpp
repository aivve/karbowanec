// Copyright (c) 2016-2026, The Karbo developers
//
// MLSAG ring signature implementation for confidential transactions.

#include "mlsag.h"
#include "crypto.h"
#include "hash.h"
#include "pedersen.h"
#include "random.h"
#include <cassert>
#include <cstring>
#include <memory>
#include <vector>

extern "C" {
#include "crypto-ops.h"
}

namespace Crypto {

// Hash-to-curve: maps a public key to a prime-order Ed25519 point.
// Matches the hash_to_ec in crypto.cpp (Keccak + elligator + cofactor clear).
static void mlsag_hash_to_ec(const PublicKey& key, ge_p3& res) {
  Hash h;
  ge_p2 point;
  ge_p1p1 point2;
  cn_fast_hash(std::addressof(key), sizeof(PublicKey), h);
  ge_fromfe_frombytes_vartime(&point, reinterpret_cast<const unsigned char*>(&h));
  ge_mul8(&point2, &point);
  ge_p1p1_to_p3(&res, &point2);
}

// Generate a random scalar in [0, l) where l is the Ed25519 group order.
static void mlsag_random_scalar(EllipticCurveScalar& res) {
  unsigned char tmp[64];
  Random::randomBytes(64, tmp);
  sc_reduce(tmp);
  memcpy(&res, tmp, 32);
}

// Compute MLSAG round challenge: c = Hs(domain || message || L1 || R1 || L2)
// where Hs is hash-to-scalar (Keccak then reduce mod l).
static void mlsag_round_hash(
  const Hash& message,
  const unsigned char L1[32],
  const unsigned char R1[32],
  const unsigned char L2[32],
  EllipticCurveScalar& c_out)
{
  static const char domain[] = "MLSAG-KarboCT-v1";
  const size_t domain_len = sizeof(domain) - 1;
  const size_t payload_len = 32 * 4;
  unsigned char buf[domain_len + payload_len];

  unsigned char* ptr = buf;
  memcpy(ptr, domain, domain_len);
  ptr += domain_len;

  memcpy(ptr, &message, 32);
  ptr += 32;
  memcpy(ptr, L1, 32);
  ptr += 32;
  memcpy(ptr, R1, 32);
  ptr += 32;
  memcpy(ptr, L2, 32);
  ptr += 32;

  Hash h;
  cn_fast_hash(buf, static_cast<size_t>(ptr - buf), h);
  memcpy(&c_out, &h, 32);
  sc_reduce32(reinterpret_cast<unsigned char*>(&c_out));
}

// Compute D = C - C_pseudo (commitment difference) as ge_p3.
static bool compute_commit_diff(
  const EllipticCurvePoint& C,
  const ge_cached& pseudo_cached,
  ge_p3& D_out)
{
  ge_p3 c_p3;
  if (ge_frombytes_vartime(&c_p3, reinterpret_cast<const unsigned char*>(&C)) != 0)
    return false;
  ge_p1p1 tmp;
  ge_sub(&tmp, &c_p3, &pseudo_cached);
  ge_p1p1_to_p3(&D_out, &tmp);
  return true;
}

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
  MLSAGSignature& sig)
{
  if (ring_size == 0 || true_index >= ring_size)
    return false;

  sig.ss.resize(ring_size);

  if (!ct_public_key_valid(ring_pubkeys[true_index])) {
    return false;
  }

  // --- Key image: I = x * Hp(P_l) ---
  ge_p3 hp_real;
  mlsag_hash_to_ec(ring_pubkeys[true_index], hp_real);
  ge_p2 image_p2;
  ge_scalarmult(&image_p2, reinterpret_cast<const unsigned char*>(&spend_privkey), &hp_real);
  ge_tobytes(reinterpret_cast<unsigned char*>(&key_image), &image_p2);

  // Precompute key image table for double-scalar-mult in decoy rounds
  ge_p3 image_p3;
  if (ge_frombytes_vartime(&image_p3, reinterpret_cast<const unsigned char*>(&key_image)) != 0)
    return false;
  EllipticCurvePoint keyImagePoint;
  memcpy(keyImagePoint.data, &key_image, sizeof(keyImagePoint.data));
  if (!point_valid_for_pedersen(keyImagePoint))
    return false;
  ge_dsmp image_pre;
  ge_dsm_precomp(image_pre, &image_p3);

  // --- Pseudo-output commitment as cached point (for C_k - C') subtraction) ---
  ge_p3 pseudo_p3;
  if (ge_frombytes_vartime(&pseudo_p3, reinterpret_cast<const unsigned char*>(&pseudo_commit)) != 0)
    return false;
  ge_cached pseudo_cached;
  ge_p3_to_cached(&pseudo_cached, &pseudo_p3);

  // --- Blinding secret: z = r_real - r_pseudo ---
  EllipticCurveScalar z;
  sc_sub(reinterpret_cast<unsigned char*>(&z),
         reinterpret_cast<const unsigned char*>(&real_blinding),
         reinterpret_cast<const unsigned char*>(&pseudo_blinding));

  // --- Generate random nonces for the real index ---
  EllipticCurveScalar alpha1, alpha2;
  mlsag_random_scalar(alpha1);
  mlsag_random_scalar(alpha2);

  // --- All challenges stored in an array for easy c_0 retrieval ---
  std::vector<EllipticCurveScalar> c(ring_size);

  unsigned char L1_bytes[32], R1_bytes[32], L2_bytes[32];
  ge_p3 tmp_p3;
  ge_p2 tmp_p2;

  // --- Compute (L1, R1, L2) at the true index using alpha nonces ---

  // L1 = alpha1 * G
  ge_scalarmult_base(&tmp_p3, reinterpret_cast<const unsigned char*>(&alpha1));
  ge_p3_tobytes(L1_bytes, &tmp_p3);

  // R1 = alpha1 * Hp(P_l)
  ge_scalarmult(&tmp_p2, reinterpret_cast<const unsigned char*>(&alpha1), &hp_real);
  ge_tobytes(R1_bytes, &tmp_p2);

  // L2 = alpha2 * G
  ge_scalarmult_base(&tmp_p3, reinterpret_cast<const unsigned char*>(&alpha2));
  ge_p3_tobytes(L2_bytes, &tmp_p3);

  // c_{l+1} = Hs(message || L1 || R1 || L2)
  mlsag_round_hash(message, L1_bytes, R1_bytes, L2_bytes,
                   c[(true_index + 1) % ring_size]);

  // --- Walk the ring: l+1 → l+2 → ... → l-1, computing decoy responses ---
  for (size_t j = 1; j < ring_size; ++j) {
    size_t i = (true_index + j) % ring_size;

    // Generate random response scalars for decoy i
    mlsag_random_scalar(sig.ss[i][0]);
    mlsag_random_scalar(sig.ss[i][1]);

    // Unpack P_i
    ge_p3 P_i;
    if (ge_frombytes_vartime(&P_i, reinterpret_cast<const unsigned char*>(&ring_pubkeys[i])) != 0)
      return false;

    // L1 = s[i][0]*G + c_i*P_i
    ge_double_scalarmult_base_vartime(&tmp_p2,
      reinterpret_cast<const unsigned char*>(&c[i]), &P_i,
      reinterpret_cast<const unsigned char*>(&sig.ss[i][0]));
    ge_tobytes(L1_bytes, &tmp_p2);

    // R1 = s[i][0]*Hp(P_i) + c_i*I
    ge_p3 hp_i;
    mlsag_hash_to_ec(ring_pubkeys[i], hp_i);
    ge_double_scalarmult_precomp_vartime(&tmp_p2,
      reinterpret_cast<const unsigned char*>(&sig.ss[i][0]), &hp_i,
      reinterpret_cast<const unsigned char*>(&c[i]), image_pre);
    ge_tobytes(R1_bytes, &tmp_p2);

    // D_i = C_i - C' (commitment difference)
    ge_p3 D_i;
    if (!compute_commit_diff(ring_commits[i], pseudo_cached, D_i))
      return false;

    // L2 = s[i][1]*G + c_i*D_i
    ge_double_scalarmult_base_vartime(&tmp_p2,
      reinterpret_cast<const unsigned char*>(&c[i]), &D_i,
      reinterpret_cast<const unsigned char*>(&sig.ss[i][1]));
    ge_tobytes(L2_bytes, &tmp_p2);

    // c_{i+1 mod n}
    mlsag_round_hash(message, L1_bytes, R1_bytes, L2_bytes,
                     c[(i + 1) % ring_size]);
  }

  // --- Close the ring: compute response scalars at the true index ---
  // c[true_index] is now the challenge at the true index (c_l)
  // s_l[0] = alpha1 - c_l * x_l
  sc_mulsub(reinterpret_cast<unsigned char*>(&sig.ss[true_index][0]),
            reinterpret_cast<const unsigned char*>(&c[true_index]),
            reinterpret_cast<const unsigned char*>(&spend_privkey),
            reinterpret_cast<const unsigned char*>(&alpha1));

  // s_l[1] = alpha2 - c_l * z
  sc_mulsub(reinterpret_cast<unsigned char*>(&sig.ss[true_index][1]),
            reinterpret_cast<const unsigned char*>(&c[true_index]),
            reinterpret_cast<const unsigned char*>(&z),
            reinterpret_cast<const unsigned char*>(&alpha2));

  // Output c_0
  sig.c0 = c[0];

  return true;
}

static bool verify_ring(
  const Hash& message,
  const PublicKey ring_pubkeys[],
  const EllipticCurvePoint ring_commits[],
  const ge_cached& pseudo_cached,
  size_t ring_size,
  const ge_dsmp& image_pre,
  const MLSAGSignature& sig)
{
  unsigned char L1_bytes[32], R1_bytes[32], L2_bytes[32];
  ge_p2 tmp_p2;
  EllipticCurveScalar c_cur = sig.c0;

  for (size_t i = 0; i < ring_size; ++i) {
    ge_p3 P_i;
    if (ge_frombytes_vartime(&P_i, reinterpret_cast<const unsigned char*>(&ring_pubkeys[i])) != 0)
      return false;
    if (!ct_public_key_valid(ring_pubkeys[i]))
      return false;

    ge_double_scalarmult_base_vartime(&tmp_p2,
      reinterpret_cast<const unsigned char*>(&c_cur), &P_i,
      reinterpret_cast<const unsigned char*>(&sig.ss[i][0]));
    ge_tobytes(L1_bytes, &tmp_p2);

    ge_p3 hp_i;
    mlsag_hash_to_ec(ring_pubkeys[i], hp_i);
    ge_double_scalarmult_precomp_vartime(&tmp_p2,
      reinterpret_cast<const unsigned char*>(&sig.ss[i][0]), &hp_i,
      reinterpret_cast<const unsigned char*>(&c_cur), image_pre);
    ge_tobytes(R1_bytes, &tmp_p2);

    ge_p3 D_i;
    if (!compute_commit_diff(ring_commits[i], pseudo_cached, D_i))
      return false;

    ge_double_scalarmult_base_vartime(&tmp_p2,
      reinterpret_cast<const unsigned char*>(&c_cur), &D_i,
      reinterpret_cast<const unsigned char*>(&sig.ss[i][1]));
    ge_tobytes(L2_bytes, &tmp_p2);

    mlsag_round_hash(message, L1_bytes, R1_bytes, L2_bytes, c_cur);
  }

  EllipticCurveScalar diff;
  sc_sub(reinterpret_cast<unsigned char*>(&diff),
         reinterpret_cast<const unsigned char*>(&c_cur),
         reinterpret_cast<const unsigned char*>(&sig.c0));
  return sc_isnonzero(reinterpret_cast<const unsigned char*>(&diff)) == 0;
}

bool mlsag_verify(
  const Hash& message,
  const PublicKey ring_pubkeys[],
  const EllipticCurvePoint ring_commits[],
  const EllipticCurvePoint& pseudo_commit,
  size_t ring_size,
  const KeyImage& key_image,
  const MLSAGSignature& sig)
{
  if (ring_size == 0 || sig.ss.size() != ring_size)
    return false;

  // Validate all response scalars are in range
  for (size_t i = 0; i < ring_size; ++i) {
    if (sc_check(reinterpret_cast<const unsigned char*>(&sig.ss[i][0])) != 0)
      return false;
    if (sc_check(reinterpret_cast<const unsigned char*>(&sig.ss[i][1])) != 0)
      return false;
  }

  // Validate c_0
  if (sc_check(reinterpret_cast<const unsigned char*>(&sig.c0)) != 0)
    return false;

  // Precompute key image table
  ge_p3 image_p3;
  if (ge_frombytes_vartime(&image_p3, reinterpret_cast<const unsigned char*>(&key_image)) != 0)
    return false;
  EllipticCurvePoint keyImagePoint;
  memcpy(keyImagePoint.data, &key_image, sizeof(keyImagePoint.data));
  if (!point_valid_for_pedersen(keyImagePoint))
    return false;
  ge_dsmp image_pre;
  ge_dsm_precomp(image_pre, &image_p3);

  // Precompute -C' for commitment difference subtraction
  ge_p3 pseudo_p3;
  if (ge_frombytes_vartime(&pseudo_p3, reinterpret_cast<const unsigned char*>(&pseudo_commit)) != 0)
    return false;
  ge_cached pseudo_cached;
  ge_p3_to_cached(&pseudo_cached, &pseudo_p3);

  return verify_ring(message, ring_pubkeys, ring_commits, pseudo_cached,
                     ring_size, image_pre, sig);
}

} // namespace Crypto
