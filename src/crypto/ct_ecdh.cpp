// Copyright (c) 2024-2026, The Karbo developers
//
// This file is part of Karbo.
//
// Karbo is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Karbo is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Karbo.  If not, see <http://www.gnu.org/licenses/>.

#include <cassert>
#include <cstring>

#include "Common/Varint.h"
#include "ct_ecdh.h"
#include "crypto.h"
#include "crypto-ops.h"
#include "hash.h"

namespace Crypto {

  PublicKey H;

  void ct_ecdh_init() {
    // H = hash_to_point("karbo_pedersen_H")
    // Uses the same hash-to-curve method as key images (cofactor-cleared).
    static const char domain[] = "karbo_pedersen_H";
    hash_data_to_ec(reinterpret_cast<const uint8_t*>(domain), sizeof(domain) - 1, H);
  }

  // Internal helper: derive scalar from shared secret concatenated with a varint index.
  // Replicates the pattern used by derivation_to_scalar in crypto.cpp.
  static void shared_secret_to_scalar(const KeyDerivation& shared_secret,
                                      size_t index, EllipticCurveScalar& res) {
    struct {
      KeyDerivation derivation;
      char index_buf[(sizeof(size_t) * 8 + 6) / 7];
    } buf;
    char* end = buf.index_buf;
    buf.derivation = shared_secret;
    Tools::write_varint(end, index);
    assert(end <= buf.index_buf + sizeof buf.index_buf);
    hash_to_scalar(&buf, end - reinterpret_cast<char*>(&buf), res);
  }

  void derive_blinding_factor(const KeyDerivation& shared_secret, size_t output_index,
                              EllipticCurveScalar& blinding_factor) {
    // r = Hs(shared_secret || output_index)
    shared_secret_to_scalar(shared_secret, output_index, blinding_factor);
  }

  bool pedersen_commit(uint64_t amount, const EllipticCurveScalar& blinding_factor,
                       PublicKey& commitment) {
    // C = amount * H + blinding_factor * G

    // Encode amount as a scalar (little-endian uint64 zero-padded to 32 bytes)
    unsigned char amount_scalar[32] = {0};
    for (int i = 0; i < 8; i++) {
      amount_scalar[i] = static_cast<unsigned char>((amount >> (8 * i)) & 0xFF);
    }

    // Compute amount * H
    ge_p3 H_point;
    if (ge_frombytes_vartime(&H_point, reinterpret_cast<const unsigned char*>(&H)) != 0) {
      return false;
    }
    ge_p2 vH_p2;
    ge_scalarmult(&vH_p2, amount_scalar, &H_point);

    // Compute blinding_factor * G
    ge_p3 rG;
    ge_scalarmult_base(&rG, reinterpret_cast<const unsigned char*>(&blinding_factor));

    // Add: C = vH + rG
    // Convert vH from p2 to p3 via bytes round-trip
    unsigned char vH_bytes[32];
    ge_tobytes(vH_bytes, &vH_p2);
    ge_p3 vH_p3;
    if (ge_frombytes_vartime(&vH_p3, vH_bytes) != 0) {
      return false;
    }

    ge_cached rG_cached;
    ge_p3_to_cached(&rG_cached, &rG);
    ge_p1p1 sum_p1p1;
    ge_add(&sum_p1p1, &vH_p3, &rG_cached);
    ge_p2 sum_p2;
    ge_p1p1_to_p2(&sum_p2, &sum_p1p1);
    ge_tobytes(reinterpret_cast<unsigned char*>(&commitment), &sum_p2);

    return true;
  }

  // Internal: compute the 8-byte amount mask from shared secret.
  // mask = Hs(shared_secret || 0x00)[0..7]
  static void compute_amount_mask(const KeyDerivation& shared_secret, uint8_t mask[8]) {
    EllipticCurveScalar scalar;
    // Use index 0 with a domain separator: we pass size_t(0) through the varint
    // encoding, but to distinguish from blinding factor derivation we use a
    // different approach: hash the shared secret with a 0x00 byte appended.
    struct {
      KeyDerivation derivation;
      uint8_t domain;
    } buf;
    buf.derivation = shared_secret;
    buf.domain = 0x00;
    hash_to_scalar(&buf, sizeof(buf), scalar);
    memcpy(mask, scalar.data, 8);
  }

  void mask_amount(const KeyDerivation& shared_secret, uint64_t amount,
                   MaskedAmount& masked) {
    // Encode amount as little-endian uint64
    uint8_t amount_le[8];
    for (int i = 0; i < 8; i++) {
      amount_le[i] = static_cast<uint8_t>((amount >> (8 * i)) & 0xFF);
    }

    // Compute mask and XOR
    uint8_t mask[8];
    compute_amount_mask(shared_secret, mask);
    for (int i = 0; i < 8; i++) {
      masked.data[i] = amount_le[i] ^ mask[i];
    }
  }

  uint64_t unmask_amount(const KeyDerivation& shared_secret,
                         const MaskedAmount& masked) {
    // Compute mask and XOR to recover amount
    uint8_t mask[8];
    compute_amount_mask(shared_secret, mask);

    uint8_t amount_le[8];
    for (int i = 0; i < 8; i++) {
      amount_le[i] = masked.data[i] ^ mask[i];
    }

    // Decode little-endian uint64
    uint64_t amount = 0;
    for (int i = 0; i < 8; i++) {
      amount |= static_cast<uint64_t>(amount_le[i]) << (8 * i);
    }
    return amount;
  }

  bool decrypt_and_verify_output(const SecretKey& view_secret_key,
                                 const PublicKey& tx_public_key,
                                 size_t output_index,
                                 const MaskedAmount& masked,
                                 const PublicKey& commitment,
                                 uint64_t& amount_out,
                                 EllipticCurveScalar& blinding_factor_out) {
    // Step 1: Re-derive shared secret = key_derivation(R, a) = 8*a*R
    KeyDerivation shared_secret;
    if (!generate_key_derivation(tx_public_key, view_secret_key, shared_secret)) {
      return false;
    }

    // Step 2: Unmask amount
    amount_out = unmask_amount(shared_secret, masked);

    // Step 3: Re-derive blinding factor
    derive_blinding_factor(shared_secret, output_index, blinding_factor_out);

    // Step 4: Recompute commitment and verify
    PublicKey expected_commitment;
    if (!pedersen_commit(amount_out, blinding_factor_out, expected_commitment)) {
      return false;
    }

    // Constant-time comparison
    return sodium_compare(commitment.data, expected_commitment.data, 32) == 0;
  }

} // namespace Crypto
