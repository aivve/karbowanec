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
#include "pedersen.h"

namespace Crypto {

  void ct_ecdh_init() {
    // No-op: kept for API compatibility. The active CT Pedersen generator is
    // provided by pedersen_get_H() in pedersen.cpp.
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
    EllipticCurveScalar amount_scalar;
    memset(amount_scalar.data, 0, sizeof(amount_scalar.data));
    for (int i = 0; i < 8; ++i) {
      amount_scalar.data[i] = static_cast<unsigned char>((amount >> (8 * i)) & 0xFF);
    }

    EllipticCurvePoint commitment_point;
    if (!Crypto::pedersen_commit(amount_scalar, blinding_factor, commitment_point)) {
      return false;
    }

    static_assert(sizeof(commitment) == sizeof(commitment_point), "Point/PublicKey size mismatch");
    memcpy(&commitment, &commitment_point, sizeof(commitment));
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
