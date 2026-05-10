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

#pragma once

#include <cstdint>
#include <CryptoTypes.h>

namespace Crypto {

// Kept for backwards compatibility with older tests/callers.
// CT Pedersen commitments now use the shared pedersen.cpp generator path.
void ct_ecdh_init();

#pragma pack(push, 1)
struct MaskedAmount {
  uint8_t data[8]; // XOR-masked little-endian uint64
};
#pragma pack(pop)

// Derive per-output blinding factor r from ECDH shared secret and output index.
//   shared_secret = generate_key_derivation(recipient_view_pub, tx_secret_key)
//   r = Hs(shared_secret || output_index)
void derive_blinding_factor(const KeyDerivation& shared_secret, size_t output_index,
                            EllipticCurveScalar& blinding_factor);

// Compute Pedersen commitment: C = v*H + r*G
bool pedersen_commit(uint64_t amount, const EllipticCurveScalar& blinding_factor,
                     PublicKey& commitment);

// Mask amount for on-chain storage.
//   mask = Hs(shared_secret || output_index || "amount-mask-v1")[0..7]
//   masked = uint64_le(amount) XOR mask
void mask_amount(const KeyDerivation& shared_secret, size_t output_index, uint64_t amount,
                 MaskedAmount& masked);

// Unmask amount from on-chain data.
uint64_t unmask_amount(const KeyDerivation& shared_secret, size_t output_index,
                       const MaskedAmount& masked);

// Recipient: given view secret key and tx public key R, recover amount and
// blinding factor for a specific output, then verify the commitment.
// Returns true if C == amount*H + blinding_factor*G.
bool decrypt_and_verify_output(const SecretKey& view_secret_key,
                               const PublicKey& tx_public_key,
                               size_t output_index,
                               const MaskedAmount& masked,
                               const PublicKey& commitment,
                               uint64_t& amount_out,
                               EllipticCurveScalar& blinding_factor_out);

} // namespace Crypto
