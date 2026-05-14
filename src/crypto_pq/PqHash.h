// Copyright (c) 2026, The Karbo developers
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

#include <array>
#include <cstddef>
#include <cstdint>

// FIPS 202 SHA3-256 + RFC 5869 HKDF-SHA3-256 for the Karbo PQ transaction
// family (block major v7, transaction version 3). See PQ Phase 1 spec §2.
//
// INTENTIONALLY DISTINCT from Crypto::cn_fast_hash and src/crypto/keccak.{c,h},
// which implement CryptoNote-Keccak (padding 0x01) — those are NOT NIST SHA-3
// and MUST NOT be used in PQ consensus paths.

namespace CryptoPQ {

using Hash256 = std::array<uint8_t, 32>;

Hash256 sha3_256(const void* data, std::size_t len) noexcept;

// HMAC-SHA3-256 (NIST FIPS 198-1 + FIPS 202 / SP 800-224). Exposed publicly
// because HKDF builds on it and tests want to KAT it directly against NIST
// CAVP vectors; also a useful primitive in its own right.
Hash256 hmac_sha3_256(const void* key, std::size_t key_len,
                      const void* msg, std::size_t msg_len) noexcept;

// HKDF-SHA3-256 with the Karbo PQ default instantiation:
//   salt = 32 zero bytes, output length L = 32 bytes.
// Spec §2 fixes salt and L for every Phase-1 protocol derivation. Use the
// explicit-parameter variant only for tests or for future protocols that
// deliberately differ.
Hash256 hkdf_sha3_256(const void* ikm,  std::size_t ikm_len,
                      const void* info, std::size_t info_len) noexcept;

// Explicit-parameter HKDF (RFC 5869). okm must point to okm_len writable
// bytes. Returns false if okm_len is outside [1, 255 * 32].
bool hkdf_sha3_256_explicit(const void* ikm,  std::size_t ikm_len,
                            const void* salt, std::size_t salt_len,
                            const void* info, std::size_t info_len,
                            void* okm, std::size_t okm_len) noexcept;

}  // namespace CryptoPQ
