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
#include <optional>
#include <vector>

// ChaCha20-Poly1305 IETF (RFC 8439) — used by the Karbo PQ transaction family
// to encrypt the per-output rho payload (spec §6.5).
//
// Karbo PQ instantiation (spec §2 / §6.5):
//   key    : 32 bytes, derived per-output from HKDF on the ML-KEM shared secret
//   nonce  : 12 zero bytes (safe — the key is unique per output)
//   aad    : out_context || LE64(amount), 40 bytes
//   msg    : rho, 32 bytes
//   tag    : 16 bytes appended to ciphertext
//
// The wrapper is general (any key/nonce/aad/msg lengths) — protocol-specific
// shapes are enforced at the call site, not here.

namespace CryptoPQ {

constexpr std::size_t kAeadKeyBytes   = 32;
constexpr std::size_t kAeadNonceBytes = 12;
constexpr std::size_t kAeadTagBytes   = 16;

using AeadKey   = std::array<uint8_t, kAeadKeyBytes>;
using AeadNonce = std::array<uint8_t, kAeadNonceBytes>;

// Encrypt msg under (key, nonce) with associated data aad. Returns
// ciphertext || tag (msg_len + 16 bytes). msg may be empty.
std::vector<uint8_t> aead_encrypt(
    const AeadKey&   key,
    const AeadNonce& nonce,
    const void* aad, std::size_t aad_len,
    const void* msg, std::size_t msg_len);

// Decrypt ciphertext || tag (in_len bytes). Returns the recovered plaintext
// on tag success; std::nullopt on tag mismatch. The tag check is constant-time.
//
// **Callers MUST treat std::nullopt as "not for me OR tampered" without
// distinguishing — exposing the difference leaks scan state.** (Spec §7
// garbage-output handling.)
std::optional<std::vector<uint8_t>> aead_decrypt(
    const AeadKey&   key,
    const AeadNonce& nonce,
    const void* aad, std::size_t aad_len,
    const void* in,  std::size_t in_len);

}  // namespace CryptoPQ
