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
#include <utility>

// ML-DSA-65 (FIPS 204) — per-output spend authorization signature for the
// Karbo PQ transaction family. Spec §2 / §6.2 / §8.
//
// Per-output spend keys are derived deterministically from the ML-KEM shared
// secret via HKDF, so deterministic keygen from a 32-byte seed is mandatory.
// liboqs upstream does not yet ship that variant — we add it via a small
// patch documented in external/liboqs/UPSTREAM.md.

namespace CryptoPQ {

constexpr std::size_t kDsaPublicKeyBytes  = 1952;
constexpr std::size_t kDsaSecretKeyBytes  = 4032;
constexpr std::size_t kDsaSignatureBytes  = 3309;
constexpr std::size_t kDsaKeypairSeedBytes = 32;  // FIPS 204 xi

using DsaPublicKey   = std::array<uint8_t, kDsaPublicKeyBytes>;
using DsaSecretKey   = std::array<uint8_t, kDsaSecretKeyBytes>;
using DsaSignature   = std::array<uint8_t, kDsaSignatureBytes>;
using DsaKeypairSeed = std::array<uint8_t, kDsaKeypairSeedBytes>;

// Random keygen — pulls 32 bytes from liboqs's CSPRNG.
std::pair<DsaPublicKey, DsaSecretKey> dsa_keygen();

// Deterministic keygen from a 32-byte seed (FIPS 204 xi). Required for
// per-output spend keys: spend_seed = HKDF(ss, ...) → keygen(spend_seed).
// The same seed always produces the same (pub, sk) pair.
std::pair<DsaPublicKey, DsaSecretKey> dsa_keygen_from_seed(const DsaKeypairSeed& seed);

// Sign `msg` (any length) with `sk`. ML-DSA-65 signatures are always
// kDsaSignatureBytes = 3309 bytes long. The wrapper enforces this and
// throws on the (extraordinarily unlikely) mismatch path.
DsaSignature dsa_sign(const DsaSecretKey& sk, const void* msg, std::size_t msg_len);

// Verify `sig` over `msg` with `pub`. Returns true on a valid signature,
// false on any failure (bad signature, wrong key, malformed input).
bool dsa_verify(const DsaPublicKey& pub,
                const void* msg, std::size_t msg_len,
                const DsaSignature& sig) noexcept;

}  // namespace CryptoPQ
