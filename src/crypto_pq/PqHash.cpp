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

#include "PqHash.h"

#include <cstring>

extern "C" {
#include <oqs/sha3.h>
}

namespace CryptoPQ {

namespace {

// SHA3-256 sponge rate r = 1088 bits = 136 bytes. Used as the HMAC block size:
// NIST SP 800-224 explicitly defines the HMAC block size for SHA-3 as the
// rate, not the digest size.
constexpr std::size_t kSha3_256RateBytes   = 136;
constexpr std::size_t kSha3_256DigestBytes = 32;

// Best-effort wipe of short-lived key material on the stack. The project's
// existing src/crypto/insecure_memzero.h is a no-op macro; the volatile cast
// here defeats the most common dead-store elimination but is not constant-time
// against a determined attacker — by design, key wiping is defence in depth.
void wipe(void* p, std::size_t n) noexcept {
  volatile auto* vp = static_cast<volatile uint8_t*>(p);
  while (n--) {
    *vp++ = 0;
  }
}

// Derive the inner/outer pad blocks from a key. key_len <= rate is the common
// case; longer keys are pre-hashed per the HMAC spec.
void hmac_pads(const uint8_t* key, std::size_t key_len,
               uint8_t ipad[kSha3_256RateBytes],
               uint8_t opad[kSha3_256RateBytes]) noexcept {
  uint8_t key_block[kSha3_256RateBytes] = {0};
  if (key_len > kSha3_256RateBytes) {
    OQS_SHA3_sha3_256(key_block, key, key_len);  // remainder stays zero
  } else if (key_len > 0) {
    std::memcpy(key_block, key, key_len);
  }
  for (std::size_t i = 0; i < kSha3_256RateBytes; ++i) {
    ipad[i] = key_block[i] ^ 0x36;
    opad[i] = key_block[i] ^ 0x5c;
  }
  wipe(key_block, sizeof(key_block));
}

// HMAC-SHA3-256 with a single contiguous message — internal byte-pointer form.
void hmac_sha3_256_raw(const uint8_t* key, std::size_t key_len,
                       const uint8_t* msg, std::size_t msg_len,
                       uint8_t out[kSha3_256DigestBytes]) noexcept {
  uint8_t ipad[kSha3_256RateBytes];
  uint8_t opad[kSha3_256RateBytes];
  hmac_pads(key, key_len, ipad, opad);

  uint8_t inner[kSha3_256DigestBytes];
  {
    OQS_SHA3_sha3_256_inc_ctx ctx;
    OQS_SHA3_sha3_256_inc_init(&ctx);
    OQS_SHA3_sha3_256_inc_absorb(&ctx, ipad, kSha3_256RateBytes);
    if (msg_len > 0) {
      OQS_SHA3_sha3_256_inc_absorb(&ctx, msg, msg_len);
    }
    OQS_SHA3_sha3_256_inc_finalize(inner, &ctx);
    OQS_SHA3_sha3_256_inc_ctx_release(&ctx);
  }
  {
    OQS_SHA3_sha3_256_inc_ctx ctx;
    OQS_SHA3_sha3_256_inc_init(&ctx);
    OQS_SHA3_sha3_256_inc_absorb(&ctx, opad, kSha3_256RateBytes);
    OQS_SHA3_sha3_256_inc_absorb(&ctx, inner, kSha3_256DigestBytes);
    OQS_SHA3_sha3_256_inc_finalize(out, &ctx);
    OQS_SHA3_sha3_256_inc_ctx_release(&ctx);
  }

  wipe(ipad,  sizeof(ipad));
  wipe(opad,  sizeof(opad));
  wipe(inner, sizeof(inner));
}

}  // namespace

Hash256 sha3_256(const void* data, std::size_t len) noexcept {
  Hash256 out;
  OQS_SHA3_sha3_256(out.data(), static_cast<const uint8_t*>(data), len);
  return out;
}

Hash256 hmac_sha3_256(const void* key, std::size_t key_len,
                      const void* msg, std::size_t msg_len) noexcept {
  Hash256 out;
  hmac_sha3_256_raw(static_cast<const uint8_t*>(key), key_len,
                    static_cast<const uint8_t*>(msg), msg_len,
                    out.data());
  return out;
}

bool hkdf_sha3_256_explicit(const void* ikm,  std::size_t ikm_len,
                            const void* salt, std::size_t salt_len,
                            const void* info, std::size_t info_len,
                            void* okm, std::size_t okm_len) noexcept {
  if (okm_len == 0 || okm_len > 255 * kSha3_256DigestBytes) {
    return false;
  }

  // Extract: PRK = HMAC(salt, IKM).
  // RFC 5869 §2.2: empty salt is treated as HashLen zero bytes.
  uint8_t default_salt[kSha3_256DigestBytes] = {0};
  const uint8_t* salt_ptr;
  std::size_t    salt_use_len;
  if (salt_len == 0 || salt == nullptr) {
    salt_ptr     = default_salt;
    salt_use_len = sizeof(default_salt);
  } else {
    salt_ptr     = static_cast<const uint8_t*>(salt);
    salt_use_len = salt_len;
  }

  uint8_t prk[kSha3_256DigestBytes];
  hmac_sha3_256_raw(salt_ptr, salt_use_len,
                    static_cast<const uint8_t*>(ikm), ikm_len,
                    prk);

  // Expand: T(i) = HMAC(PRK, T(i-1) || info || byte(i)).
  // PRK is exactly 32 bytes <= rate, so we can derive the pads once and
  // reuse them across the loop iterations. Each iteration only needs to
  // re-absorb T(i-1), info, and the counter byte.
  uint8_t ipad[kSha3_256RateBytes];
  uint8_t opad[kSha3_256RateBytes];
  hmac_pads(prk, kSha3_256DigestBytes, ipad, opad);

  uint8_t t_prev[kSha3_256DigestBytes];
  uint8_t t_curr[kSha3_256DigestBytes];
  const auto* info_bytes = static_cast<const uint8_t*>(info);
  auto*       okm_bytes  = static_cast<uint8_t*>(okm);

  std::size_t produced = 0;
  uint8_t     counter  = 0;

  while (produced < okm_len) {
    counter++;  // Cap at 255 enforced by the okm_len bound above.

    uint8_t inner[kSha3_256DigestBytes];
    {
      OQS_SHA3_sha3_256_inc_ctx ctx;
      OQS_SHA3_sha3_256_inc_init(&ctx);
      OQS_SHA3_sha3_256_inc_absorb(&ctx, ipad, kSha3_256RateBytes);
      if (counter > 1) {
        OQS_SHA3_sha3_256_inc_absorb(&ctx, t_prev, kSha3_256DigestBytes);
      }
      if (info_len > 0) {
        OQS_SHA3_sha3_256_inc_absorb(&ctx, info_bytes, info_len);
      }
      OQS_SHA3_sha3_256_inc_absorb(&ctx, &counter, 1);
      OQS_SHA3_sha3_256_inc_finalize(inner, &ctx);
      OQS_SHA3_sha3_256_inc_ctx_release(&ctx);
    }
    {
      OQS_SHA3_sha3_256_inc_ctx ctx;
      OQS_SHA3_sha3_256_inc_init(&ctx);
      OQS_SHA3_sha3_256_inc_absorb(&ctx, opad, kSha3_256RateBytes);
      OQS_SHA3_sha3_256_inc_absorb(&ctx, inner, kSha3_256DigestBytes);
      OQS_SHA3_sha3_256_inc_finalize(t_curr, &ctx);
      OQS_SHA3_sha3_256_inc_ctx_release(&ctx);
    }
    wipe(inner, sizeof(inner));

    const std::size_t remaining = okm_len - produced;
    const std::size_t take      = remaining < kSha3_256DigestBytes
                                      ? remaining
                                      : kSha3_256DigestBytes;
    std::memcpy(okm_bytes + produced, t_curr, take);
    produced += take;

    std::memcpy(t_prev, t_curr, kSha3_256DigestBytes);
  }

  wipe(prk,    sizeof(prk));
  wipe(ipad,   sizeof(ipad));
  wipe(opad,   sizeof(opad));
  wipe(t_prev, sizeof(t_prev));
  wipe(t_curr, sizeof(t_curr));
  return true;
}

Hash256 hkdf_sha3_256(const void* ikm,  std::size_t ikm_len,
                      const void* info, std::size_t info_len) noexcept {
  Hash256 out{};
  hkdf_sha3_256_explicit(ikm, ikm_len,
                         /*salt=*/nullptr, /*salt_len=*/0,
                         info, info_len,
                         out.data(), out.size());
  return out;
}

}  // namespace CryptoPQ
