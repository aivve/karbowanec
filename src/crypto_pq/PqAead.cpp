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

#include "PqAead.h"

extern "C" {
#include <chacha20poly1305.h>
}

namespace CryptoPQ {

std::vector<uint8_t> aead_encrypt(
    const AeadKey&   key,
    const AeadNonce& nonce,
    const void* aad, std::size_t aad_len,
    const void* msg, std::size_t msg_len)
{
    std::vector<uint8_t> out(msg_len + kAeadTagBytes);
    chacha20poly1305_ietf_encrypt(
        out.data(),
        static_cast<const uint8_t*>(aad), aad_len,
        static_cast<const uint8_t*>(msg), msg_len,
        key.data(), nonce.data());
    return out;
}

std::optional<std::vector<uint8_t>> aead_decrypt(
    const AeadKey&   key,
    const AeadNonce& nonce,
    const void* aad, std::size_t aad_len,
    const void* in,  std::size_t in_len)
{
    if (in_len < kAeadTagBytes) {
        return std::nullopt;
    }
    std::vector<uint8_t> out(in_len - kAeadTagBytes);
    int rc = chacha20poly1305_ietf_decrypt(
        out.data(),
        static_cast<const uint8_t*>(aad), aad_len,
        static_cast<const uint8_t*>(in), in_len,
        key.data(), nonce.data());
    if (rc != 0) {
        return std::nullopt;
    }
    return out;
}

}  // namespace CryptoPQ
