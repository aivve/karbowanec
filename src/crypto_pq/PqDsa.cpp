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

#include "PqDsa.h"

#include <stdexcept>

extern "C" {
#include <oqs/sig_ml_dsa.h>
}

namespace CryptoPQ {

static_assert(kDsaPublicKeyBytes   == OQS_SIG_ml_dsa_65_length_public_key,    "ML-DSA-65 pubkey size mismatch");
static_assert(kDsaSecretKeyBytes   == OQS_SIG_ml_dsa_65_length_secret_key,    "ML-DSA-65 secret key size mismatch");
static_assert(kDsaSignatureBytes   == OQS_SIG_ml_dsa_65_length_signature,     "ML-DSA-65 signature size mismatch");
static_assert(kDsaKeypairSeedBytes == OQS_SIG_ml_dsa_65_length_keypair_seed,  "ML-DSA-65 keypair seed size mismatch");

std::pair<DsaPublicKey, DsaSecretKey> dsa_keygen() {
    DsaPublicKey pub;
    DsaSecretKey sk;
    OQS_STATUS rc = OQS_SIG_ml_dsa_65_keypair(pub.data(), sk.data());
    if (rc != OQS_SUCCESS) {
        throw std::runtime_error("OQS_SIG_ml_dsa_65_keypair failed");
    }
    return {pub, sk};
}

std::pair<DsaPublicKey, DsaSecretKey> dsa_keygen_from_seed(const DsaKeypairSeed& seed) {
    DsaPublicKey pub;
    DsaSecretKey sk;
    OQS_STATUS rc = OQS_SIG_ml_dsa_65_keypair_derand(pub.data(), sk.data(), seed.data());
    if (rc != OQS_SUCCESS) {
        throw std::runtime_error("OQS_SIG_ml_dsa_65_keypair_derand failed");
    }
    return {pub, sk};
}

DsaSignature dsa_sign(const DsaSecretKey& sk, const void* msg, std::size_t msg_len) {
    DsaSignature sig;
    std::size_t  sig_len = sig.size();
    OQS_STATUS rc = OQS_SIG_ml_dsa_65_sign(
        sig.data(), &sig_len,
        static_cast<const uint8_t*>(msg), msg_len,
        sk.data());
    if (rc != OQS_SUCCESS) {
        throw std::runtime_error("OQS_SIG_ml_dsa_65_sign failed");
    }
    if (sig_len != kDsaSignatureBytes) {
        // ML-DSA-65 signatures are fixed-length per FIPS 204; a different
        // length here would mean the upstream library shipped something
        // incompatible. Fail loudly rather than silently truncate.
        throw std::runtime_error("ML-DSA-65 signature length mismatch");
    }
    return sig;
}

bool dsa_verify(const DsaPublicKey& pub,
                const void* msg, std::size_t msg_len,
                const DsaSignature& sig) noexcept {
    OQS_STATUS rc = OQS_SIG_ml_dsa_65_verify(
        static_cast<const uint8_t*>(msg), msg_len,
        sig.data(), sig.size(),
        pub.data());
    return rc == OQS_SUCCESS;
}

}  // namespace CryptoPQ
