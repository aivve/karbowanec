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

#include "PqKem.h"

#include <stdexcept>

extern "C" {
#include <oqs/kem_ml_kem.h>
}

namespace CryptoPQ {

static_assert(kKemPublicKeyBytes  == OQS_KEM_ml_kem_768_length_public_key,    "ML-KEM-768 pubkey size mismatch");
static_assert(kKemSecretKeyBytes  == OQS_KEM_ml_kem_768_length_secret_key,    "ML-KEM-768 secret key size mismatch");
static_assert(kKemCiphertextBytes == OQS_KEM_ml_kem_768_length_ciphertext,    "ML-KEM-768 ciphertext size mismatch");
static_assert(kKemSharedBytes     == OQS_KEM_ml_kem_768_length_shared_secret, "ML-KEM-768 shared secret size mismatch");
static_assert(kKemKeypairSeedBytes == OQS_KEM_ml_kem_768_length_keypair_seed, "ML-KEM-768 keypair seed size mismatch");

std::pair<KemPublicKey, KemSecretKey> kem_keygen() {
    KemPublicKey pub;
    KemSecretKey sk;
    OQS_STATUS rc = OQS_KEM_ml_kem_768_keypair(pub.data(), sk.data());
    if (rc != OQS_SUCCESS) {
        throw std::runtime_error("OQS_KEM_ml_kem_768_keypair failed");
    }
    return {pub, sk};
}

std::pair<KemPublicKey, KemSecretKey> kem_keygen_from_seed(const KemKeypairSeed& seed) {
    KemPublicKey pub;
    KemSecretKey sk;
    OQS_STATUS rc = OQS_KEM_ml_kem_768_keypair_derand(pub.data(), sk.data(), seed.data());
    if (rc != OQS_SUCCESS) {
        throw std::runtime_error("OQS_KEM_ml_kem_768_keypair_derand failed");
    }
    return {pub, sk};
}

std::pair<KemCiphertext, KemShared> kem_encaps(const KemPublicKey& pub) {
    KemCiphertext ct;
    KemShared     ss;
    OQS_STATUS rc = OQS_KEM_ml_kem_768_encaps(ct.data(), ss.data(), pub.data());
    if (rc != OQS_SUCCESS) {
        throw std::runtime_error("OQS_KEM_ml_kem_768_encaps failed");
    }
    return {ct, ss};
}

KemShared kem_decaps(const KemSecretKey& sk, const KemCiphertext& ct) {
    KemShared ss;
    // FIPS 203 §7.3 (implicit rejection): on malformed ct this returns
    // pseudorandom bytes derived from sk and ct, NOT an error. liboqs mirrors
    // that behaviour and returns OQS_SUCCESS in both the valid and the
    // implicit-rejection path. We propagate the value unchanged — discarding
    // it via "tag failure" higher up is the spec-defined channel for
    // detecting non-owned outputs.
    OQS_STATUS rc = OQS_KEM_ml_kem_768_decaps(ss.data(), ct.data(), sk.data());
    if (rc != OQS_SUCCESS) {
        // Reachable only on internal errors (e.g. allocator failure); not on
        // adversarial ciphertexts. Treat as a hard fault.
        throw std::runtime_error("OQS_KEM_ml_kem_768_decaps failed");
    }
    return ss;
}

}  // namespace CryptoPQ
