// Copyright (c) 2016-2026, The Karbo developers
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

#include "WebWalletAuth.h"

#include <chrono>
#include <fstream>
#include <sstream>

#include "Common/StringTools.h"
#include "crypto/crypto.h"
#include "crypto/hash.h"
#include "crypto/random.h"

namespace PaymentService {

WebWalletAuth::WebWalletAuth(const std::string& sidecarPath, Logging::ILogger& logger)
  : m_sidecarPath(sidecarPath)
  , m_logger(logger, "WebWalletAuth")
{
}

uint64_t WebWalletAuth::now() const {
  return static_cast<uint64_t>(
    std::chrono::duration_cast<std::chrono::seconds>(
      std::chrono::system_clock::now().time_since_epoch()
    ).count()
  );
}

// --- Nonce management ---

std::string WebWalletAuth::generateNonce() {
  std::lock_guard<std::mutex> lock(m_nonceMutex);

  purgeExpiredNonces();

  // Generate 32 random bytes
  uint8_t nonceBytes[32];
  Random::randomBytes(32, nonceBytes);
  std::string nonce = Common::toHex(nonceBytes, 32);

  m_nonces[nonce] = now() + NONCE_LIFETIME_SECONDS;

  m_logger(Logging::DEBUGGING) << "Generated nonce, active nonces: " << m_nonces.size();
  return nonce;
}

bool WebWalletAuth::consumeNonce(const std::string& nonce) {
  std::lock_guard<std::mutex> lock(m_nonceMutex);

  auto it = m_nonces.find(nonce);
  if (it == m_nonces.end()) {
    return false;
  }

  bool valid = it->second > now();
  m_nonces.erase(it);
  return valid;
}

void WebWalletAuth::purgeExpiredNonces() {
  uint64_t currentTime = now();
  for (auto it = m_nonces.begin(); it != m_nonces.end(); ) {
    if (it->second <= currentTime) {
      it = m_nonces.erase(it);
    } else {
      ++it;
    }
  }
}

// --- Signature verification ---

bool WebWalletAuth::verifyAddressSignature(
    const std::string& address,
    const std::string& spendPublicKeyHex,
    const std::string& nonce,
    const std::string& signatureHex)
{
  Crypto::PublicKey publicKey;
  if (!Common::podFromHex(spendPublicKeyHex, publicKey)) {
    m_logger(Logging::WARNING) << "Invalid public key format: " << spendPublicKeyHex;
    return false;
  }

  Crypto::Signature signature;
  if (!Common::podFromHex(signatureHex, signature)) {
    m_logger(Logging::WARNING) << "Invalid signature format";
    return false;
  }

  // Hash the message: address + nonce
  std::string message = address + nonce;
  Crypto::Hash messageHash;
  Crypto::cn_fast_hash(message.data(), message.size(), messageHash);

  bool valid = Crypto::check_signature(messageHash, publicKey, signature);
  if (!valid) {
    m_logger(Logging::WARNING) << "Signature verification failed for address: " << address;
  }
  return valid;
}

// --- Token management ---

std::string WebWalletAuth::hashToken(const std::string& token) const {
  Crypto::Hash hash;
  Crypto::cn_fast_hash(token.data(), token.size(), hash);
  return Common::podToHex(hash);
}

std::string WebWalletAuth::issueToken(const std::string& address, const std::string& spendPublicKeyHex) {
  std::lock_guard<std::mutex> lock(m_authMutex);

  // Generate 256-bit random token
  uint8_t tokenBytes[32];
  Random::randomBytes(32, tokenBytes);
  std::string token = Common::toHex(tokenBytes, 32);

  AddressAuthRecord record;
  record.publicSpendKey = spendPublicKeyHex;
  record.tokenHash = hashToken(token);
  record.tokenExpiresAt = now() + TOKEN_LIFETIME_SECONDS;

  auto it = m_addresses.find(address);
  if (it != m_addresses.end()) {
    record.registeredAt = it->second.registeredAt;
  } else {
    record.registeredAt = now();
  }

  m_addresses[address] = record;

  save();

  m_logger(Logging::DEBUGGING) << "Issued token for address: " << address;
  return token;
}

bool WebWalletAuth::validateToken(const std::string& address, const std::string& token) {
  std::lock_guard<std::mutex> lock(m_authMutex);

  auto it = m_addresses.find(address);
  if (it == m_addresses.end()) {
    return false;
  }

  if (it->second.tokenExpiresAt <= now()) {
    m_logger(Logging::DEBUGGING) << "Token expired for address: " << address;
    return false;
  }

  return it->second.tokenHash == hashToken(token);
}

bool WebWalletAuth::revokeToken(const std::string& address) {
  std::lock_guard<std::mutex> lock(m_authMutex);

  auto it = m_addresses.find(address);
  if (it == m_addresses.end()) {
    return false;
  }

  m_addresses.erase(it);
  save();
  return true;
}

bool WebWalletAuth::hasAddress(const std::string& address) const {
  std::lock_guard<std::mutex> lock(m_authMutex);
  return m_addresses.count(address) > 0;
}

// --- Persistence ---

void WebWalletAuth::load() {
  std::lock_guard<std::mutex> lock(m_authMutex);

  std::ifstream file(m_sidecarPath);
  if (!file.is_open()) {
    m_logger(Logging::INFO) << "Auth sidecar file not found, starting with empty auth state";
    return;
  }

  try {
    std::stringstream buffer;
    buffer << file.rdbuf();
    std::string content = buffer.str();

    if (content.empty()) {
      return;
    }

    Common::JsonValue root = Common::JsonValue::fromString(content);
    if (!root.contains("addresses") || !root("addresses").isObject()) {
      m_logger(Logging::WARNING) << "Auth sidecar file has invalid format, resetting";
      return;
    }

    const auto& addressesObj = root("addresses").getObject();
    for (const auto& kv : addressesObj) {
      const std::string& addr = kv.first;
      const auto& rec = kv.second;

      AddressAuthRecord record;
      record.publicSpendKey = rec("publicSpendKey").getString();
      record.tokenHash = rec("tokenHash").getString();
      record.tokenExpiresAt = static_cast<uint64_t>(rec("tokenExpiresAt").getInteger());
      record.registeredAt = static_cast<uint64_t>(rec("registeredAt").getInteger());

      m_addresses[addr] = record;
    }

    m_logger(Logging::INFO) << "Loaded " << m_addresses.size() << " address auth records";
  } catch (const std::exception& e) {
    m_logger(Logging::WARNING) << "Failed to parse auth sidecar file: " << e.what()
      << ". All addresses will require re-authentication.";
    m_addresses.clear();
  }
}

void WebWalletAuth::save() {
  // Caller must hold m_authMutex

  try {
    Common::JsonValue root(Common::JsonValue::OBJECT);
    Common::JsonValue addresses(Common::JsonValue::OBJECT);

    for (const auto& kv : m_addresses) {
      Common::JsonValue record(Common::JsonValue::OBJECT);
      record.insert("publicSpendKey", kv.second.publicSpendKey);
      record.insert("tokenHash", kv.second.tokenHash);
      record.insert("tokenExpiresAt", static_cast<int64_t>(kv.second.tokenExpiresAt));
      record.insert("registeredAt", static_cast<int64_t>(kv.second.registeredAt));
      addresses.insert(kv.first, record);
    }

    root.insert("addresses", addresses);

    std::string content = root.toString();

    std::ofstream file(m_sidecarPath, std::ios::trunc);
    if (!file.is_open()) {
      m_logger(Logging::ERROR) << "Failed to open auth sidecar file for writing: " << m_sidecarPath;
      return;
    }

    file << content;
    file.close();

    m_logger(Logging::DEBUGGING) << "Saved " << m_addresses.size() << " address auth records";
  } catch (const std::exception& e) {
    m_logger(Logging::ERROR) << "Failed to save auth sidecar file: " << e.what();
  }
}

} // namespace PaymentService
