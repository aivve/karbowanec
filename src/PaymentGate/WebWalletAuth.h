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

#pragma once

#include <string>
#include <unordered_map>
#include <mutex>
#include <cstdint>

#include "crypto/crypto.h"
#include "crypto/hash.h"
#include "Common/JsonValue.h"
#include "Logging/LoggerRef.h"

namespace PaymentService {

struct AddressAuthRecord {
  std::string publicSpendKey;
  std::string tokenHash;
  uint64_t tokenExpiresAt;
  uint64_t registeredAt;
};

class WebWalletAuth {
public:
  WebWalletAuth(const std::string& sidecarPath, Logging::ILogger& logger);

  // Nonce management
  std::string generateNonce();
  bool consumeNonce(const std::string& nonce);

  // Address authentication
  bool verifyAddressSignature(const std::string& address,
                              const std::string& spendPublicKeyHex,
                              const std::string& nonce,
                              const std::string& signatureHex);

  // Token management
  std::string issueToken(const std::string& address, const std::string& spendPublicKeyHex);
  bool validateToken(const std::string& address, const std::string& token);
  bool revokeToken(const std::string& address);
  bool hasAddress(const std::string& address) const;

  // Persistence
  void load();
  void save();

  static const uint64_t NONCE_LIFETIME_SECONDS = 60;
  static const uint64_t TOKEN_LIFETIME_SECONDS = 86400; // 24 hours

private:
  void purgeExpiredNonces();
  std::string hashToken(const std::string& token) const;
  uint64_t now() const;

  std::string m_sidecarPath;
  Logging::LoggerRef m_logger;

  mutable std::mutex m_nonceMutex;
  std::unordered_map<std::string, uint64_t> m_nonces; // nonce -> expires_at

  mutable std::mutex m_authMutex;
  std::unordered_map<std::string, AddressAuthRecord> m_addresses; // address -> auth record
};

} // namespace PaymentService
