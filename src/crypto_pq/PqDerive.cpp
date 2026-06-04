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

#include "PqDerive.h"

#include <cstring>

namespace CryptoPQ {

namespace {

// Append the bytes of a domain tag, excluding the trailing NUL terminator.
template <std::size_t N>
void appendDomain(std::vector<uint8_t>& buf, const char (&tag)[N]) {
  // N includes the NUL; hash N-1 bytes.
  buf.insert(buf.end(), tag, tag + (N - 1));
}

void appendBytes(std::vector<uint8_t>& buf, const void* p, std::size_t n) {
  const auto* b = static_cast<const uint8_t*>(p);
  buf.insert(buf.end(), b, b + n);
}

void appendLe32(std::vector<uint8_t>& buf, uint32_t v) {
  buf.push_back(static_cast<uint8_t>(v & 0xFF));
  buf.push_back(static_cast<uint8_t>((v >> 8) & 0xFF));
  buf.push_back(static_cast<uint8_t>((v >> 16) & 0xFF));
  buf.push_back(static_cast<uint8_t>((v >> 24) & 0xFF));
}

void appendLe64(std::vector<uint8_t>& buf, uint64_t v) {
  for (int i = 0; i < 8; ++i) {
    buf.push_back(static_cast<uint8_t>((v >> (8 * i)) & 0xFF));
  }
}

}  // namespace

Hash256 inputsHash(const std::vector<InputRef>& inputs) noexcept {
  std::vector<uint8_t> buf;
  buf.reserve(sizeof(kDomainInputsHash) + inputs.size() * 36);
  appendDomain(buf, kDomainInputsHash);
  for (const auto& in : inputs) {
    appendBytes(buf, in.prevTxid.data(), in.prevTxid.size());
    appendLe32(buf, in.prevOutIndex);
  }
  return sha3_256(buf.data(), buf.size());
}

Hash256 outContext(const Hash256& inputsHash,
                   const KemCiphertext& kemCt,
                   uint32_t outputIndex) noexcept {
  std::vector<uint8_t> buf;
  buf.reserve(sizeof(kDomainOutContext) + 32 + kemCt.size() + 4);
  appendDomain(buf, kDomainOutContext);
  appendBytes(buf, inputsHash.data(), inputsHash.size());
  appendBytes(buf, kemCt.data(), kemCt.size());
  appendLe32(buf, outputIndex);
  return sha3_256(buf.data(), buf.size());
}

Hash256 deriveSpendSeed(const KemShared& ss, const Hash256& outContext) noexcept {
  std::vector<uint8_t> info;
  info.reserve(sizeof(kDomainSpendSeed) + outContext.size());
  appendDomain(info, kDomainSpendSeed);
  appendBytes(info, outContext.data(), outContext.size());
  return hkdf_sha3_256(ss.data(), ss.size(), info.data(), info.size());
}

Hash256 deriveAeadKey(const KemShared& ss, const Hash256& outContext) noexcept {
  std::vector<uint8_t> info;
  info.reserve(sizeof(kDomainAeadKey) + outContext.size());
  appendDomain(info, kDomainAeadKey);
  appendBytes(info, outContext.data(), outContext.size());
  return hkdf_sha3_256(ss.data(), ss.size(), info.data(), info.size());
}

Hash256 spendCommit(const DsaPublicKey& pkI, const Rho& rho) noexcept {
  std::vector<uint8_t> buf;
  buf.reserve(sizeof(kDomainSpendCommit) + pkI.size() + rho.size());
  appendDomain(buf, kDomainSpendCommit);
  appendBytes(buf, pkI.data(), pkI.size());
  appendBytes(buf, rho.data(), rho.size());
  return sha3_256(buf.data(), buf.size());
}

Hash256 nullifier(const DsaPublicKey& pkI, const Rho& rho) noexcept {
  std::vector<uint8_t> buf;
  buf.reserve(sizeof(kDomainNullifier) + pkI.size() + rho.size());
  appendDomain(buf, kDomainNullifier);
  appendBytes(buf, pkI.data(), pkI.size());
  appendBytes(buf, rho.data(), rho.size());
  return sha3_256(buf.data(), buf.size());
}

Hash256 txSigningDigest(const UnsignedTx& tx) noexcept {
  std::vector<uint8_t> buf;
  appendDomain(buf, kDomainTxSign);
  buf.push_back(tx.version);
  appendLe32(buf, static_cast<uint32_t>(tx.inputs.size()));
  for (const auto& in : tx.inputs) {
    appendBytes(buf, in.prevTxid.data(), in.prevTxid.size());
    appendLe32(buf, in.prevOutIndex);
    appendBytes(buf, in.authPub.data(), in.authPub.size());
    appendBytes(buf, in.rhoReveal.data(), in.rhoReveal.size());
  }
  appendLe32(buf, static_cast<uint32_t>(tx.outputs.size()));
  for (const auto& out : tx.outputs) {
    buf.push_back(out.type);
    appendLe64(buf, out.amount);
    appendBytes(buf, out.kemCt.data(), out.kemCt.size());
    appendBytes(buf, out.encPayload.data(), out.encPayload.size());
    appendBytes(buf, out.spendCommit.data(), out.spendCommit.size());
  }
  appendLe64(buf, tx.fee);
  return sha3_256(buf.data(), buf.size());
}

}  // namespace CryptoPQ
