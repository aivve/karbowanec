// KV binary roundtrip self-test for the queryblockslite response shape.
//
// The wallet's binary RPC client uses KV binary (storeToBinaryKeyValue /
// loadFromBinaryKeyValue). This test mirrors what the daemon sends back for
// a block that contains a v2 mixed-input transaction, then deserializes it
// the same way the wallet does. If this fails it's exactly the
// "Failed to parse binary response" the wallet logs as Network error.

#include <cassert>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <vector>

#include "CryptoNote.h"
#include "CryptoNoteConfig.h"
#include "CryptoNoteCore/CryptoNoteSerialization.h"
#include "CryptoNoteCore/CryptoNoteTools.h"
#include "CryptoNoteProtocol/CryptoNoteProtocolDefinitions.h"
#include "Rpc/CoreRpcServerCommandsDefinitions.h"
#include "Serialization/SerializationTools.h"
#include "Serialization/KVBinaryInputStreamSerializer.h"
#include "Common/MemoryInputStream.h"

using namespace CryptoNote;

namespace {

void fill(uint8_t* p, size_t n, uint8_t seed) {
  for (size_t i = 0; i < n; ++i) p[i] = static_cast<uint8_t>(seed + i);
}

template <class T>
T makePod(uint8_t seed) {
  T v;
  fill(reinterpret_cast<uint8_t*>(&v), sizeof(T), seed);
  return v;
}

TransactionPrefix buildV2PrefixCommon() {
  TransactionPrefix p;
  p.version = TRANSACTION_VERSION_CT;
  p.unlockTime = 0;
  p.fee = 10000000000ULL;
  p.extra = {0x01, 0xAA, 0xBB, 0xCC};

  // One ConfidentialOutput
  ConfidentialOutput cout;
  cout.targetKey = makePod<Crypto::PublicKey>(0x60);
  cout.commitment = makePod<Crypto::EllipticCurvePoint>(0x70);
  std::memset(cout.maskedAmount.data(), 0x80, 8);
  TransactionOutput out;
  out.amount = 0;
  out.target = cout;
  p.outputs.push_back(out);

  return p;
}

KeyInput makeKeyInput(uint8_t seed) {
  KeyInput ki;
  ki.amount = 100000000ULL;
  ki.outputIndexes = {7, 1, 4};
  ki.keyImage = makePod<Crypto::KeyImage>(seed);
  return ki;
}

ConfidentialInput makeConfidentialInput(uint8_t seed) {
  ConfidentialInput cin;
  for (size_t k = 0; k < 4; ++k) {
    cin.ringMembers.push_back(RingMemberRef{
        100000000ULL + k * 10, static_cast<uint32_t>(k)});
    cin.ringPubkeys.push_back(makePod<Crypto::PublicKey>(seed + 0x10 + static_cast<uint8_t>(k)));
    cin.ringCommitments.push_back(makePod<Crypto::EllipticCurvePoint>(seed + 0x20 + static_cast<uint8_t>(k)));
  }
  cin.pseudoCommitment = makePod<Crypto::EllipticCurvePoint>(seed + 0x30);
  cin.keyImage = makePod<Crypto::KeyImage>(seed + 0x40);
  return cin;
}

TransactionPrefix buildMixedV2Prefix() {
  TransactionPrefix p = buildV2PrefixCommon();
  p.inputs.push_back(makeKeyInput(0x10));
  p.inputs.push_back(makeConfidentialInput(0x20));
  return p;
}

TransactionPrefix buildPureCtV2Prefix() {
  TransactionPrefix p = buildV2PrefixCommon();
  p.inputs.push_back(makeConfidentialInput(0x10));
  p.inputs.push_back(makeConfidentialInput(0x30));
  return p;
}

TransactionPrefix buildPureKeyInputV2Prefix() {
  TransactionPrefix p = buildV2PrefixCommon();
  p.inputs.push_back(makeKeyInput(0x10));
  p.inputs.push_back(makeKeyInput(0x20));
  return p;
}

#define CHECK(cond)                                                            \
  do {                                                                         \
    if (!(cond)) {                                                             \
      std::fprintf(stderr, "FAIL @ %s:%d: %s\n", __FILE__, __LINE__, #cond);   \
      return 1;                                                                \
    }                                                                          \
  } while (0)

int tryRoundtrip(const char* label, const TransactionPrefix& src) {
  COMMAND_RPC_QUERY_BLOCKS_LITE::response rsp;
  rsp.startHeight = 100;
  rsp.currentHeight = 101;
  rsp.fullOffset = 100;
  rsp.status = "OK";

  BlockShortInfo item;
  item.blockId = makePod<Crypto::Hash>(0xFA);
  TransactionPrefixInfo tpi;
  tpi.txHash = makePod<Crypto::Hash>(0xFB);
  tpi.txPrefix = src;
  item.txPrefixes.push_back(std::move(tpi));
  rsp.items.push_back(std::move(item));

  std::string body = storeToBinaryKeyValue(rsp);
  std::printf("[%s] KV binary response: %zu bytes\n", label, body.size());

  COMMAND_RPC_QUERY_BLOCKS_LITE::response parsed;
  try {
    Common::MemoryInputStream stream(body.data(), body.size());
    KVBinaryInputStreamSerializer s(stream);
    serialize(parsed, s);
    std::printf("[%s] OK\n", label);
    return 0;
  } catch (const std::exception& e) {
    std::fprintf(stderr, "[%s] FAIL: KV deserialize threw: %s\n", label, e.what());
    return 1;
  }
}

int run() {
  int rc = 0;
  rc |= tryRoundtrip("pure-CT v2",        buildPureCtV2Prefix());
  rc |= tryRoundtrip("pure-KeyInput v2",  buildPureKeyInputV2Prefix());
  rc |= tryRoundtrip("mixed v2",          buildMixedV2Prefix());
  if (rc != 0) return 1;

  std::printf("OK: KV-binary v2 prefix round-trip (pure-CT, pure-KeyInput, mixed)\n");
  return 0;
}

}  // namespace

int main() {
  return run();
}
