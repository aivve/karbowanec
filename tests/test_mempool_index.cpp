#include <cstring>
#include <exception>
#include <iostream>
#include <string>

#include <boost/filesystem.hpp>

#include "CryptoNoteConfig.h"
#include "CryptoNoteCore/Currency.h"
#include "CryptoNoteCore/TransactionPool.h"
#include "CryptoNoteCore/VerificationContext.h"
#include "Logging/ConsoleLogger.h"
#include "UnitTests/ICoreStub.h"

namespace {

class AcceptingTransactionValidator : public CryptoNote::ITransactionValidator {
public:
  bool checkTransactionInputs(const CryptoNote::Transaction& tx, CryptoNote::BlockInfo& maxUsedBlock) override {
    return true;
  }

  bool checkTransactionInputs(const CryptoNote::Transaction& tx, CryptoNote::BlockInfo& maxUsedBlock,
                              CryptoNote::BlockInfo& lastFailed) override {
    return true;
  }

  bool haveSpentKeyImages(const CryptoNote::Transaction& tx) override {
    return false;
  }

  bool checkTransactionSize(size_t blobSize) override {
    return true;
  }
};

class FixedTimeProvider : public CryptoNote::ITimeProvider {
public:
  explicit FixedTimeProvider(time_t currentTime) : timeNow(currentTime) {}

  time_t now() override {
    return timeNow;
  }

  time_t timeNow;
};

template<typename Pod>
void fillPod(Pod& value, uint8_t seed) {
  std::memset(&value, 0, sizeof(value));
  uint8_t* bytes = reinterpret_cast<uint8_t*>(&value);
  for (size_t i = 0; i < sizeof(value); ++i) {
    bytes[i] = static_cast<uint8_t>(seed + i);
  }
}

CryptoNote::Transaction makeConfidentialPoolTransaction(const Crypto::PublicKey& outputKey) {
  CryptoNote::Transaction tx;
  tx.version = CryptoNote::TRANSACTION_VERSION_CT;
  tx.unlockTime = 0;
  tx.fee = 1;

  CryptoNote::ConfidentialInput input;
  input.ringAmount = CryptoNote::parameters::CT_CONFIDENTIAL_OUTPUT_AMOUNT;
  Crypto::PublicKey ringKey;
  fillPod(ringKey, 0x11);
  input.ringPubkeys.push_back(ringKey);
  fillPod(input.ringCommitments.emplace_back(), 0x21);
  fillPod(input.pseudoCommitment, 0x31);
  fillPod(input.keyImage, 0x41);
  tx.inputs.push_back(input);

  CryptoNote::ConfidentialOutput output;
  output.targetKey = outputKey;
  fillPod(output.commitment, 0x51);
  output.maskedAmount.fill(0x61);

  CryptoNote::TransactionOutput txOutput;
  txOutput.amount = 0;
  txOutput.target = output;
  tx.outputs.push_back(txOutput);

  tx.ctSignatures.resize(tx.inputs.size());
  tx.ctSignatures[0].ss.resize(input.ringPubkeys.size());
  tx.ctProofs.resize(tx.outputs.size());

  return tx;
}

bool expect(bool condition, const char* message) {
  if (!condition) {
    std::cerr << message << std::endl;
    return false;
  }

  return true;
}

} // namespace

int main() {
  try {
    Logging::ConsoleLogger logger;
    logger.setMaxLevel(Logging::ERROR);

    CryptoNote::Currency currency = CryptoNote::CurrencyBuilder(logger)
      .txPoolFileName("mempool_index_test_pool.bin")
      .currency();

    const boost::filesystem::path configDir =
      boost::filesystem::temp_directory_path() / boost::filesystem::unique_path("karbo-mempool-index-%%%%-%%%%-%%%%");

    boost::filesystem::remove_all(configDir);

    AcceptingTransactionValidator validator;
    ICoreStub core;
    FixedTimeProvider timeProvider(1000);

    Crypto::PublicKey outputKey;
    fillPod(outputKey, 0x71);

    Crypto::Hash txId;
    fillPod(txId, 0x81);

    {
      CryptoNote::tx_memory_pool pool(currency, validator, core, timeProvider, logger);
      CryptoNote::Transaction tx = makeConfidentialPoolTransaction(outputKey);
      CryptoNote::tx_verification_context tvc;

      if (!expect(pool.init(configDir.string()), "failed to initialize first mempool")) {
        return 1;
      }
      if (!expect(pool.add_tx(tx, txId, 256, tvc, false), "failed to add CT transaction to first mempool")) {
        return 1;
      }

      Crypto::Hash resolvedTx;
      uint32_t resolvedOut = UINT32_MAX;
      if (!expect(pool.findOutputByPubkey(outputKey, resolvedTx, resolvedOut), "first mempool did not index CT output pubkey")) {
        return 1;
      }
      if (!expect(resolvedTx == txId && resolvedOut == 0, "first mempool resolved incorrect CT output")) {
        return 1;
      }
      if (!expect(pool.deinit(), "failed to persist first mempool")) {
        return 1;
      }
    }

    {
      CryptoNote::tx_memory_pool restartedPool(currency, validator, core, timeProvider, logger);
      if (!expect(restartedPool.init(configDir.string()), "failed to initialize restarted mempool")) {
        return 1;
      }

      Crypto::Hash resolvedTx;
      uint32_t resolvedOut = UINT32_MAX;
      if (!expect(restartedPool.findOutputByPubkey(outputKey, resolvedTx, resolvedOut),
                  "restarted mempool did not rebuild CT output pubkey index")) {
        return 1;
      }
      if (!expect(resolvedTx == txId && resolvedOut == 0, "restarted mempool resolved incorrect CT output")) {
        return 1;
      }

      if (!expect(restartedPool.deinit(), "failed to persist restarted mempool")) {
        return 1;
      }
    }

    boost::filesystem::remove_all(configDir);
    std::cout << "Mempool index restart test passed" << std::endl;
    return 0;
  } catch (const std::exception& e) {
    std::cerr << "unexpected exception: " << e.what() << std::endl;
    return 1;
  }
}
