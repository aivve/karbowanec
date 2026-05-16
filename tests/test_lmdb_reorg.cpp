#include "CryptoNoteCore/Account.h"
#include "CryptoNoteCore/Core.h"
#include "CryptoNoteCore/CoreConfig.h"
#include "CryptoNoteCore/Currency.h"
#include "CryptoNoteCore/LMDBBlockchainDB.h"
#include "CryptoNoteCore/MinerConfig.h"
#include "Logging/ConsoleLogger.h"
#include "System/Dispatcher.h"
#include "TestGenerator/TestGenerator.h"
#include "liblmdb/lmdb.h"

#include <ctime>
#include <cstring>
#include <filesystem>
#include <iostream>
#include <list>
#include <stdexcept>
#include <string>
#include <vector>

namespace {

bool expect(bool condition, const std::string& message) {
  if (!condition) {
    std::cerr << "[FAIL] " << message << std::endl;
    return false;
  }
  return true;
}

#pragma pack(push, 1)
struct LegacyDbBlockMeta {
  uint8_t  hash[32];
  uint8_t  prevHash[32];
  uint64_t timestamp;
  uint64_t cumulativeDifficulty;
  uint64_t alreadyGeneratedCoins;
  uint32_t blockCumulativeSize;
  uint32_t height;
  uint16_t txCount;
  uint8_t  majorVersion;
  uint8_t  minorVersion;
};
#pragma pack(pop)
static_assert(sizeof(LegacyDbBlockMeta) == 100, "LegacyDbBlockMeta must be 100 bytes");

void encodeBE32(uint8_t* out, uint32_t value) {
  out[0] = static_cast<uint8_t>((value >> 24) & 0xFF);
  out[1] = static_cast<uint8_t>((value >> 16) & 0xFF);
  out[2] = static_cast<uint8_t>((value >> 8) & 0xFF);
  out[3] = static_cast<uint8_t>(value & 0xFF);
}

bool writeLegacyBlockMeta(const std::filesystem::path& dataDir, uint32_t height) {
  MDB_env* env = nullptr;
  MDB_txn* txn = nullptr;
  MDB_dbi blockMetaDbi = 0;

  int rc = mdb_env_create(&env);
  if (rc != 0) return false;

  rc = mdb_env_set_maxdbs(env, 16);
  if (rc == 0) rc = mdb_env_set_mapsize(env, size_t(1) << 20);
  if (rc == 0) rc = mdb_env_open(env, dataDir.string().c_str(), MDB_NORDAHEAD, 0664);
  if (rc == 0) rc = mdb_txn_begin(env, nullptr, 0, &txn);
  if (rc == 0) rc = mdb_dbi_open(txn, "block_meta", MDB_CREATE, &blockMetaDbi);

  LegacyDbBlockMeta legacy{};
  for (size_t i = 0; i < sizeof(legacy.hash); ++i) {
    legacy.hash[i] = static_cast<uint8_t>(i + 1);
    legacy.prevHash[i] = static_cast<uint8_t>(0xF0 - i);
  }
  legacy.timestamp = 123456;
  legacy.cumulativeDifficulty = 789;
  legacy.alreadyGeneratedCoins = 456;
  legacy.blockCumulativeSize = 321;
  legacy.height = height;
  legacy.txCount = 2;
  legacy.majorVersion = CryptoNote::BLOCK_MAJOR_VERSION_1;
  legacy.minorVersion = CryptoNote::BLOCK_MINOR_VERSION_0;

  uint8_t keyBuf[4];
  encodeBE32(keyBuf, height);
  MDB_val key{sizeof(keyBuf), keyBuf};
  MDB_val value{sizeof(legacy), &legacy};
  if (rc == 0) rc = mdb_put(txn, blockMetaDbi, &key, &value, 0);

  if (rc == 0) {
    rc = mdb_txn_commit(txn);
    txn = nullptr;
  }

  if (txn != nullptr) {
    mdb_txn_abort(txn);
  }
  if (env != nullptr) {
    mdb_env_close(env);
  }

  return rc == 0;
}

bool runLegacyMetaReadScenario() {
  const std::filesystem::path dataDir =
      std::filesystem::path("lmdb_legacy_meta_test_data");
  std::error_code ec;
  std::filesystem::remove_all(dataDir, ec);
  std::filesystem::create_directories(dataDir, ec);
  if (ec) {
    std::cerr << "[FAIL] could not create legacy metadata test directory: " << ec.message() << std::endl;
    return false;
  }

  constexpr uint32_t legacyHeight = 60000;
  if (!expect(writeLegacyBlockMeta(dataDir, legacyHeight), "failed to write legacy block_meta record")) {
    std::filesystem::remove_all(dataDir, ec);
    return false;
  }

  CryptoNote::LMDBBlockchainDB db;
  if (!expect(db.open(dataDir.string()), "failed to open legacy metadata DB")) {
    std::filesystem::remove_all(dataDir, ec);
    return false;
  }

  CryptoNote::DbBlockMeta meta{};
  if (!expect(db.getBlockMeta(legacyHeight, meta), "failed to decode legacy block_meta record")) {
    db.close();
    std::filesystem::remove_all(dataDir, ec);
    return false;
  }
  if (!expect(meta.height == legacyHeight, "legacy block_meta height mismatch") ||
      !expect(meta.majorVersion == CryptoNote::BLOCK_MAJOR_VERSION_1, "legacy block_meta major version mismatch") ||
      !expect(meta.minorVersion == CryptoNote::BLOCK_MINOR_VERSION_0, "legacy block_meta minor version mismatch") ||
      !expect(meta.confidentialSupply == 0, "legacy block_meta confidential supply should default to zero") ||
      !expect(meta.pqPlainSupply == 0, "legacy block_meta PQ plain supply should default to zero")) {
    db.close();
    std::filesystem::remove_all(dataDir, ec);
    return false;
  }

  std::vector<CryptoNote::DbBlockMeta> metas;
  if (!expect(db.getBlockMetaRange(legacyHeight, legacyHeight, metas), "failed to range-read legacy block_meta record") ||
      !expect(metas.size() == 1, "legacy block_meta range read returned wrong count") ||
      !expect(metas.front().majorVersion == CryptoNote::BLOCK_MAJOR_VERSION_1, "legacy block_meta range major version mismatch")) {
    db.close();
    std::filesystem::remove_all(dataDir, ec);
    return false;
  }

  db.close();
  std::filesystem::remove_all(dataDir, ec);
  return true;
}

bool submitBlock(CryptoNote::Core& core,
                 const CryptoNote::Block& block,
                 bool expectMainChain,
                 bool expectSwitchToAlt,
                 const std::string& label) {
  CryptoNote::block_verification_context bvc{};
  core.handle_incoming_block(block, bvc, false, false);

  if (!expect(!bvc.m_verification_failed, label + ": verification failed")) return false;
  if (!expect(!bvc.m_marked_as_orphaned, label + ": block marked as orphan")) return false;
  if (!expect(bvc.m_added_to_main_chain == expectMainChain, label + ": unexpected m_added_to_main_chain")) return false;
  if (!expect(bvc.m_switched_to_alt_chain == expectSwitchToAlt, label + ": unexpected m_switched_to_alt_chain")) return false;
  return true;
}

bool buildMainBlock(CryptoNote::Core& core,
                    const CryptoNote::Currency& currency,
                    test_generator& generator,
                    const CryptoNote::AccountBase& miner,
                    uint64_t timestamp,
                    CryptoNote::Block& out) {
  uint32_t chainHeight = core.getCurrentBlockchainHeight();
  Crypto::Hash tailHash = core.get_tail_id();

  uint64_t alreadyGenerated = 0;
  if (!core.getAlreadyGeneratedCoins(tailHash, alreadyGenerated)) {
    return false;
  }

  std::vector<size_t> blockSizes;
  if (!core.getBackwardBlocksSizes(chainHeight - 1, blockSizes, currency.rewardBlocksWindow())) {
    return false;
  }

  std::list<CryptoNote::Transaction> txList;
  if (!generator.constructBlock(out, chainHeight, tailHash, miner, timestamp,
                                alreadyGenerated, blockSizes, txList)) {
    return false;
  }

  const CryptoNote::difficulty_type difficulty = core.getNextBlockDifficulty();
  if (difficulty > 1) {
    fillNonce(out, difficulty);
  }

  // Re-index with the final hash (constructBlock() indexed an internal nonce variant).
  generator.addBlock(out, 0, 0, blockSizes, alreadyGenerated);
  return true;
}

bool buildAlternativeBlock(CryptoNote::Core& core,
                           const CryptoNote::Currency& currency,
                           test_generator& generator,
                           const CryptoNote::AccountBase& miner,
                           const CryptoNote::Block& prev,
                           CryptoNote::Block& out) {
  const Crypto::Hash prevHash = get_block_hash(prev);
  const uint32_t nextHeight = get_block_height(prev) + 1;
  const uint64_t timestamp = prev.timestamp + currency.difficultyTarget();

  uint64_t alreadyGenerated = 0;
  try {
    alreadyGenerated = generator.getAlreadyGeneratedCoins(prevHash);
  } catch (const std::exception&) {
    return false;
  }

  std::vector<size_t> blockSizes;
  try {
    generator.getLastNBlockSizes(blockSizes, prevHash, currency.rewardBlocksWindow());
  } catch (const std::exception&) {
    return false;
  }

  std::list<CryptoNote::Transaction> txList;
  if (!generator.constructBlock(out, nextHeight, prevHash, miner, timestamp,
                                alreadyGenerated, blockSizes, txList)) {
    return false;
  }

  const CryptoNote::difficulty_type difficulty =
      core.get_blockchain_storage().getDifficultyForNextBlock(prevHash);
  if (difficulty > 1) {
    fillNonce(out, difficulty);
  }

  // Re-index with the final hash (constructBlock() indexed an internal nonce variant).
  generator.addBlock(out, 0, 0, blockSizes, alreadyGenerated);
  return true;
}

bool runReorgScenario() {
  Logging::ConsoleLogger logger;
  const CryptoNote::Currency currency = CryptoNote::CurrencyBuilder(logger).currency();

  const std::filesystem::path dataDir =
      std::filesystem::path("lmdb_reorg_test_data");
  std::error_code ec;
  std::filesystem::remove_all(dataDir, ec);
  std::filesystem::create_directories(dataDir, ec);
  if (ec) {
    std::cerr << "[FAIL] could not create data directory: " << ec.message() << std::endl;
    return false;
  }

  System::Dispatcher dispatcher;
  CryptoNote::Core core(currency, nullptr, logger, dispatcher, 0, false);
  CryptoNote::CoreConfig coreConfig;
  coreConfig.configFolder = dataDir.string();
  CryptoNote::MinerConfig minerConfig;

  if (!expect(core.init(coreConfig, minerConfig, false), "core.init failed")) {
    std::filesystem::remove_all(dataDir, ec);
    return false;
  }

  test_generator generator(currency);
  CryptoNote::AccountBase miner;
  miner.generate();

  std::vector<CryptoNote::Block> mainChain;
  mainChain.reserve(16);

  Crypto::Hash genesisHash = core.getBlockIdByHeight(0);
  CryptoNote::Block genesis;
  if (!expect(core.getBlockByHash(genesisHash, genesis), "failed to load genesis block")) {
    core.deinit();
    std::filesystem::remove_all(dataDir, ec);
    return false;
  }

  // Seed generator metadata with genesis so backward-size walks from forks work.
  std::vector<size_t> emptySizes;
  generator.addBlock(genesis, 0, 0, emptySizes, 0);
  mainChain.push_back(genesis);

  const uint64_t now = static_cast<uint64_t>(std::time(nullptr));
  const uint64_t startTimestamp = now - 24 * 60 * 60; // keep synthetic chain well behind wall clock
  for (size_t i = 0; i < 15; ++i) {
    CryptoNote::Block block;
    const uint64_t timestamp = (i == 0) ? startTimestamp : (mainChain.back().timestamp + currency.difficultyTarget());
    if (!expect(buildMainBlock(core, currency, generator, miner, timestamp, block),
                "failed to construct main-chain block")) {
      core.deinit();
      std::filesystem::remove_all(dataDir, ec);
      return false;
    }

    if (!expect(submitBlock(core, block, true, false, "submit main block"),
                "main block submit checks failed")) {
      core.deinit();
      std::filesystem::remove_all(dataDir, ec);
      return false;
    }

    mainChain.push_back(block);
  }

  if (!expect(core.getCurrentBlockchainHeight() == 16, "unexpected main chain height after growth")) {
    core.deinit();
    std::filesystem::remove_all(dataDir, ec);
    return false;
  }

  const Crypto::Hash oldMainTipHash = get_block_hash(mainChain.back());
  const Crypto::Hash oldMainTipCoinbase = getObjectHash(mainChain.back().baseTransaction);

  const CryptoNote::Block forkBase = mainChain[9]; // height 9
  CryptoNote::Block altPrev = forkBase;
  std::vector<CryptoNote::Block> altChain;
  altChain.reserve(7);

  for (size_t i = 0; i < 7; ++i) {
    CryptoNote::Block altBlock;
    if (!expect(buildAlternativeBlock(core, currency, generator, miner, altPrev, altBlock),
                "failed to construct alt-chain block")) {
      core.deinit();
      std::filesystem::remove_all(dataDir, ec);
      return false;
    }

    const bool shouldSwitch = (i == 6);
    if (!expect(submitBlock(core, altBlock, shouldSwitch, shouldSwitch, "submit alt block"),
                "alt block submit checks failed")) {
      core.deinit();
      std::filesystem::remove_all(dataDir, ec);
      return false;
    }

    altChain.push_back(altBlock);
    altPrev = altBlock;
  }

  uint32_t topHeight = 0;
  Crypto::Hash topHash = Crypto::Hash();
  core.get_blockchain_top(topHeight, topHash);

  const Crypto::Hash newMainTipHash = get_block_hash(altChain.back());
  const Crypto::Hash newMainTipCoinbase = getObjectHash(altChain.back().baseTransaction);

  if (!expect(core.getCurrentBlockchainHeight() == 17, "unexpected height after reorg")) {
    core.deinit();
    std::filesystem::remove_all(dataDir, ec);
    return false;
  }
  if (!expect(topHeight == 16, "unexpected top height after reorg")) {
    core.deinit();
    std::filesystem::remove_all(dataDir, ec);
    return false;
  }
  if (!expect(topHash == newMainTipHash, "unexpected top hash after reorg")) {
    core.deinit();
    std::filesystem::remove_all(dataDir, ec);
    return false;
  }
  if (!expect(core.getBlockIdByHeight(16) == newMainTipHash, "main-chain tip id lookup mismatch")) {
    core.deinit();
    std::filesystem::remove_all(dataDir, ec);
    return false;
  }
  if (!expect(core.getBlockIdByHeight(15) != oldMainTipHash, "old main tip still visible at height 15")) {
    core.deinit();
    std::filesystem::remove_all(dataDir, ec);
    return false;
  }

  Crypto::Hash locatedBlock = Crypto::Hash();
  uint32_t locatedHeight = 0;
  if (!expect(!core.getBlockContainingTx(oldMainTipCoinbase, locatedBlock, locatedHeight),
              "old-branch coinbase is still indexed as main-chain tx")) {
    core.deinit();
    std::filesystem::remove_all(dataDir, ec);
    return false;
  }
  if (!expect(core.getBlockContainingTx(newMainTipCoinbase, locatedBlock, locatedHeight),
              "new main tip coinbase is missing from tx index")) {
    core.deinit();
    std::filesystem::remove_all(dataDir, ec);
    return false;
  }
  if (!expect(locatedBlock == newMainTipHash && locatedHeight == 16,
              "new main tip coinbase resolved to wrong block")) {
    core.deinit();
    std::filesystem::remove_all(dataDir, ec);
    return false;
  }

  core.rollbackBlockchain(9);
  core.get_blockchain_top(topHeight, topHash);

  const Crypto::Hash expectedRollbackTip = get_block_hash(mainChain[9]);
  if (!expect(core.getCurrentBlockchainHeight() == 10, "unexpected height after rollback")) {
    core.deinit();
    std::filesystem::remove_all(dataDir, ec);
    return false;
  }
  if (!expect(topHeight == 9 && topHash == expectedRollbackTip, "rollback tip mismatch")) {
    core.deinit();
    std::filesystem::remove_all(dataDir, ec);
    return false;
  }
  if (!expect(!core.getBlockContainingTx(newMainTipCoinbase, locatedBlock, locatedHeight),
              "alt-branch coinbase is still indexed after rollback")) {
    core.deinit();
    std::filesystem::remove_all(dataDir, ec);
    return false;
  }

  core.deinit();
  std::filesystem::remove_all(dataDir, ec);
  return true;
}

} // namespace

int main() {
  if (!runLegacyMetaReadScenario()) {
    return 1;
  }

  if (!runReorgScenario()) {
    return 1;
  }

  std::cout << "[PASS] LMDB legacy metadata and reorg scenarios" << std::endl;
  return 0;
}
