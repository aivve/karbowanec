// Copyright (c) 2012-2016, The CryptoNote developers, The Bytecoin developers, The Monero developers
// Copyright (c) 2018, Ryo Currency Project
// Copyright (c) 2016-2020, The Karbo developers
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

#include "Blockchain.h"

#include <algorithm>
#include <numeric>
#include <cstdio>
#include <cmath>
#include <boost/foreach.hpp>
#include "Common/Math.h"
#include "Common/int-util.h"
#include "Common/ShuffleGenerator.h"
#include "Common/StringTools.h"
#include "Common/StdInputStream.h"
#include "Common/StdOutputStream.h"
#include "Common/Varint.hpp"
#include "Rpc/CoreRpcServerCommandsDefinitions.h"
#include "Serialization/BinarySerializationTools.h"
#include "Serialization/SerializationTools.h"
#include "BlockchainExplorer/BlockchainExplorerDataBuilder.h"
#include "CryptoNoteTools.h"
#include "TransactionExtra.h"

using namespace Logging;
using namespace Common;

namespace {

std::string appendPath(const std::string& path, const std::string& fileName) {
  std::string result = path;
  if (!result.empty()) {
    result += '/';
  }

  result += fileName;
  return result;
}

// for debug print
template <typename T>
static bool print_as_json(const T& obj) {
  std::cout << CryptoNote::storeToJson(obj) << ENDL;
  return true;
}

}

namespace std {
bool operator<(const Crypto::Hash& hash1, const Crypto::Hash& hash2) {
  return memcmp(&hash1, &hash2, Crypto::HASH_SIZE) < 0;
}

bool operator<(const Crypto::KeyImage& keyImage1, const Crypto::KeyImage& keyImage2) {
  return memcmp(&keyImage1, &keyImage2, 32) < 0;
}
}

#define CURRENT_BLOCKCACHE_STORAGE_ARCHIVE_VER 2
#define CURRENT_BLOCKCHAININDICES_STORAGE_ARCHIVE_VER 1

namespace CryptoNote {
class BlockCacheSerializer;
class BlockchainIndicesSerializer;
}

namespace CryptoNote {

template<typename K, typename V, typename Hash>
bool serialize(google::sparse_hash_map<K, V, Hash>& value, Common::StringView name, CryptoNote::ISerializer& serializer) {
  return serializeMap(value, name, serializer, [&value](size_t size) { value.resize(size); });
}

template<typename K, typename Hash>
bool serialize(google::sparse_hash_set<K, Hash>& value, Common::StringView name, CryptoNote::ISerializer& serializer) {
  size_t size = value.size();
  if (!serializer.beginArray(size, name)) {
    return false;
  }

  if (serializer.type() == ISerializer::OUTPUT) {
    for (auto& key : value) {
      serializer(const_cast<K&>(key), "");
    }
  } else {
    value.resize(size);
    while (size--) {
      K key;
      serializer(key, "");
      value.insert(key);
    }
  }

  serializer.endArray();
  return true;
}

// custom serialization to speedup cache loading
bool serialize(std::vector<std::pair<Blockchain::TransactionIndex, uint16_t>>& value, Common::StringView name, CryptoNote::ISerializer& s) {
  const size_t elementSize = sizeof(std::pair<Blockchain::TransactionIndex, uint16_t>);
  size_t size = value.size() * elementSize;

  if (!s.beginArray(size, name)) {
    return false;
  }

  if (s.type() == CryptoNote::ISerializer::INPUT) {
    if (size % elementSize != 0) {
      throw std::runtime_error("Invalid vector size");
    }
    value.resize(size / elementSize);
  }

  if (size) {
    s.binary(value.data(), size, "");
  }

  s.endArray();
  return true;
}

void serialize(Blockchain::TransactionIndex& value, ISerializer& s) {
  s(value.block, "block");
  s(value.transaction, "tx");
}

class BlockCacheSerializer {

public:
  BlockCacheSerializer(Blockchain& bs, const Crypto::Hash lastBlockHash, ILogger& logger) :
    m_bs(bs), m_lastBlockHash(lastBlockHash), m_loaded(false), logger(logger, "BlockCacheSerializer") {
  }

  void load(const std::string& filename) {
    try {
      std::ifstream stdStream(filename, std::ios::binary);
      if (!stdStream) {
        return;
      }

      StdInputStream stream(stdStream);
      BinaryInputStreamSerializer s(stream);
      CryptoNote::serialize(*this, s);
    } catch (std::exception& e) {
      logger(WARNING) << "loading failed: " << e.what();
    }
  }

  bool save(const std::string& filename) {
    try {
      std::ofstream file(filename, std::ios::binary);
      if (!file) {
        return false;
      }

      StdOutputStream stream(file);
      BinaryOutputStreamSerializer s(stream);
      CryptoNote::serialize(*this, s);
    } catch (std::exception&) {
      return false;
    }

    return true;
  }

  void serialize(ISerializer& s) {
    auto start = std::chrono::steady_clock::now();

    uint8_t version = CURRENT_BLOCKCACHE_STORAGE_ARCHIVE_VER;
    s(version, "version");

    // ignore old versions, do rebuild
    if (version < CURRENT_BLOCKCACHE_STORAGE_ARCHIVE_VER)
      return;

    std::string operation;
    if (s.type() == ISerializer::INPUT) {
      operation = "- loading ";
      Crypto::Hash blockHash;
      s(blockHash, "last_block");

      if (blockHash != m_lastBlockHash) {
        return;
      }

    } else {
      operation = "- saving ";
      s(m_lastBlockHash, "last_block");
    }

    //logger(INFO) << operation << "block index...";
    //s(m_bs.m_blockIndex, "block_index");

    //logger(INFO) << operation << "transaction map...";
    //s(m_bs.m_transactionMap, "transactions");

    //logger(INFO) << operation << "spent keys...";
    //s(m_bs.m_spent_key_images, "spent_keys");

    //logger(INFO) << operation << "outputs...";
    //s(m_bs.m_outputs, "outputs");

    //logger(INFO) << operation << "multi-signature outputs...";
    //s(m_bs.m_multisignatureOutputs, "multisig_outputs");

    auto dur = std::chrono::steady_clock::now() - start;

    logger(INFO) << "Serialization time: " << std::chrono::duration_cast<std::chrono::milliseconds>(dur).count() << "ms";

    m_loaded = true;
  }

  bool loaded() const {
    return m_loaded;
  }

private:

  LoggerRef logger;
  bool m_loaded;
  Blockchain& m_bs;
  Crypto::Hash m_lastBlockHash;
};

class BlockchainIndicesSerializer {

public:
  BlockchainIndicesSerializer(Blockchain& bs, const Crypto::Hash lastBlockHash, ILogger& logger) :
    m_bs(bs), m_lastBlockHash(lastBlockHash), m_loaded(false), logger(logger, "BlockchainIndicesSerializer") {
  }

  void serialize(ISerializer& s) {

    uint8_t version = CURRENT_BLOCKCHAININDICES_STORAGE_ARCHIVE_VER;

    KV_MEMBER(version);

    // ignore old versions, do rebuild
    if (version != CURRENT_BLOCKCHAININDICES_STORAGE_ARCHIVE_VER)
      return;

    std::string operation;

    if (s.type() == ISerializer::INPUT) {
      operation = "- loading ";

      Crypto::Hash blockHash;
      s(blockHash, "blockHash");

      if (blockHash != m_lastBlockHash) {
        return;
      }

    } else {
      operation = "- saving ";
      s(m_lastBlockHash, "blockHash");
    }

    //logger(INFO) << operation << "paymentID index...";
    //s(m_bs.m_paymentIdIndex, "paymentIdIndex");

    //logger(INFO) << operation << "timestamp index...";
    //s(m_bs.m_timestampIndex, "timestampIndex");

    //logger(INFO) << operation << "generated transactions index...";
    //s(m_bs.m_generatedTransactionsIndex, "generatedTransactionsIndex");

    m_loaded = true;
  }

  template<class Archive> void serialize(Archive& ar, unsigned int version) {

    // ignore old versions, do rebuild
    if (version < CURRENT_BLOCKCHAININDICES_STORAGE_ARCHIVE_VER)
      return;

    std::string operation;
    if (Archive::is_loading::value) {
      operation = "- loading ";
      Crypto::Hash blockHash;
      ar & blockHash;

      if (blockHash != m_lastBlockHash) {
        return;
      }

    } else {
      operation = "- saving ";
      ar & m_lastBlockHash;
    }

    //logger(INFO) << operation << "paymentID index...";
    //ar & m_bs.m_paymentIdIndex;

    //logger(INFO) << operation << "timestamp index...";
    //ar & m_bs.m_timestampIndex;

    //logger(INFO) << operation << "generated transactions index...";
    //ar & m_bs.m_generatedTransactionsIndex;

    m_loaded = true;
  }

  bool loaded() const {
    return m_loaded;
  }

private:

  LoggerRef logger;
  bool m_loaded;
  Blockchain& m_bs;
  Crypto::Hash m_lastBlockHash;
};

Blockchain::Blockchain(const Currency& currency, tx_memory_pool& tx_pool, ILogger& logger, bool blockchainIndexesEnabled, bool blockchainReadOnly, const std::string& config_folder) :
logger(logger, "Blockchain"),
m_currency(currency),
m_tx_pool(tx_pool),
m_current_block_cumul_sz_limit(0),
m_db(blockchainReadOnly ? Common::O_READ_EXISTING : Common::O_OPEN_ALWAYS, config_folder + "/blockchain"),
//m_upgradeDetectorV2(currency, m_db, BLOCK_MAJOR_VERSION_2, config_folder, logger),
//m_upgradeDetectorV3(currency, m_db, BLOCK_MAJOR_VERSION_3, config_folder, logger),
//m_upgradeDetectorV4(currency, m_db, BLOCK_MAJOR_VERSION_4, config_folder, logger),
//m_upgradeDetectorV5(currency, m_db, BLOCK_MAJOR_VERSION_5, config_folder, logger),
m_checkpoints(logger),
//m_paymentIdIndex(blockchainIndexesEnabled),
//m_timestampIndex(blockchainIndexesEnabled),
//m_generatedTransactionsIndex(blockchainIndexesEnabled),
m_orphanBlocksIndex(blockchainIndexesEnabled),
m_blockchainIndexesEnabled(blockchainIndexesEnabled),
m_height(0),
m_lastGeneratedTxNumber(0),
m_synchronized(false)
{
  //m_outputs.set_deleted_key(0);
}

bool Blockchain::addObserver(IBlockchainStorageObserver* observer) {
  return m_observerManager.add(observer);
}

bool Blockchain::removeObserver(IBlockchainStorageObserver* observer) {
  return m_observerManager.remove(observer);
}

bool Blockchain::checkTransactionInputs(const CryptoNote::Transaction& tx, BlockInfo& maxUsedBlock) {
  return checkTransactionInputs(tx, maxUsedBlock.height, maxUsedBlock.id);
}

bool Blockchain::checkTransactionInputs(const CryptoNote::Transaction& tx, BlockInfo& maxUsedBlock, BlockInfo& lastFailed) {

  BlockInfo tail;

  //not the best implementation at this time, sorry :(
  //check is ring_signature already checked ?
  if (maxUsedBlock.empty()) {
    //not checked, lets try to check
    if (!lastFailed.empty() && getCurrentBlockchainHeight() > lastFailed.height && getBlockIdByHeight(lastFailed.height) == lastFailed.id) {
      return false; //we already sure that this tx is broken for this height
    }

    if (!checkTransactionInputs(tx, maxUsedBlock.height, maxUsedBlock.id, &tail)) {
      lastFailed = tail;
      return false;
    }
  } else {
    if (maxUsedBlock.height >= getCurrentBlockchainHeight()) {
      return false;
    }

    if (getBlockIdByHeight(maxUsedBlock.height) != maxUsedBlock.id) {
      //if we already failed on this height and id, skip actual ring signature check
      if (lastFailed.id == getBlockIdByHeight(lastFailed.height)) {
        return false;
      }
    }

    //check ring signature again, it is possible (with very small chance) that this transaction become again valid
    if (!checkTransactionInputs(tx, maxUsedBlock.height, maxUsedBlock.id, &tail)) {
      lastFailed = tail;
      return false;
    }
  }

  return true;
}

bool Blockchain::haveSpentKeyImages(const CryptoNote::Transaction& tx) {
  return this->haveTransactionKeyImagesAsSpent(tx);
}

/**
* \pre m_blockchain_lock is locked
*/
bool Blockchain::checkTransactionSize(size_t blobSize) {
  if (blobSize > getCurrentCumulativeBlocksizeLimit() - m_currency.minerTxBlobReservedSize()) {
    logger(ERROR) << "transaction is too big " << blobSize << ", maximum allowed size is " <<
      (getCurrentCumulativeBlocksizeLimit() - m_currency.minerTxBlobReservedSize());
    return false;
  }

  return true;
}

bool Blockchain::haveTransaction(const Crypto::Hash &id) {
  std::lock_guard<decltype(m_blockchain_lock)> lk(m_blockchain_lock);
  //return m_transactionMap.find(id) != m_transactionMap.end();

  Platform::DB::Value v;
  if (m_db.get(TRANSACTIONS_INDEX_PREFIX + DB::to_binary_key(id.data, sizeof(id.data)), v))
    return true;

  return false;
}

bool Blockchain::have_tx_keyimg_as_spent(const Crypto::KeyImage &key_im) {
  return  checkIfSpent(key_im);
}

bool Blockchain::checkIfSpent(const Crypto::KeyImage& keyImage, uint32_t blockIndex) {
  std::lock_guard<decltype(m_blockchain_lock)> lk(m_blockchain_lock);
  /*auto it = m_spent_key_images.find(keyImage);
  if (it == m_spent_key_images.end()) {
    return false;
  }

  return it->second <= blockIndex;*/

  std::string s;
  if (!m_db.get(SPENT_KEY_IMAGES_INDEX_PREFIX + DB::to_binary_key(keyImage.data, sizeof(keyImage.data)), s))
    return false;
  uint32_t height = Common::integer_cast<uint32_t>(Common::read_varint_sqlite4(s));

  return height <= blockIndex;
}

bool Blockchain::checkIfSpent(const Crypto::KeyImage& keyImage) {
  Platform::DB::Value v;
  if (m_db.get(SPENT_KEY_IMAGES_INDEX_PREFIX + DB::to_binary_key(keyImage.data, sizeof(keyImage.data)), v))
    return true;

  return false;
}

uint32_t Blockchain::getCurrentBlockchainHeight() {
  std::lock_guard<decltype(m_blockchain_lock)> lk(m_blockchain_lock);
  //return static_cast<uint32_t>(m_blocks.size());

  return m_height.load(std::memory_order_relaxed);
}

bool Blockchain::init(const std::string& config_folder, bool load_existing) {
  std::lock_guard<decltype(m_blockchain_lock)> lk(m_blockchain_lock);
  if (!config_folder.empty() && !Tools::create_directories_if_necessary(config_folder)) {
    logger(ERROR, BRIGHT_RED) << "Failed to create data directory: " << m_config_folder;
    return false;
  }

  m_config_folder = config_folder;

  std::string version;
  if (!m_db.get("$version", version)) {
    DB::Cursor cur = m_db.begin(std::string{});
    if (!cur.end())
      throw std::runtime_error("Blockchain indexes database format unknown version, please delete " + m_db.get_path());
    version = version_current;
    m_db.put("$version", version, false);
  }
  if (version != version_current)
    return false;  // BlockChainState will upgrade DB, we must not continue or risk crashing

  m_db.get("$version", version);
  logger(INFO) << "Blockchain DB version: " << version;

  DB::Cursor cur1 = m_db.rbegin(BLOCK_INDEX_PREFIX);
  m_height.store(cur1.end() ? 0 : Common::integer_cast<uint32_t>(Common::read_varint_sqlite4(cur1.get_suffix())) + 1, std::memory_order_relaxed);

  logger(INFO, BRIGHT_WHITE) << "Loading blockchain...";

  //if (!m_blocks.open(appendPath(config_folder, m_currency.blocksFileName()), appendPath(config_folder, m_currency.blockIndexesFileName()), 1024)) {
  //  return false;
  //}

  Crypto::Hash firstBlockHash;
  if (m_height.load(std::memory_order_relaxed) == 0) {
    logger(INFO, BRIGHT_WHITE)
      << "Blockchain not loaded, generating genesis block.";
    block_verification_context bvc = boost::value_initialized<block_verification_context>();
    pushBlock(m_currency.genesisBlock(), get_block_hash(m_currency.genesisBlock()), bvc);
    if (bvc.m_verification_failed) {
      logger(ERROR, BRIGHT_RED) << "Failed to add genesis block to blockchain";
      return false;
    }
  }
  else {
    DB::Cursor cur = m_db.begin(BLOCK_INDEX_PREFIX);
    auto v = cur.get_value_array();
    std::copy(v.begin(), v.end(), firstBlockHash.data);
    if (!(firstBlockHash == m_currency.genesisBlockHash())) {
      logger(ERROR, BRIGHT_RED) << "Failed to init: genesis block mismatch. "
        "Probably you set --testnet flag with data "
        "dir with non-test blockchain or another "
        "network.";
      return false;
    }
  }

  if (load_existing && !cur1.end()/* && !m_blocks.empty()*/) {
    //logger(INFO, BRIGHT_WHITE) << "Loading blockchain cache...";

    //BlockCacheSerializer loader(*this, firstBlockHash, logger.getLogger());
    //loader.load(appendPath(config_folder, m_currency.blocksCacheFileName()));

    //if (!loader.loaded()) {
    //  logger(WARNING, BRIGHT_YELLOW) << "No actual blockchain cache found, rebuilding internal structures...";
    //  rebuildCache();
    //}

    //if (m_blockchainIndexesEnabled) {
    //  loadBlockchainIndices();
    //}
  } /*else {
    m_blocks.clear();
  }*/


  DB::Cursor cur2 = m_db.rbegin(GENERATED_TRANSACTIONS_INDEX_PREFIX);
  m_lastGeneratedTxNumber = cur2.end() ? 0 : Common::integer_cast<uint32_t>(Common::read_varint_sqlite4(cur2.get_suffix()));

  uint32_t lastValidCheckpointHeight = 0;
  if (!checkCheckpoints(lastValidCheckpointHeight)) {
    logger(WARNING, BRIGHT_YELLOW) << "Invalid checkpoint found. Rollback blockchain to height=" << lastValidCheckpointHeight;
    rollbackBlockchainTo(lastValidCheckpointHeight);
  }

  
  //if (!m_upgradeDetectorV2.init() || !m_upgradeDetectorV3.init() || !m_upgradeDetectorV4.init() || !m_upgradeDetectorV5.init()/* || !m_upgradeDetectorV6.init()*/) {
  //  logger(ERROR, BRIGHT_RED) << "Failed to initialize upgrade detector. Trying self healing procedure.";
    //return false;
  //}
  /*
  bool reinitUpgradeDetectors = false;
  if (!checkUpgradeHeight(m_upgradeDetectorV2)) {
    uint32_t upgradeHeight = m_upgradeDetectorV2.upgradeHeight();
    assert(upgradeHeight != UpgradeDetectorBase::UNDEF_HEIGHT);
    logger(WARNING, BRIGHT_YELLOW) << "Invalid block version at " << upgradeHeight + 1 << ": real=" << static_cast<int>(m_blocks[upgradeHeight + 1].bl.majorVersion) <<
      " expected=" << static_cast<int>(m_upgradeDetectorV2.targetVersion()) << ". Rollback blockchain to height=" << upgradeHeight;
    rollbackBlockchainTo(upgradeHeight);
    reinitUpgradeDetectors = true;
  } else if (!checkUpgradeHeight(m_upgradeDetectorV3)) {
    uint32_t upgradeHeight = m_upgradeDetectorV3.upgradeHeight();
    logger(WARNING, BRIGHT_YELLOW) << "Invalid block version at " << upgradeHeight + 1 << ": real=" << static_cast<int>(m_blocks[upgradeHeight + 1].bl.majorVersion) <<
      " expected=" << static_cast<int>(m_upgradeDetectorV3.targetVersion()) << ". Rollback blockchain to height=" << upgradeHeight;
    rollbackBlockchainTo(upgradeHeight);
    reinitUpgradeDetectors = true;
  } else if (!checkUpgradeHeight(m_upgradeDetectorV4)) {
    uint32_t upgradeHeight = m_upgradeDetectorV4.upgradeHeight();
    logger(WARNING, BRIGHT_YELLOW) << "Invalid block version at " << upgradeHeight + 1 << ": real=" << static_cast<int>(m_blocks[upgradeHeight + 1].bl.majorVersion) <<
      " expected=" << static_cast<int>(m_upgradeDetectorV4.targetVersion()) << ". Rollback blockchain to height=" << upgradeHeight;
    rollbackBlockchainTo(upgradeHeight);
    reinitUpgradeDetectors = true;
	} else if (!checkUpgradeHeight(m_upgradeDetectorV5)) {
    uint32_t upgradeHeight = m_upgradeDetectorV5.upgradeHeight();
    logger(WARNING, BRIGHT_YELLOW) << "Invalid block version at " << upgradeHeight + 1 << ": real=" << static_cast<int>(m_blocks[upgradeHeight + 1].bl.majorVersion) <<
      " expected=" << static_cast<int>(m_upgradeDetectorV5.targetVersion()) << ". Rollback blockchain to height=" << upgradeHeight;
    rollbackBlockchainTo(upgradeHeight);
    reinitUpgradeDetectors = true;
  }

  if (reinitUpgradeDetectors && (!m_upgradeDetectorV2.init() || !m_upgradeDetectorV3.init() || !m_upgradeDetectorV4.init() || !m_upgradeDetectorV5.init())) {
    logger(ERROR, BRIGHT_RED) << "Failed to initialize upgrade detector";
    return false;
  }
  */

  update_next_cumulative_size_limit();

  DB::Cursor cur3 = m_db.rbegin(TIMESTAMP_INDEX_PREFIX);
  uint64_t tip_timestamp = cur1.end() ? time(NULL) : 
    Common::integer_cast<uint64_t>(Common::read_varint_sqlite4(cur3.get_suffix()));
  uint64_t timestamp_diff = time(NULL) - tip_timestamp;
  if (/*!m_blocks.back().bl.timestamp*/cur1.end()) {
    timestamp_diff = time(NULL) - 1341378000;
  }

  logger(INFO, BRIGHT_GREEN)
    << "Blockchain initialized. last block: " << (m_height.load(std::memory_order_relaxed) - 1) << ", "
    << Common::timeIntervalToString(timestamp_diff)
    << " time ago, current difficulty: " << getDifficultyForNextBlock();
  return true;
}

void Blockchain::db_commit() {
  std::lock_guard<decltype(m_blockchain_lock)> lk(m_blockchain_lock);

  //logger(INFO) << "Blockchain::db_commit started...";
  try {
    m_db.commit_db_txn();
  } 
  catch (const std::exception& e) {
    logger(ERROR, BRIGHT_RED) << "Exception during DB commit: " << e.what();
  }
  catch (...) {
    logger(ERROR, BRIGHT_RED) << "Unknown error during DB commit";
  }
  //logger(INFO) << "BlockChain::db_commit finished...";
}

void Blockchain::on_synchronized() {
  m_synchronized = true;
  db_commit();
}

void Blockchain::rebuildCache() {
 /* std::chrono::steady_clock::time_point timePoint = std::chrono::steady_clock::now();
  //m_blockIndex.clear();
  //m_transactionMap.clear();
  //m_spent_key_images.clear();
  //m_outputs.clear();
  //m_multisignatureOutputs.clear();
  for (uint32_t b = 0; b < m_blocks.size(); ++b) {

    if (b % 1000 == 0) {
      logger(INFO, BRIGHT_WHITE) << "Height " << b << " of " << m_height.load(std::memory_order_relaxed);
    }
    //const BlockEntry& block = m_blocks[b];
    //m_blockIndex.push(blockHash);
    for (uint16_t t = 0; t < block.transactions.size(); ++t) {
      const TransactionEntry& transaction = block.transactions[t];
      Crypto::Hash transactionHash = getObjectHash(transaction.tx);
      TransactionIndex transactionIndex = { b, t };
      //m_transactionMap.insert(std::make_pair(transactionHash, transactionIndex));

      // process inputs
      for (auto& i : transaction.tx.inputs) {
        if (i.type() == typeid(KeyInput)) {
          //m_spent_key_images.insert(std::make_pair(::boost::get<KeyInput>(i).keyImage, b));
        } else if (i.type() == typeid(MultisignatureInput)) {
          auto out = ::boost::get<MultisignatureInput>(i);
          //m_multisignatureOutputs[out.amount][out.outputIndex].isUsed = true;
        }
      }

      // process outputs
      for (uint16_t o = 0; o < transaction.tx.outputs.size(); ++o) {
        const auto& out = transaction.tx.outputs[o];
        if (out.target.type() == typeid(KeyOutput)) {
          //m_outputs[out.amount].push_back(std::make_pair<>(transactionIndex, o));
        } else if (out.target.type() == typeid(MultisignatureOutput)) {
          //MultisignatureOutputUsage usage = { transactionIndex, o, false };
          //m_multisignatureOutputs[out.amount].push_back(usage);
        }
      }
    }
  }

  std::chrono::duration<double> duration = std::chrono::steady_clock::now() - timePoint;
  logger(INFO, BRIGHT_WHITE) << "Rebuilding internal structures took: " << duration.count();*/
}

bool Blockchain::storeCache() {
  std::lock_guard<decltype(m_blockchain_lock)> lk(m_blockchain_lock);

  logger(INFO, BRIGHT_WHITE) << "Saving blockchain at height " << m_height.load(std::memory_order_relaxed) - 1 << "...";
  db_commit();

  /*BlockCacheSerializer ser(*this, getTailId(), logger.getLogger());
  if (!ser.save(appendPath(m_config_folder, m_currency.blocksCacheFileName()))) {
    logger(ERROR, BRIGHT_RED) << "Failed to save blockchain cache";
    return false;
  }*/

  return true;
}

bool Blockchain::deinit() {
  storeCache();
  //if (m_blockchainIndexesEnabled) {
  //  storeBlockchainIndices();
  //}
  assert(m_messageQueueList.empty());
  return true;
}

bool Blockchain::resetAndSetGenesisBlock(const Block& b) {
  std::lock_guard<decltype(m_blockchain_lock)> lk(m_blockchain_lock);
  //m_blocks.clear();
  //m_blockIndex.clear();
  //m_transactionMap.clear();

  //m_spent_key_images.clear();
  m_alternative_chains.clear();
  //m_outputs.clear();
  //m_multisignatureOutputs.clear();
  //m_paymentIdIndex.clear();
  //m_timestampIndex.clear();
  //m_generatedTransactionsIndex.clear();
  m_orphanBlocksIndex.clear();

  block_verification_context bvc = boost::value_initialized<block_verification_context>();
  addNewBlock(b, bvc);
  return bvc.m_added_to_main_chain && !bvc.m_verification_failed;
}

Crypto::Hash Blockchain::getTailId(uint32_t& height) {
  /*assert(!m_blocks.empty());
  std::lock_guard<decltype(m_blockchain_lock)> lk(m_blockchain_lock);
  height = getCurrentBlockchainHeight() - 1;
  return getTailId();*/

  std::lock_guard<decltype(m_blockchain_lock)> lk(m_blockchain_lock);

  Crypto::Hash tail_id;
  DB::Cursor cur = m_db.rbegin(BLOCK_INDEX_PREFIX);
  height = cur.end() ? 0 : Common::integer_cast<uint32_t>(Common::read_varint_sqlite4(cur.get_suffix())) + 1;
  BinaryArray ba = cur.get_value_array();
  memcpy(&tail_id, ba.data(), ba.size());

  return tail_id;
}

Crypto::Hash Blockchain::getTailId() {
  std::lock_guard<decltype(m_blockchain_lock)> lk(m_blockchain_lock);
  //return m_blocks.empty() ? NULL_HASH : m_blockIndex.getTailId();

  DB::Cursor cur = m_db.rbegin(BLOCK_INDEX_PREFIX);

  if (cur.end())
    return NULL_HASH;

  Crypto::Hash tail_id;
  BinaryArray ba = cur.get_value_array();
  memcpy(&tail_id, ba.data(), ba.size());

  return tail_id;
}

std::vector<Crypto::Hash> Blockchain::buildSparseChain() {
  std::lock_guard<decltype(m_blockchain_lock)> lk(m_blockchain_lock);
  //assert(m_blockIndex.size() != 0);
  //return doBuildSparseChain(m_blockIndex.getTailId());
  return doBuildSparseChain(getTailId());
}

std::vector<Crypto::Hash> Blockchain::buildSparseChain(const Crypto::Hash& startBlockId) {
  std::lock_guard<decltype(m_blockchain_lock)> lk(m_blockchain_lock);
  assert(haveBlock(startBlockId));
  return doBuildSparseChain(startBlockId);
}

std::vector<Crypto::Hash> Blockchain::build_sparse_chain(const Crypto::Hash& startBlockId) {
  //assert(m_index.count(startBlockId) > 0);

  uint32_t startBlockHeight = 0;
  if(!getBlockHeight(startBlockId, startBlockHeight))
    throw std::runtime_error("Blockchain::build_sparse_chain, failed to get entry from DB");

  std::vector<Crypto::Hash> result;
  size_t sparseChainEnd = static_cast<size_t>(startBlockHeight + 1);
  for (size_t i = 1; i <= sparseChainEnd; i *= 2) {
    //  result.emplace_back(m_container[sparseChainEnd - i]);
    std::string s;
    Crypto::Hash h = NULL_HASH;
    if (!m_db.get(BLOCK_INDEX_PREFIX + Common::write_varint_sqlite4(sparseChainEnd - i), s))
      throw std::runtime_error("Blockchain::build_sparse_chain, failed to get entry from DB");
    std::copy(s.begin(), s.end(), h.data);
    result.emplace_back(h);
  }

  //if (result.back() != m_container[0]) {
  //  result.emplace_back(m_container[0]);
  //}
  DB::Cursor cur = m_db.begin(BLOCK_INDEX_PREFIX);
  auto v = cur.get_value_array();
  Crypto::Hash z;
  std::copy(v.begin(), v.end(), z.data);
  if (result.back() != z) {
    result.emplace_back(z);
  }

  return result;
}

std::vector<Crypto::Hash> Blockchain::doBuildSparseChain(const Crypto::Hash& startBlockId) {
  //assert(m_blockIndex.size() != 0);

  std::vector<Crypto::Hash> sparseChain;

  //if (m_blockIndex.hasBlock(startBlockId)) {
  if (haveBlock(startBlockId)) {
    //sparseChain = m_blockIndex.buildSparseChain(startBlockId);
    sparseChain = build_sparse_chain(startBlockId);
  } else {
    assert(m_alternative_chains.count(startBlockId) > 0);

    std::vector<Crypto::Hash> alternativeChain;
    Crypto::Hash blockchainAncestor;
    for (auto it = m_alternative_chains.find(startBlockId); it != m_alternative_chains.end(); it = m_alternative_chains.find(blockchainAncestor)) {
      alternativeChain.emplace_back(it->first);
      blockchainAncestor = it->second.bl.previousBlockHash;
    }

    for (size_t i = 1; i <= alternativeChain.size(); i *= 2) {
      sparseChain.emplace_back(alternativeChain[i - 1]);
    }

    assert(!sparseChain.empty());
    //assert(m_blockIndex.hasBlock(blockchainAncestor));
    assert(haveBlock(blockchainAncestor));
    //std::vector<Crypto::Hash> sparseMainChain = m_blockIndex.buildSparseChain(blockchainAncestor);
    std::vector<Crypto::Hash> sparseMainChain = build_sparse_chain(blockchainAncestor);
    sparseChain.reserve(sparseChain.size() + sparseMainChain.size());
    std::copy(sparseMainChain.begin(), sparseMainChain.end(), std::back_inserter(sparseChain));
  }

  return sparseChain;
}

Crypto::Hash Blockchain::getBlockIdByHeight(uint32_t height) {
  std::lock_guard<decltype(m_blockchain_lock)> lk(m_blockchain_lock);
  //assert(height < m_blockIndex.size());
  //return m_blockIndex.getBlockId(height);

  std::string s;
  Crypto::Hash h = NULL_HASH;
  getBlockIdByHeight(height, h);
  
  return h;
}

bool Blockchain::getBlockIdByHeight(uint32_t height, Crypto::Hash &hash) {
  std::string s;
  if (!m_db.get(BLOCK_INDEX_PREFIX + Common::write_varint_sqlite4(height), s)) {
    logger(ERROR, BRIGHT_RED) << "Blockchain::getBlockIdByHeight, failed to get entry from DB";
    return false;
  }
  Crypto::Hash h = NULL_HASH;
  std::copy(s.begin(), s.end(), h.data);
  hash = h;

  return true;
}

bool Blockchain::getBlockByHash(const Crypto::Hash& blockHash, Block& b) {
  std::lock_guard<decltype(m_blockchain_lock)> lk(m_blockchain_lock);

  //uint32_t height = 0;

  //if (m_blockIndex.getBlockHeight(blockHash, height)) {
  //  b = m_blocks[height].bl;
  //  return true;
  //}

  BinaryArray ba;
  auto key = BLOCK_PREFIX + DB::to_binary_key(blockHash.data, sizeof(blockHash.data)) + BLOCK_SUFFIX;
  if (m_db.get(key, ba)) {
    BlockEntry pb;
    if (!fromBinaryArray(pb, ba))
      return false;
    b = pb.bl;
    return true;
  }
  
  logger(INFO) << "Get alt. block requested: " << blockHash;

  auto blockByHashIterator = m_alternative_chains.find(blockHash);
  if (blockByHashIterator != m_alternative_chains.end()) {
    b = blockByHashIterator->second.bl;
    return true;
  }

  return false;
}

bool Blockchain::getBlockEntryByHeight(const uint32_t &height, BlockEntry &e) {
  std::string s;
  Crypto::Hash h;
  if (!m_db.get(BLOCK_INDEX_PREFIX + Common::write_varint_sqlite4(height), s))
    return false;
  std::copy(s.begin(), s.end(), h.data);

  auto key = BLOCK_PREFIX + DB::to_binary_key(h.data, sizeof(h.data)) + BLOCK_SUFFIX;
  BinaryArray ba;
  m_db.get(key, ba);
  if (!fromBinaryArray(e, ba))
    return false;

  return true;
}

bool Blockchain::getBlockByHeight(const uint32_t &height, Block &blk) {
  BlockEntry e;
  if (!getBlockEntryByHeight(height, e))
    return false;
  blk = e.bl;
  return true;
}

bool Blockchain::getBlockHeight(const Crypto::Hash& blockId, uint32_t& blockHeight) {
  std::lock_guard<decltype(m_blockchain_lock)> lock(m_blockchain_lock);
  //return m_blockIndex.getBlockHeight(blockId, blockHeight);

  BinaryArray ba;
  auto key = BLOCK_PREFIX + DB::to_binary_key(blockId.data, sizeof(blockId.data)) + BLOCK_SUFFIX;
  if (!m_db.get(key, ba))
    return false;
  BlockEntry e;
  if (!fromBinaryArray(e, ba))
    return false;
  blockHeight = e.height;
  assert(e.height == boost::get<BaseInput>(e.bl.baseTransaction.inputs.front()).blockIndex);

  return true;
}

difficulty_type Blockchain::getDifficultyForNextBlock() {
  std::lock_guard<decltype(m_blockchain_lock)> lk(m_blockchain_lock);
  std::vector<uint64_t> timestamps;
  std::vector<difficulty_type> cumulative_difficulties;
  uint8_t BlockMajorVersion = getBlockMajorVersionForHeight(m_height.load(std::memory_order_relaxed));
  size_t offset;
  offset = m_height.load(std::memory_order_relaxed) - std::min<size_t>(m_height.load(std::memory_order_relaxed), static_cast<size_t>(m_currency.difficultyBlocksCountByBlockVersion(BlockMajorVersion)));

  if (offset == 0) {
    ++offset;
  }
  //for (; offset < m_height; offset++) {
    //timestamps.push_back(m_blocks[offset].bl.timestamp);
    //cumulative_difficulties.push_back(m_blocks[offset].cumulative_difficulty);
  //}

  size_t start_offset = offset;
  auto middle = Common::write_varint_sqlite4(start_offset);
  for (DB::Cursor cur = m_db.begin(BLOCK_INDEX_PREFIX, middle); offset < m_height.load(std::memory_order_relaxed) || !cur.end(); offset++, cur.next()) {
    auto v = cur.get_value_array();
    Crypto::Hash id;
    std::copy(v.begin(), v.end(), id.data);
    auto key = BLOCK_PREFIX + DB::to_binary_key(id.data, sizeof(id.data)) + BLOCK_SUFFIX;
    BinaryArray ba;
    m_db.get(key, ba);
    BlockEntry e;
    fromBinaryArray(e, ba);
    timestamps.push_back(e.bl.timestamp);
    cumulative_difficulties.push_back(e.cumulative_difficulty);
  }

  return m_currency.nextDifficulty(static_cast<uint32_t>(m_height.load(std::memory_order_relaxed)), BlockMajorVersion, timestamps, cumulative_difficulties);
}

difficulty_type Blockchain::getAvgDifficulty(uint32_t height, size_t window) {
  std::lock_guard<decltype(m_blockchain_lock)> lk(m_blockchain_lock);
  height = std::min<uint32_t>(height, m_height.load(std::memory_order_relaxed) - 1);
  if (height <= 1)
    return 1;

  BlockEntry e1;
  getBlockEntryByHeight(height, e1);

  if (window == height) {
    return e1.cumulative_difficulty / height;
  }

  size_t offset;
  offset = height - std::min<uint32_t>(height, std::min<uint32_t>(m_height.load(std::memory_order_relaxed) - 1, static_cast<uint32_t>(window)));
  if (offset == 0) {
    ++offset;
  }

  BlockEntry e2;
  getBlockEntryByHeight((uint32_t)offset, e2);

  difficulty_type cumulDiffForPeriod = e1.cumulative_difficulty - e2.cumulative_difficulty;
  return cumulDiffForPeriod / std::min<uint32_t>(m_height.load(std::memory_order_relaxed) - 1, static_cast<uint32_t>(window));
}

difficulty_type Blockchain::getAvgDifficulty(uint32_t height) {
  std::lock_guard<decltype(m_blockchain_lock)> lk(m_blockchain_lock);
  height = std::min<uint32_t>(height, m_height.load(std::memory_order_relaxed) - 1);
  if (height <= 1)
    return 1;
  
  BlockEntry e;
  getBlockEntryByHeight(height, e);

  return e.cumulative_difficulty / height;
}

uint64_t Blockchain::getBlockTimestamp(uint32_t height) {
  assert(height < m_height.load(std::memory_order_relaxed) - 1);
  //return m_blocks[height].bl.timestamp;

  BlockEntry e;
  getBlockEntryByHeight(height, e);

  return e.bl.timestamp;
}

uint64_t Blockchain::getMinimalFee(uint32_t height) {
  std::lock_guard<decltype(m_blockchain_lock)> lk(m_blockchain_lock);
  if (height == 0 || m_height.load(std::memory_order_relaxed) <= 1) {
    return 0;
  }
  if (height > m_height.load(std::memory_order_relaxed) - 1) {
    height = m_height.load(std::memory_order_relaxed) - 1;
  }
  if (height < 3) {
    height = 3;
  }
  uint32_t window = std::min(height, std::min<uint32_t>(m_height.load(std::memory_order_relaxed) - 1, static_cast<uint32_t>(m_currency.expectedNumberOfBlocksPerDay())));
  if (window == 0) {
    ++window;
  }
  size_t offset = height - window;
  if (offset == 0) {
    ++offset;
  }

  BlockEntry e1;
  getBlockEntryByHeight(height, e1);
  BlockEntry e2;
  getBlockEntryByHeight((uint32_t)offset, e2);

  // calculate average difficulty for ~last month
  uint64_t avgDifficultyCurrent = getAvgDifficulty(height, window * 7 * 4);

  // historical reference trailing average difficulty
  //uint64_t avgDifficultyHistorical = m_blocks[height].cumulative_difficulty / height;
  uint64_t avgDifficultyHistorical = e1.cumulative_difficulty / height;

  // calculate average reward for ~last day (base, excluding fees)
  //uint64_t avgRewardCurrent = (m_blocks[height].already_generated_coins - m_blocks[offset].already_generated_coins) / window;
  uint64_t avgRewardCurrent = (e1.already_generated_coins - e2.already_generated_coins) / window;
    
  // historical reference trailing average reward
  //uint64_t avgRewardHistorical = m_blocks[height].already_generated_coins / height;
  uint64_t avgRewardHistorical = e1.already_generated_coins / height;

  return m_currency.getMinimalFee(avgDifficultyCurrent, avgRewardCurrent, avgDifficultyHistorical, avgRewardHistorical, height);
}

uint64_t Blockchain::getCoinsInCirculation() {
  std::lock_guard<decltype(m_blockchain_lock)> lk(m_blockchain_lock);
  if (m_height.load(std::memory_order_relaxed) == 0) {
    return 0;
  } else {
    BlockEntry e;
    getBlockEntryByHeight(m_height.load(std::memory_order_relaxed) - 1, e);
    return e.already_generated_coins;
  }
}

uint64_t Blockchain::getCoinsInCirculation(uint32_t height) {
  std::lock_guard<decltype(m_blockchain_lock)> lk(m_blockchain_lock);
  if (m_height.load(std::memory_order_relaxed) == 0) {
    return 0;
  }
  else {
    //return m_blocks[height].already_generated_coins;
    BlockEntry e;
    getBlockEntryByHeight(height, e);

    return e.already_generated_coins;
  }
}

uint8_t Blockchain::getBlockMajorVersionForHeight(uint32_t height) const {
  if (height > m_currency.upgradeHeight(BLOCK_MAJOR_VERSION_5)) {
    return BLOCK_MAJOR_VERSION_5;
  } else if (height > m_currency.upgradeHeight(BLOCK_MAJOR_VERSION_4)) {
    return BLOCK_MAJOR_VERSION_4;
  } else if (height > m_currency.upgradeHeight(BLOCK_MAJOR_VERSION_3)) {
    return BLOCK_MAJOR_VERSION_3;
  } else if (height > m_currency.upgradeHeight(BLOCK_MAJOR_VERSION_2)) {
    return BLOCK_MAJOR_VERSION_2;
  } else {
    return BLOCK_MAJOR_VERSION_1;
  }
}

bool Blockchain::rollback_blockchain_switching(std::list<Block> &original_chain, size_t rollback_height) {
  std::lock_guard<decltype(m_blockchain_lock)> lk(m_blockchain_lock);
  // remove failed subchain
  for (size_t i = m_height.load(std::memory_order_relaxed) - 1; i >= rollback_height; i--) {
    popBlock();
  }

  // return back original chain
  for (auto &bl : original_chain) {
    block_verification_context bvc =
      boost::value_initialized<block_verification_context>();
    bool r = pushBlock(bl, get_block_hash(bl), bvc);
    if (!(r && bvc.m_added_to_main_chain)) {
      logger(ERROR, BRIGHT_RED) << "PANIC!!! failed to add (again) block while "
        "chain switching during the rollback!";
      return false;
    }
  }

  logger(INFO, BRIGHT_WHITE) << "Rollback success.";
  return true;
}

bool Blockchain::switch_to_alternative_blockchain(std::list<blocks_ext_by_hash::iterator>& alt_chain, bool discard_disconnected_chain) {
  std::lock_guard<decltype(m_blockchain_lock)> lk(m_blockchain_lock);

  if (!(alt_chain.size())) {
    logger(ERROR, BRIGHT_RED) << "switch_to_alternative_blockchain: empty chain passed";
    return false;
  }

  size_t split_height = alt_chain.front()->second.height;

  if (!(m_height.load(std::memory_order_relaxed) > split_height)) {
    logger(ERROR, BRIGHT_RED) << "switch_to_alternative_blockchain: blockchain size is lower than split height";
    return false;
  }

  // Poisson check, courtesy of ryo-project
  // https://github.com/ryo-currency/ryo-writeups/blob/master/poisson-writeup.md
  // For longer reorgs, check if the timestamps are probable - if they aren't the diff algo has failed
  // This check is meant to detect an offline bypass of timestamp < time() + ftl check
  // It doesn't need to be very strict as it synergises with the median check
  if (alt_chain.size() >= CryptoNote::parameters::POISSON_CHECK_TRIGGER)
  {
    uint64_t alt_chain_size = alt_chain.size();
    uint64_t high_timestamp = alt_chain.back()->second.bl.timestamp;
    Crypto::Hash low_block = alt_chain.front()->second.bl.previousBlockHash;

    //Make sure that the high_timestamp is really highest
    for (const blocks_ext_by_hash::iterator &it : alt_chain)
    {
      if (high_timestamp < it->second.bl.timestamp)
        high_timestamp = it->second.bl.timestamp;
    }

    uint64_t block_ftl = CryptoNote::parameters::CRYPTONOTE_BLOCK_FUTURE_TIME_LIMIT_V1;
    // This would fail later anyway
    if (high_timestamp > get_adjusted_time() + block_ftl)
    {
      logger(ERROR, BRIGHT_RED) << "Attempting to move to an alternate chain, but it failed FTL check! Timestamp: " << high_timestamp << ", limit: " << get_adjusted_time() + block_ftl;
      return false;
    }

    logger(WARNING) << "Poisson check triggered by reorg size of " << alt_chain_size;

    uint64_t failed_checks = 0, i = 1;
    for (; i <= CryptoNote::parameters::POISSON_CHECK_DEPTH; i++)
    {
      // This means we reached the genesis block
      if (low_block == NULL_HASH)
        break;

      Block blk;
      getBlockByHash(low_block, blk);

      uint64_t low_timestamp = blk.timestamp;
      low_block = blk.previousBlockHash;

      if (low_timestamp >= high_timestamp)
      {
        logger(INFO) << "Skipping check at depth " << i << " due to tampered timestamp on main chain.";
        failed_checks++;
        continue;
      }

      double lam = double(high_timestamp - low_timestamp) / double(CryptoNote::parameters::DIFFICULTY_TARGET);
      if (calc_poisson_ln(lam, alt_chain_size + i) < CryptoNote::parameters::POISSON_LOG_P_REJECT)
      {
        logger(INFO) << "Poisson check at depth " << i << " failed! delta_t: " << (high_timestamp - low_timestamp) << " size: " << alt_chain_size + i;
        failed_checks++;
      }
    }

    i--; //Convert to number of checks
    logger(INFO) << "Poisson check result " << failed_checks << " fails out of " << i;

    if (failed_checks > i / 2)
    {
      logger(ERROR, BRIGHT_RED) << "Attempting to move to an alternate chain, but it failed Poisson check! " << failed_checks << " fails out of " << i << " alt_chain_size: " << alt_chain_size;
      return false;
    }
  }

  // Compare transactions in proposed alt chain vs current main chain and reject if some transaction is missing in the alt chain
  /*std::vector<Crypto::Hash> mainChainTxHashes, altChainTxHashes;
  for (size_t i = m_blocks.size() - 1; i >= split_height; i--) {
    Block b = m_blocks[i].bl;
    std::copy(b.transactionHashes.begin(), b.transactionHashes.end(), std::inserter(mainChainTxHashes, mainChainTxHashes.end()));
  }
  for (auto alt_ch_iter = alt_chain.begin(); alt_ch_iter != alt_chain.end(); alt_ch_iter++) {
    auto ch_ent = *alt_ch_iter;
    Block b = ch_ent->second.bl;
    std::copy(b.transactionHashes.begin(), b.transactionHashes.end(), std::inserter(altChainTxHashes, altChainTxHashes.end()));
  }
  for (auto main_ch_it = mainChainTxHashes.begin(); main_ch_it != mainChainTxHashes.end(); main_ch_it++) {
    auto tx_hash = *main_ch_it;
    if (std::find(altChainTxHashes.begin(), altChainTxHashes.end(), tx_hash) == altChainTxHashes.end()) {
      logger(ERROR, BRIGHT_RED) << "Attempting to switch to an alternate chain, but it lacks transaction " << Common::podToHex(tx_hash) << " from main chain, rejected";
      mainChainTxHashes.clear();
      mainChainTxHashes.shrink_to_fit();
      altChainTxHashes.clear();
      altChainTxHashes.shrink_to_fit();
      return false;
    }
  }  TODO DO THIS LATER */ 

  //disconnecting old chain
  std::list<Block> disconnected_chain;

  size_t i = m_height.load(std::memory_order_relaxed) - 1;
  auto middle = Common::write_varint_sqlite4(m_height.load(std::memory_order_relaxed) - 1);
  for (DB::Cursor cur = m_db.rbegin(BLOCK_INDEX_PREFIX, middle); i >= split_height; i--, cur.next()) {
    auto v = cur.get_value_array();
    Crypto::Hash id;
    std::copy(v.begin(), v.end(), id.data);
    auto key = BLOCK_PREFIX + DB::to_binary_key(id.data, sizeof(id.data)) + BLOCK_SUFFIX;
    BinaryArray ba;
    m_db.get(key, ba);
    BlockEntry e;
    fromBinaryArray(e, ba);
    Block b = e.bl;
    popBlock();
    disconnected_chain.push_front(b);
  }

  /*for (size_t i = m_blocks.size() - 1; i >= split_height; i--) {
    Block b = m_blocks[i].bl;
    popBlock();
    //if (!(r)) { logger(ERROR, BRIGHT_RED) << "failed to remove block on chain switching"; return false; }
    disconnected_chain.push_front(b);
  }*/

  //connecting new alternative chain
  for (auto alt_ch_iter = alt_chain.begin(); alt_ch_iter != alt_chain.end(); alt_ch_iter++) {
    auto ch_ent = *alt_ch_iter;
    block_verification_context bvc = boost::value_initialized<block_verification_context>();
    bool r = pushBlock(ch_ent->second.bl, get_block_hash(ch_ent->second.bl), bvc);
    if (!r || !bvc.m_added_to_main_chain) {
      logger(INFO, BRIGHT_WHITE) << "Failed to switch to alternative blockchain";
      rollback_blockchain_switching(disconnected_chain, split_height);
      //add_block_as_invalid(ch_ent->second, get_block_hash(ch_ent->second.bl));
      logger(INFO, BRIGHT_WHITE) << "The block was inserted as invalid while connecting new alternative chain,  block_id: " << get_block_hash(ch_ent->second.bl);
      m_orphanBlocksIndex.remove(ch_ent->second.bl);
      m_alternative_chains.erase(ch_ent);

      for (auto alt_ch_to_orph_iter = ++alt_ch_iter; alt_ch_to_orph_iter != alt_chain.end(); alt_ch_to_orph_iter++) {
        //block_verification_context bvc = boost::value_initialized<block_verification_context>();
        //add_block_as_invalid((*alt_ch_iter)->second, (*alt_ch_iter)->first);
        m_orphanBlocksIndex.remove((*alt_ch_to_orph_iter)->second.bl);
        m_alternative_chains.erase(*alt_ch_to_orph_iter);
      }

      return false;
    }
  }

  if (!discard_disconnected_chain) {
    //pushing old chain as alternative chain
    for (auto& old_ch_ent : disconnected_chain) {
      block_verification_context bvc = boost::value_initialized<block_verification_context>();
      bool r = handle_alternative_block(old_ch_ent, get_block_hash(old_ch_ent), bvc, false);
      if (!r) {
        logger(WARNING, BRIGHT_YELLOW) << ("Failed to push ex-main chain blocks to alternative chain ");
        break;
      }
    }
  }

  std::vector<Crypto::Hash> blocksFromCommonRoot;
  blocksFromCommonRoot.reserve(alt_chain.size() + 1);
  blocksFromCommonRoot.push_back(alt_chain.front()->second.bl.previousBlockHash);

  //removing all_chain entries from alternative chain
  for (auto ch_ent : alt_chain) {
    blocksFromCommonRoot.push_back(get_block_hash(ch_ent->second.bl));
    m_orphanBlocksIndex.remove(ch_ent->second.bl);
    m_alternative_chains.erase(ch_ent);
  }

  sendMessage(BlockchainMessage(ChainSwitchMessage(std::move(blocksFromCommonRoot))));

  logger(INFO, BRIGHT_GREEN) << "REORGANIZE SUCCESS! on height: " << split_height << ", new blockchain size: " << m_height.load(std::memory_order_relaxed);
  return true;
}

//------------------------------------------------------------------
// This function calculates the difficulty target for the block being added to an alternate chain.
difficulty_type Blockchain::get_next_difficulty_for_alternative_chain(const std::list<blocks_ext_by_hash::iterator>& alt_chain, BlockEntry& bei) {
  std::vector<uint64_t> timestamps;
  std::vector<difficulty_type> cumulative_difficulties;
  uint8_t BlockMajorVersion = getBlockMajorVersionForHeight(m_height.load(std::memory_order_relaxed));

  // if the alt chain isn't long enough to calculate the difficulty target
  // based on its blocks alone, need to get more blocks from the main chain
  if (alt_chain.size() < m_currency.difficultyBlocksCountByBlockVersion(BlockMajorVersion)) {
    std::lock_guard<decltype(m_blockchain_lock)> lk(m_blockchain_lock);
    size_t main_chain_stop_offset = alt_chain.size() ? alt_chain.front()->second.height : bei.height;
    size_t main_chain_count = m_currency.difficultyBlocksCountByBlockVersion(BlockMajorVersion) - std::min(m_currency.difficultyBlocksCountByBlockVersion(BlockMajorVersion), alt_chain.size());
    main_chain_count = std::min(main_chain_count, main_chain_stop_offset);
    size_t main_chain_start_offset = main_chain_stop_offset - main_chain_count;

    if (!main_chain_start_offset)
      ++main_chain_start_offset; //skip genesis block
    
    // get difficulties and timestamps from relevant main chain blocks
    /*for (; main_chain_start_offset < main_chain_stop_offset; ++main_chain_start_offset) {
      timestamps.push_back(m_blocks[main_chain_start_offset].bl.timestamp);
      cumulative_difficulties.push_back(m_blocks[main_chain_start_offset].cumulative_difficulty);
    }*/

    auto middle = Common::write_varint_sqlite4(main_chain_start_offset);
    for (DB::Cursor cur = m_db.begin(BLOCK_INDEX_PREFIX, middle); main_chain_start_offset < main_chain_stop_offset; ++main_chain_start_offset, cur.next()) {
      auto v = cur.get_value_array();
      Crypto::Hash id;
      std::copy(v.begin(), v.end(), id.data);
      auto key = BLOCK_PREFIX + DB::to_binary_key(id.data, sizeof(id.data)) + BLOCK_SUFFIX;
      BinaryArray ba;
      m_db.get(key, ba);
      BlockEntry e;
      fromBinaryArray(e, ba);
      timestamps.push_back(e.bl.timestamp);
      cumulative_difficulties.push_back(e.cumulative_difficulty);
    }

    // make sure we haven't accidentally grabbed too many blocks... ???
    if (!((alt_chain.size() + timestamps.size()) <= m_currency.difficultyBlocksCountByBlockVersion(BlockMajorVersion))) {
      logger(ERROR, BRIGHT_RED) << "Internal error, alt_chain.size()[" << alt_chain.size() << "] + timestamps.size()[" << timestamps.size() <<
        "] NOT <= m_currency.difficultyBlocksCount()[" << m_currency.difficultyBlocksCountByBlockVersion(BlockMajorVersion) << ']'; return false;
    }
    for (auto it : alt_chain) {
      timestamps.push_back(it->second.bl.timestamp);
      cumulative_difficulties.push_back(it->second.cumulative_difficulty);
    }
  // if the alt chain is long enough for the difficulty calc, grab difficulties
  // and timestamps from it alone
  } else {
    timestamps.resize(std::min(alt_chain.size(), m_currency.difficultyBlocksCountByBlockVersion(BlockMajorVersion)));
    cumulative_difficulties.resize(std::min(alt_chain.size(), m_currency.difficultyBlocksCountByBlockVersion(BlockMajorVersion)));
    size_t count = 0;
    size_t max_i = timestamps.size() - 1;
    // get difficulties and timestamps from most recent blocks in alt chain
    BOOST_REVERSE_FOREACH(auto it, alt_chain) {
      timestamps[max_i - count] = it->second.bl.timestamp;
      cumulative_difficulties[max_i - count] = it->second.cumulative_difficulty;
      count++;
      if (count >= m_currency.difficultyBlocksCountByBlockVersion(BlockMajorVersion)) {
        break;
      }
    }
  }

  return m_currency.nextDifficulty(m_height.load(std::memory_order_relaxed), BlockMajorVersion, timestamps, cumulative_difficulties);
}

bool Blockchain::prevalidate_miner_transaction(const Block& b, uint32_t height) {

  if (!(b.baseTransaction.inputs.size() == 1)) {
    logger(ERROR, BRIGHT_RED)
      << "coinbase transaction in the block has no inputs";
    return false;
  }

  if (!(b.baseTransaction.signatures.empty())) {
    logger(ERROR, BRIGHT_RED)
      << "coinbase transaction in the block shouldn't have signatures";
    return false;
  }

  if (!(b.baseTransaction.inputs[0].type() == typeid(BaseInput))) {
    logger(ERROR, BRIGHT_RED)
      << "coinbase transaction in the block has the wrong type";
    return false;
  }

  if (boost::get<BaseInput>(b.baseTransaction.inputs[0]).blockIndex != height) {
    logger(INFO, BRIGHT_RED) << "The miner transaction in block has invalid height: " <<
      boost::get<BaseInput>(b.baseTransaction.inputs[0]).blockIndex << ", expected: " << height;
    return false;
  }

  if (!(b.baseTransaction.unlockTime == height + (b.majorVersion < BLOCK_MAJOR_VERSION_5 ? m_currency.minedMoneyUnlockWindow() : m_currency.minedMoneyUnlockWindow_v1()))) {
    logger(ERROR, BRIGHT_RED)
      << "coinbase transaction transaction have wrong unlock time="
      << b.baseTransaction.unlockTime << ", expected "
      << height + (b.majorVersion < BLOCK_MAJOR_VERSION_5 ? m_currency.minedMoneyUnlockWindow() : m_currency.minedMoneyUnlockWindow_v1());
    return false;
  }

  if (!check_outs_overflow(b.baseTransaction)) {
    logger(INFO, BRIGHT_RED) << "miner transaction have money overflow in block " << get_block_hash(b);
    return false;
  }

  return true;
}

bool Blockchain::validate_miner_transaction(const Block& b, uint32_t height, size_t cumulativeBlockSize,
  uint64_t alreadyGeneratedCoins, uint64_t fee, uint64_t& reward, int64_t& emissionChange) {

  uint64_t minerReward = 0;
  for (auto& o : b.baseTransaction.outputs) {
    minerReward += o.amount;
  }

  std::vector<size_t> lastBlocksSizes;
  get_last_n_blocks_sizes(lastBlocksSizes, m_currency.rewardBlocksWindow());
  size_t blocksSizeMedian = Common::medianValue(lastBlocksSizes);

  auto blockMajorVersion = getBlockMajorVersionForHeight(height);
  if (!m_currency.getBlockReward(blockMajorVersion, blocksSizeMedian, cumulativeBlockSize, alreadyGeneratedCoins, fee, reward, emissionChange)) {
    logger(INFO, BRIGHT_WHITE) << "block size " << cumulativeBlockSize << " is bigger than allowed for this blockchain";
    return false;
  }

  if (minerReward > reward) {
    logger(ERROR, BRIGHT_RED) << "Coinbase transaction spend too much money: " << m_currency.formatAmount(minerReward) <<
      ", block reward is " << m_currency.formatAmount(reward);
    return false;
  } else if (minerReward < reward) {
    logger(ERROR, BRIGHT_RED) << "Coinbase transaction doesn't use full amount of block reward: spent " <<
      m_currency.formatAmount(minerReward) << ", block reward is " << m_currency.formatAmount(reward);
    return false;
  }

  return true;
}

bool Blockchain::getBackwardBlocksSize(size_t from_height, std::vector<size_t>& sz, size_t count) {
  std::lock_guard<decltype(m_blockchain_lock)> lk(m_blockchain_lock);
  if (!(from_height < m_height.load(std::memory_order_relaxed))) {
    logger(ERROR, BRIGHT_RED)
      << "Internal error: get_backward_blocks_sizes called with from_height="
      << from_height << ", blockchain height = " << m_height.load(std::memory_order_relaxed);
    return false;
  }
  size_t start_offset = (from_height + 1) - std::min((from_height + 1), count);
  //for (size_t i = start_offset; i != from_height + 1; i++) {
  //  sz.push_back(m_blocks[i].block_cumulative_size);
  //}
  size_t i = start_offset;
  auto middle = Common::write_varint_sqlite4(start_offset);
  for (DB::Cursor cur = m_db.begin(BLOCK_INDEX_PREFIX, middle); i != from_height + 1; i++, cur.next()) {
    auto v = cur.get_value_array();
    Crypto::Hash id;
    std::copy(v.begin(), v.end(), id.data);
    auto key = BLOCK_PREFIX + DB::to_binary_key(id.data, sizeof(id.data)) + BLOCK_SUFFIX;
    BinaryArray ba;
    m_db.get(key, ba);
    BlockEntry e;
    fromBinaryArray(e, ba);
    sz.push_back(e.block_cumulative_size);
  }
  return true;
}

bool Blockchain::get_last_n_blocks_sizes(std::vector<size_t>& sz, size_t count) {
  std::lock_guard<decltype(m_blockchain_lock)> lk(m_blockchain_lock);
  if (m_height.load(std::memory_order_relaxed) == 0) {
    return true;
  }

  return getBackwardBlocksSize(m_height.load(std::memory_order_relaxed) - 1, sz, count);
}

uint64_t Blockchain::getCurrentCumulativeBlocksizeLimit() {
  return m_current_block_cumul_sz_limit;
}

bool Blockchain::complete_timestamps_vector(uint8_t blockMajorVersion, uint64_t start_top_height, std::vector<uint64_t>& timestamps) {
  if (timestamps.size() >= m_currency.timestampCheckWindow(blockMajorVersion))
    return true;

  std::lock_guard<decltype(m_blockchain_lock)> lk(m_blockchain_lock);
  size_t need_elements = m_currency.timestampCheckWindow(blockMajorVersion) - timestamps.size();
  if (!(start_top_height < m_height.load(std::memory_order_relaxed))) { logger(ERROR, BRIGHT_RED) << "internal error: passed start_height = "
    << start_top_height << " not less then m_height=" << m_height.load(std::memory_order_relaxed); return false; }
  size_t stop_offset = start_top_height > need_elements ? start_top_height - need_elements : 0;
  do {
    //timestamps.push_back(m_blocks[start_top_height].bl.timestamp);
    
    std::string s;
    Crypto::Hash h;
    if (!m_db.get(BLOCK_INDEX_PREFIX + Common::write_varint_sqlite4(start_top_height), s))
      return false;
    std::copy(s.begin(), s.end(), h.data);

    auto key = BLOCK_PREFIX + DB::to_binary_key(h.data, sizeof(h.data)) + BLOCK_SUFFIX;
    BlockEntry e;
    BinaryArray ba;
    m_db.get(key, ba);
    if (!fromBinaryArray(e, ba))
      return false;
    timestamps.push_back(e.bl.timestamp);

    if (start_top_height == 0)
      break;
    --start_top_height;
  } while (start_top_height != stop_offset);
  return true;
}

bool Blockchain::handle_alternative_block(const Block& b, const Crypto::Hash& id, block_verification_context& bvc, bool sendNewAlternativeBlockMessage) {
  std::lock_guard<decltype(m_blockchain_lock)> lk(m_blockchain_lock);

  auto block_height = get_block_height(b);
  if (block_height == 0) {
    logger(ERROR, BRIGHT_RED) <<
      "Block with id: " << Common::podToHex(id) << " (as alternative) have wrong miner transaction";
    bvc.m_verification_failed = true;
    return false;
  }

  // get fresh checkpoints from DNS - the best we have right now
#ifndef __ANDROID__
  m_checkpoints.load_checkpoints_from_dns();
#endif

  if (!m_checkpoints.is_alternative_block_allowed(getCurrentBlockchainHeight(), block_height)) {
    logger(TRACE) << "Block with id: " << id << std::endl <<
      " can't be accepted for alternative chain, block height: " << block_height << std::endl <<
      " blockchain height: " << getCurrentBlockchainHeight();
    bvc.m_verification_failed = true;
    return false;
  }

  if (!checkBlockVersion(b, id)) {
    bvc.m_verification_failed = true;
    return false;
  }

  //if (!checkParentBlockSize(b, id)) {
  //  bvc.m_verification_failed = true;
  //  return false;
  //}

  size_t cumulativeSize;
  if (!getBlockCumulativeSize(b, cumulativeSize)) {
    logger(TRACE) << "Block with id: " << id << " has at least one unknown transaction. Cumulative size is calculated imprecisely";
  }

  if (!checkCumulativeBlockSize(id, cumulativeSize, block_height)) {
    bvc.m_verification_failed = true;
    return false;
  }

  //block is not related with head of main chain
  //first of all - look in alternative chains container
  uint32_t mainPrevHeight = 0;
  const bool mainPrev = getBlockHeight(b.previousBlockHash, mainPrevHeight);
  const auto it_prev = m_alternative_chains.find(b.previousBlockHash);

  if (it_prev != m_alternative_chains.end() || mainPrev) {
    //we have new block in alternative chain

    //build alternative subchain, front -> mainchain, back -> alternative head
    blocks_ext_by_hash::iterator alt_it = it_prev; //m_alternative_chains.find()
    std::list<blocks_ext_by_hash::iterator> alt_chain;
    std::vector<uint64_t> timestamps;
    while (alt_it != m_alternative_chains.end()) {
      alt_chain.push_front(alt_it);
      timestamps.push_back(alt_it->second.bl.timestamp);
      alt_it = m_alternative_chains.find(alt_it->second.bl.previousBlockHash);
    }

    // if block to be added connects to known blocks that aren't part of the
    // main chain -- that is, if we're adding on to an alternate chain
    if (alt_chain.size()) {
      // make sure alt chain doesn't somehow start past the end of the main chain
      if (!(m_height.load(std::memory_order_relaxed) > alt_chain.front()->second.height)) { logger(ERROR, BRIGHT_RED) << "main blockchain wrong height"; return false; }
      // make sure block connects correctly to the main chain
      Crypto::Hash h = NULL_HASH;
      //get_block_hash(m_blocks[alt_chain.front()->second.height - 1].bl, h);
      std::string s;
      if (!m_db.get(BLOCK_INDEX_PREFIX + Common::write_varint_sqlite4(alt_chain.front()->second.height - 1), s))
        return false;
      std::copy(s.begin(), s.end(), h.data);

      if (!(h == alt_chain.front()->second.bl.previousBlockHash)) { logger(ERROR, BRIGHT_RED) << "alternative chain have wrong connection to main chain"; return false; }
      complete_timestamps_vector(b.majorVersion, alt_chain.front()->second.height - 1, timestamps);
    } else {
      // if block parent is not part of main chain or an alternate chain, we ignore it
      if (!(mainPrev)) { logger(ERROR, BRIGHT_RED) << "internal error: broken imperative condition it_main_prev != m_blocks_index.end()"; return false; }
	  complete_timestamps_vector(b.majorVersion, mainPrevHeight, timestamps);
    }

    // check timestamp correct - verify that the block's timestamp is within the acceptable range
    // (not earlier than the median of the last X blocks)
    if (!check_block_timestamp(timestamps, b)) {
      logger(INFO, BRIGHT_RED) <<
        "Block with id: " << id
        << ENDL << " for alternative chain, have invalid timestamp: " << b.timestamp;
      //add_block_as_invalid(b, id);//do not add blocks to invalid storage before proof of work check was passed
      bvc.m_verification_failed = true;
      return false;
    }

    BlockEntry bei = boost::value_initialized<BlockEntry>();
    bei.bl = b;
    bei.height = static_cast<uint32_t>(alt_chain.size() ? it_prev->second.height + 1 : mainPrevHeight + 1);

    bool is_a_checkpoint;
    if (!m_checkpoints.check_block(bei.height, id, is_a_checkpoint)) {
      logger(ERROR, BRIGHT_RED) <<
        "CHECKPOINT VALIDATION FAILED";
      bvc.m_verification_failed = true;
      return false;
    }

    // Disable merged mining
    if (bei.bl.majorVersion >= CryptoNote::BLOCK_MAJOR_VERSION_5) {
      TransactionExtraMergeMiningTag mmTag;
      if (getMergeMiningTagFromExtra(bei.bl.baseTransaction.extra, mmTag)) {
        logger(ERROR, BRIGHT_RED) << "Merge mining tag was found in extra of miner transaction";
        return false;
      }
    }

    // Check the block's hash against the difficulty target for its alt chain
    difficulty_type current_diff = get_next_difficulty_for_alternative_chain(alt_chain, bei);
    if (!(current_diff)) { logger(ERROR, BRIGHT_RED) << "!!!!!!! DIFFICULTY OVERHEAD !!!!!!!"; return false; }
    Crypto::Hash proof_of_work = NULL_HASH;
    // Always check PoW for alternative blocks
    if (!m_currency.checkProofOfWork(m_cn_context, bei.bl, current_diff, proof_of_work)) {
      logger(INFO, BRIGHT_RED) <<
        "Block with id: " << id
        << ENDL << " for alternative chain, have not enough proof of work: " << proof_of_work
        << ENDL << " expected difficulty: " << current_diff;
      bvc.m_verification_failed = true;
      return false;
    }

    if (!prevalidate_miner_transaction(b, bei.height)) {
      logger(INFO, BRIGHT_RED) <<
        "Block with id: " << Common::podToHex(id) << " (as alternative) have wrong miner transaction.";
      bvc.m_verification_failed = true;
      return false;
    }

    std::string s;
    Crypto::Hash h;
    if (!m_db.get(BLOCK_INDEX_PREFIX + Common::write_varint_sqlite4(mainPrevHeight), s))
      return false;
    std::copy(s.begin(), s.end(), h.data);
    auto key = BLOCK_PREFIX + DB::to_binary_key(h.data, sizeof(h.data)) + BLOCK_SUFFIX;
    BlockEntry e;
    BinaryArray ba;
    m_db.get(key, ba);
    if (!fromBinaryArray(e, ba))
      return false;

    bei.cumulative_difficulty = alt_chain.size() ? it_prev->second.cumulative_difficulty : e.cumulative_difficulty;
    bei.cumulative_difficulty += current_diff;

#ifdef _DEBUG
    auto i_dres = m_alternative_chains.find(id);
    if (!(i_dres == m_alternative_chains.end())) { logger(ERROR, BRIGHT_RED) << "insertion of new alternative block returned as it already exist"; return false; }
#endif

    auto i_res = m_alternative_chains.insert(blocks_ext_by_hash::value_type(id, bei));
    if (!(i_res.second)) { logger(ERROR, BRIGHT_RED) << "insertion of new alternative block returned as it already exist"; return false; }

    m_orphanBlocksIndex.add(bei.bl);

    alt_chain.push_back(i_res.first);

    DB::Cursor cur = m_db.rbegin(BLOCK_INDEX_PREFIX);
    BinaryArray tip_ba = cur.get_value_array();
    BlockEntry tip_be;
    fromBinaryArray(tip_be, tip_ba);


    if (is_a_checkpoint) {
      //do reorganize!
      logger(INFO, BRIGHT_GREEN) <<
        "###### REORGANIZE on height: " << alt_chain.front()->second.height << " of " << m_height.load(std::memory_order_relaxed) - 1 <<
        ", checkpoint is found in alternative chain on height " << bei.height;
      bool r = switch_to_alternative_blockchain(alt_chain, true);
      if (r) {
        bvc.m_added_to_main_chain = true;
        bvc.m_switched_to_alt_chain = true;
      } else {
        bvc.m_verification_failed = true;
      }
      return r;
    } else if (tip_be.cumulative_difficulty < bei.cumulative_difficulty) //check if difficulty bigger then in main chain
    {
      //do reorganize!
      logger(INFO, BRIGHT_GREEN) <<
        "###### REORGANIZE on height: " << alt_chain.front()->second.height << " of " << m_height.load(std::memory_order_relaxed) - 1 << " with cum_difficulty " << tip_be.cumulative_difficulty
        << ENDL << " alternative blockchain size: " << alt_chain.size() << " with cum_difficulty " << bei.cumulative_difficulty;
      bool r = switch_to_alternative_blockchain(alt_chain, false);
      if (r) {
        bvc.m_added_to_main_chain = true;
        bvc.m_switched_to_alt_chain = true;
      } else {
        bvc.m_verification_failed = true;
      }
      return r;
    } else {
      logger(INFO, BRIGHT_BLUE) <<
        "----- BLOCK ADDED AS ALTERNATIVE ON HEIGHT " << bei.height
        << ENDL << "id:\t" << id
        << ENDL << "PoW:\t" << proof_of_work
        << ENDL << "difficulty:\t" << current_diff;
      if (sendNewAlternativeBlockMessage) {
        sendMessage(BlockchainMessage(NewAlternativeBlockMessage(id)));
      }
      return true;
    }
  } else {
    //block orphaned
    bvc.m_marked_as_orphaned = true;
    logger(INFO, BRIGHT_RED) <<
      "Block recognized as orphaned and rejected, id = " << id;
  }

  return true;
}

bool Blockchain::getBlocks(uint32_t start_offset, uint32_t count, std::list<Block>& blocks, std::list<Transaction>& txs) {
  /*std::lock_guard<decltype(m_blockchain_lock)> lk(m_blockchain_lock);
  if (start_offset >= m_blocks.size())
    return false;
  for (size_t i = start_offset; i < start_offset + count && i < m_blocks.size(); i++) {
    blocks.push_back(m_blocks[i].bl);
    std::list<Crypto::Hash> missed_ids;
    getTransactions(m_blocks[i].bl.transactionHashes, txs, missed_ids);
    if (!(!missed_ids.size())) { logger(ERROR, BRIGHT_RED) << "have missed transactions in own block in main blockchain"; return false; }
  }*/

  std::lock_guard<decltype(m_blockchain_lock)> lk(m_blockchain_lock);

  if (start_offset >= m_height.load(std::memory_order_relaxed))
    return false;

  uint32_t cnt = 0;
  auto middle = Common::write_varint_sqlite4(start_offset);
  for (DB::Cursor cur = m_db.begin(BLOCK_INDEX_PREFIX, middle); !cur.end(); ++cnt, cur.next()) {
    auto v = cur.get_value_array();
    Crypto::Hash id;
    std::copy(v.begin(), v.end(), id.data);
    auto key = BLOCK_PREFIX + DB::to_binary_key(id.data, sizeof(id.data)) + BLOCK_SUFFIX;
    BinaryArray ba;
    m_db.get(key, ba);
    BlockEntry e;
    fromBinaryArray(e, ba);
    blocks.push_back(e.bl);
    std::list<Crypto::Hash> missed_ids;
    getTransactions(e.bl.transactionHashes, txs, missed_ids);
    if (!(!missed_ids.size())) { logger(ERROR, BRIGHT_RED) << "have missed transactions in own block in main blockchain"; return false; }

    if (cnt > count)
      break;
  }

  return true;
}

bool Blockchain::getBlocks(uint32_t start_offset, uint32_t count, std::list<Block>& blocks) {
  /*std::lock_guard<decltype(m_blockchain_lock)> lk(m_blockchain_lock);
  if (start_offset >= m_blocks.size()) {
    return false;
  }

  for (uint32_t i = start_offset; i < start_offset + count && i < m_blocks.size(); i++) {
    blocks.push_back(m_blocks[i].bl);
  }*/

  std::lock_guard<decltype(m_blockchain_lock)> lk(m_blockchain_lock);

  if (start_offset >= m_height.load(std::memory_order_relaxed))
    return false;

  uint32_t cnt = 0;
  auto middle = Common::write_varint_sqlite4(start_offset);
  for (DB::Cursor cur = m_db.begin(BLOCK_INDEX_PREFIX, middle); !cur.end(); ++cnt, cur.next()) {
    auto v = cur.get_value_array();
    Crypto::Hash id;
    std::copy(v.begin(), v.end(), id.data);
    auto key = BLOCK_PREFIX + DB::to_binary_key(id.data, sizeof(id.data)) + BLOCK_SUFFIX;
    BinaryArray ba;
    m_db.get(key, ba);
    BlockEntry e;
    fromBinaryArray(e, ba);
    blocks.push_back(e.bl);
    
    if (cnt > count)
      break;
  }

  return true;
}

bool Blockchain::handleGetObjects(NOTIFY_REQUEST_GET_OBJECTS::request& arg, NOTIFY_RESPONSE_GET_OBJECTS::request& rsp) { //Deprecated. Should be removed with CryptoNoteProtocolHandler.
  std::lock_guard<decltype(m_blockchain_lock)> lk(m_blockchain_lock);
  rsp.current_blockchain_height = getCurrentBlockchainHeight();
  std::list<Block> blocks;
  getBlocks(arg.blocks, blocks, rsp.missed_ids);
  for (const auto& bl : blocks) {
    std::list<Crypto::Hash> missed_tx_id;
    std::list<Transaction> txs;
    getTransactions(bl.transactionHashes, txs, rsp.missed_ids);
    if (!(!missed_tx_id.size())) { logger(ERROR, BRIGHT_RED) << "Internal error: have missed missed_tx_id.size()=" << missed_tx_id.size() << ENDL << "for block id = " << get_block_hash(bl); return false; } //WTF???
    rsp.blocks.push_back(block_complete_entry());
    block_complete_entry& e = rsp.blocks.back();
    //pack block
    e.block = asString(toBinaryArray(bl));
    //pack transactions
    for (Transaction& tx : txs) {
      e.txs.push_back(asString(toBinaryArray(tx)));
    }
  }

  //get another transactions, if need
  std::list<Transaction> txs;
  getTransactions(arg.txs, txs, rsp.missed_ids);
  //pack aside transactions
  for (const auto& tx : txs) {
    rsp.txs.push_back(asString(toBinaryArray(tx)));
  }

  return true;
}

bool Blockchain::getAlternativeBlocks(std::list<Block>& blocks) {
  std::lock_guard<decltype(m_blockchain_lock)> lk(m_blockchain_lock);
  for (auto& alt_bl : m_alternative_chains) {
    blocks.push_back(alt_bl.second.bl);
  }

  return true;
}

uint32_t Blockchain::getAlternativeBlocksCount() {
  std::lock_guard<decltype(m_blockchain_lock)> lk(m_blockchain_lock);
  return static_cast<uint32_t>(m_alternative_chains.size());
}

bool Blockchain::add_out_to_get_random_outs(std::vector<std::pair<TransactionIndex, uint16_t>>& amount_outs, COMMAND_RPC_GET_RANDOM_OUTPUTS_FOR_AMOUNTS::outs_for_amount& result_outs, uint64_t amount, size_t i) {
  std::lock_guard<decltype(m_blockchain_lock)> lk(m_blockchain_lock);
  const Transaction& tx = transactionByIndex(amount_outs[i].first).tx;
  if (!(tx.outputs.size() > amount_outs[i].second)) {
    logger(ERROR, BRIGHT_RED) << "internal error: in global outs index, transaction out index="
      << amount_outs[i].second << " more than transaction outputs = " << tx.outputs.size() << ", for tx id = " << getObjectHash(tx); return false;
  }
  if (!(tx.outputs[amount_outs[i].second].target.type() == typeid(KeyOutput))) { logger(ERROR, BRIGHT_RED) << "unknown tx out type"; return false; }

  //check if transaction is unlocked
  if (!is_tx_spendtime_unlocked(tx.unlockTime))
    return false;

  COMMAND_RPC_GET_RANDOM_OUTPUTS_FOR_AMOUNTS::out_entry& oen = *result_outs.outs.insert(result_outs.outs.end(), COMMAND_RPC_GET_RANDOM_OUTPUTS_FOR_AMOUNTS::out_entry());
  oen.global_amount_index = static_cast<uint32_t>(i);
  oen.out_key = boost::get<KeyOutput>(tx.outputs[amount_outs[i].second].target).key;
  return true;
}

size_t Blockchain::find_end_of_allowed_index(const std::vector<std::pair<TransactionIndex, uint16_t>>& amount_outs) {
  std::lock_guard<decltype(m_blockchain_lock)> lk(m_blockchain_lock);
  if (amount_outs.empty()) {
    return 0;
  }

  size_t i = amount_outs.size();
  do {
    --i;
    if (amount_outs[i].first.block + (amount_outs[i].first.block < CryptoNote::parameters::UPGRADE_HEIGHT_V5 ? m_currency.minedMoneyUnlockWindow() : m_currency.minedMoneyUnlockWindow_v1()) <= getCurrentBlockchainHeight()) {
      return i + 1;
    }
  } while (i != 0);

  return 0;
}

bool Blockchain::getRandomOutsByAmount(const COMMAND_RPC_GET_RANDOM_OUTPUTS_FOR_AMOUNTS::request& req, COMMAND_RPC_GET_RANDOM_OUTPUTS_FOR_AMOUNTS::response& res) {
  std::lock_guard<decltype(m_blockchain_lock)> lk(m_blockchain_lock);

  for (uint64_t amount : req.amounts) {
    COMMAND_RPC_GET_RANDOM_OUTPUTS_FOR_AMOUNTS::outs_for_amount& result_outs = *res.outs.insert(res.outs.end(), COMMAND_RPC_GET_RANDOM_OUTPUTS_FOR_AMOUNTS::outs_for_amount());
    result_outs.amount = amount;
    //auto it = m_outputs.find(amount);
    //if (it == m_outputs.end()) {

    BinaryArray ba;
    const auto key = OUTPUTS_INDEX_PREFIX + Common::write_varint_sqlite4(amount);
    if (!m_db.get(key, ba)) {
      logger(ERROR, BRIGHT_RED) <<
        "COMMAND_RPC_GET_RANDOM_OUTPUTS_FOR_AMOUNTS: not outs for amount " << amount << ", wallet should use some real outs when it lookup for some mix, so, at least one out for this amount should exist";
      continue;//actually this is strange situation, wallet should use some real outs when it lookup for some mix, so, at least one out for this amount should exist
    }
    OutputsEntry oe;
    if (!fromBinaryArray(oe, ba)) {
      throw std::runtime_error("Blockchain::getRandomOutsByAmount, failed to parse output entry from DB");
    }

    //std::vector<std::pair<TransactionIndex, uint16_t>>& amount_outs = it->second;
    std::vector<std::pair<TransactionIndex, uint16_t>>& amount_outs = oe.outputs;
    //it is not good idea to use top fresh outs, because it increases possibility of transaction canceling on split
    //lets find upper bound of not fresh outs
    size_t up_index_limit = find_end_of_allowed_index(amount_outs);
    if (!(up_index_limit <= amount_outs.size())) { logger(ERROR, BRIGHT_RED) << "internal error: find_end_of_allowed_index returned wrong index=" << up_index_limit << ", with amount_outs.size = " << amount_outs.size(); return false; }

	if(amount_outs.size() > req.outs_count)
    {
      std::set<size_t> used;
      size_t try_count = 0;
      for(uint64_t j = 0; j != req.outs_count && try_count < up_index_limit;)
      {
	    // triangular distribution over [a,b) with a=0, mode c=b=up_index_limit
        uint64_t r = Random::randomValue<size_t>() % ((uint64_t)1 << 53);
        double frac = std::sqrt((double)r / ((uint64_t)1 << 53));
        size_t i = (size_t)(frac*up_index_limit);
        if(used.count(i))
          continue;
        bool added = add_out_to_get_random_outs(amount_outs, result_outs, amount, i);
        used.insert(i);
        if(added)
          ++j;
        ++try_count;
      }
    }else
    {
      for(size_t i = 0; i != up_index_limit; i++)
        add_out_to_get_random_outs(amount_outs, result_outs, amount, i);
    }
  }
  return true;
}

bool Blockchain::findSupplement(const std::vector<Crypto::Hash>& ids, uint32_t& offset) {
  for (const auto& id : ids) {
    if (getBlockHeight(id, offset)) {
      return true;
    }
  }

  return false;
}

uint32_t Blockchain::findBlockchainSupplement(const std::vector<Crypto::Hash>& qblock_ids) {
  std::lock_guard<decltype(m_blockchain_lock)> lk(m_blockchain_lock);
  assert(!qblock_ids.empty());
  //assert(qblock_ids.back() == m_blockIndex.getBlockId(0));
  assert(qblock_ids.back() == getBlockIdByHeight(0));

  uint32_t blockIndex;
  // assert above guarantees that method returns true
  findSupplement(qblock_ids, blockIndex);
  return blockIndex;
}

uint64_t Blockchain::blockDifficulty(size_t i) {
  std::lock_guard<decltype(m_blockchain_lock)> lk(m_blockchain_lock);
  if (!(i < m_height.load(std::memory_order_relaxed))) { logger(ERROR, BRIGHT_RED) << "wrong block index i = " << i << " at Blockchain::block_difficulty()"; return false; }
  if (i == 0) {
    return 1;
  }

  BlockEntry e1, e2;
  getBlockEntryByHeight(i, e1);
  getBlockEntryByHeight(i - 1, e2);

  //return m_blocks[i].cumulative_difficulty - m_blocks[i - 1].cumulative_difficulty;
  return e1.cumulative_difficulty - e2.cumulative_difficulty;
}

uint64_t Blockchain::blockCumulativeDifficulty(size_t i) {
  std::lock_guard<decltype(m_blockchain_lock)> lk(m_blockchain_lock);
  if (!(i < m_height.load(std::memory_order_relaxed))) { logger(ERROR, BRIGHT_RED) << "wrong block index i = " << i << " at Blockchain::block_difficulty()"; return false; }

  BlockEntry e;
  getBlockEntryByHeight(i, e);

  return e.cumulative_difficulty;
}

bool Blockchain::getblockEntry(size_t i, uint64_t& block_cumulative_size, difficulty_type& difficulty, uint64_t& already_generated_coins, uint64_t& reward, uint64_t& transactions_count, uint64_t& timestamp) {
  std::lock_guard<decltype(m_blockchain_lock)> lk(m_blockchain_lock);
  if (!(i < m_height.load(std::memory_order_relaxed))) { logger(ERROR, BRIGHT_RED) << "wrong block index i = " << i << " at Blockchain::get_block_entry()"; return false; }

  BlockEntry e1, e2;
  getBlockEntryByHeight(i, e1);
  getBlockEntryByHeight(i - 1, e2);

  block_cumulative_size = e1.block_cumulative_size;
  difficulty = e1.cumulative_difficulty - e2.cumulative_difficulty;
  already_generated_coins = e1.already_generated_coins;
  reward = e1.already_generated_coins - e2.already_generated_coins;
  timestamp = e1.bl.timestamp;
  transactions_count = e1.bl.transactionHashes.size();

  return true;
}

void Blockchain::print_blockchain(uint64_t start_index, uint64_t end_index) {
  std::stringstream ss;
  std::lock_guard<decltype(m_blockchain_lock)> lk(m_blockchain_lock);
  if (start_index >= m_height.load(std::memory_order_relaxed)) {
    logger(INFO, BRIGHT_WHITE) <<
      "Wrong starter index set: " << start_index << ", expected max index " << m_height.load(std::memory_order_relaxed) - 1;
    return;
  }

  size_t i = start_index;
  auto middle = Common::write_varint_sqlite4(start_index);
  for (DB::Cursor cur = m_db.begin(BLOCK_INDEX_PREFIX, middle); (i != m_height.load(std::memory_order_relaxed) && i != end_index) || !cur.end(); i++, cur.next()) {
    auto v = cur.get_value_array();
    Crypto::Hash id;
    std::copy(v.begin(), v.end(), id.data);
    auto key = BLOCK_PREFIX + DB::to_binary_key(id.data, sizeof(id.data)) + BLOCK_SUFFIX;
    BinaryArray ba;
    m_db.get(key, ba);
    BlockEntry e;
    fromBinaryArray(e, ba);

    ss << "height " << i << ", timestamp " << e.bl.timestamp << ", cumul_dif " << e.cumulative_difficulty << ", cumul_size " << e.block_cumulative_size
      << "\nid\t\t" << get_block_hash(e.bl)
      << "\ndifficulty\t\t" << blockDifficulty(i) << ", nonce " << e.bl.nonce << ", tx_count " << e.bl.transactionHashes.size() << ENDL;
  }
  /*for (size_t i = start_index; i != m_blocks.size() && i != end_index; i++) {
    ss << "height " << i << ", timestamp " << m_blocks[i].bl.timestamp << ", cumul_dif " << m_blocks[i].cumulative_difficulty << ", cumul_size " << m_blocks[i].block_cumulative_size
      << "\nid\t\t" << get_block_hash(m_blocks[i].bl)
      << "\ndifficulty\t\t" << blockDifficulty(i) << ", nonce " << m_blocks[i].bl.nonce << ", tx_count " << m_blocks[i].bl.transactionHashes.size() << ENDL;
  }*/
  logger(DEBUGGING) <<
    "Current blockchain:" << ENDL << ss.str();
  logger(INFO, BRIGHT_WHITE) <<
    "Blockchain printed with log level 1";
}

void Blockchain::print_blockchain_index() {
  std::stringstream ss;
  std::lock_guard<decltype(m_blockchain_lock)> lk(m_blockchain_lock);

  std::vector<Crypto::Hash> blockIds = getBlockIds(0, std::numeric_limits<uint32_t>::max());   //m_blockIndex.getBlockIds(0, std::numeric_limits<uint32_t>::max());
  logger(INFO, BRIGHT_WHITE) << "Current blockchain index:";

  size_t height = 0;
  for (auto i = blockIds.begin(); i != blockIds.end(); ++i, ++height) {
    logger(INFO, BRIGHT_WHITE) << "id\t\t" << *i << " height" << height;
  }

}

void Blockchain::print_blockchain_outs(const std::string& file) {
  // TODO DB
  /*std::stringstream ss;
  std::lock_guard<decltype(m_blockchain_lock)> lk(m_blockchain_lock);
  for (const outputs_container::value_type& v : m_outputs) {
    const std::vector<std::pair<TransactionIndex, uint16_t>>& vals = v.second;
    if (!vals.empty()) {
      ss << "amount: " << v.first << ENDL;
      for (size_t i = 0; i != vals.size(); i++) {
        ss << "\t" << getObjectHash(transactionByIndex(vals[i].first).tx) << ": " << vals[i].second << ENDL;
      }
    }
  }

  if (Common::saveStringToFile(file, ss.str())) {
    logger(INFO, BRIGHT_WHITE) <<
      "Current outputs index writen to file: " << file;
  } else {
    logger(WARNING, BRIGHT_YELLOW) <<
      "Failed to write current outputs index to file: " << file;
  }*/
}

std::vector<Crypto::Hash> Blockchain::findBlockchainSupplement(const std::vector<Crypto::Hash>& remoteBlockIds, size_t maxCount,
  uint32_t& totalBlockCount, uint32_t& startBlockIndex) {

  assert(!remoteBlockIds.empty());
  //assert(remoteBlockIds.back() == m_blockIndex.getBlockId(0));
  assert(remoteBlockIds.back() == getBlockIdByHeight(0));

  std::lock_guard<decltype(m_blockchain_lock)> lk(m_blockchain_lock);
  totalBlockCount = getCurrentBlockchainHeight();
  startBlockIndex = findBlockchainSupplement(remoteBlockIds);

  //return m_blockIndex.getBlockIds(startBlockIndex, static_cast<uint32_t>(maxCount));
  return getBlockIds(startBlockIndex, static_cast<uint32_t>(maxCount));
}

bool Blockchain::haveBlock(const Crypto::Hash& id) {
  std::lock_guard<decltype(m_blockchain_lock)> lk(m_blockchain_lock);
  //if (m_blockIndex.hasBlock(id))
  //  return true;

  BinaryArray ba;
  auto key = BLOCK_PREFIX + DB::to_binary_key(id.data, sizeof(id.data)) + BLOCK_SUFFIX;
  if (m_db.get(key, ba))
    return true;
  
  if (m_alternative_chains.count(id))
    return true;

  return false;
}

size_t Blockchain::getTotalTransactions() {
  std::lock_guard<decltype(m_blockchain_lock)> lk(m_blockchain_lock);
  //return m_transactionMap.size();

  //GENERATED_TRANSACTIONS_INDEX_PREFIX
  return m_lastGeneratedTxNumber;
}

bool Blockchain::getTransactionOutputGlobalIndexes(const Crypto::Hash& tx_id, std::vector<uint32_t>& indexs) {
  /*std::lock_guard<decltype(m_blockchain_lock)> lk(m_blockchain_lock);
  auto it = m_transactionMap.find(tx_id);
  if (it == m_transactionMap.end()) {
    logger(WARNING, YELLOW) << "warning: get_tx_outputs_gindexs failed to find transaction with id = " << tx_id;
    return false;
  }*/

  std::lock_guard<decltype(m_blockchain_lock)> lk(m_blockchain_lock);

  BinaryArray ba;
  if (!m_db.get(TRANSACTIONS_INDEX_PREFIX + DB::to_binary_key(tx_id.data, sizeof(tx_id.data)), ba)) {
    logger(WARNING, YELLOW) << "warning: get_tx_outputs_gindexs failed to find transaction with id = " << tx_id;
    return false;
  }
  TransactionIndex ti;
  if (!fromBinaryArray(ti, ba)) {
    logger(WARNING, YELLOW) << "warning: get_tx_outputs_gindexs failed to parse DB record";
      return false;
  }

  //const TransactionEntry& tx = transactionByIndex(it->second);
  const TransactionEntry& tx = transactionByIndex(ti);
  if (!(tx.m_global_output_indexes.size())) { logger(ERROR, BRIGHT_RED) << "internal error: global indexes for transaction " << tx_id << " is empty"; return false; }
  indexs.resize(tx.m_global_output_indexes.size());
  for (size_t i = 0; i < tx.m_global_output_indexes.size(); ++i) {
    indexs[i] = tx.m_global_output_indexes[i];
  }

  return true;
}

bool Blockchain::get_out_by_msig_gindex(uint64_t amount, uint64_t gindex, MultisignatureOutput& out) {
  std::lock_guard<decltype(m_blockchain_lock)> lk(m_blockchain_lock);
  //auto it = m_multisignatureOutputs.find(amount);
  //if (it == m_multisignatureOutputs.end()) {
  //  return false;
  //}

  BinaryArray ba;
  const auto key = MULTUSIGNATURE_OUTPUTS_INDEX_PREFIX + Common::write_varint_sqlite4(amount);
  if (!m_db.get(key, ba)) {
    return false;
  }
  MultisignatureOutputEntry me;
  if (!fromBinaryArray(me, ba)) {
    throw std::runtime_error("Blockchain::get_out_by_msig_gindex, failed to parse multisignature outputs entry from DB");
  }

  //if (it->second.size() <= gindex) {
  if (me.multisignatureOutputs.size() <= gindex) {
    return false;
  }

  //auto msigUsage = it->second[gindex];
  auto msigUsage = me.multisignatureOutputs[gindex];
  auto& targetOut = transactionByIndex(msigUsage.transactionIndex).tx.outputs[msigUsage.outputIndex].target;
  if (targetOut.type() != typeid(MultisignatureOutput)) {
    return false;
  }

  out = boost::get<MultisignatureOutput>(targetOut);
  return true;
}


bool Blockchain::checkTransactionInputs(const Transaction& tx, uint32_t& max_used_block_height, Crypto::Hash& max_used_block_id, BlockInfo* tail) {
  std::lock_guard<decltype(m_blockchain_lock)> lk(m_blockchain_lock);

  if (tail)
    tail->id = getTailId(tail->height);

  bool res = checkTransactionInputs(tx, &max_used_block_height);
  if (!res) return false;
  if (!(max_used_block_height < m_height.load(std::memory_order_relaxed))) { logger(ERROR, BRIGHT_RED) << "internal error: max used block index="
    << max_used_block_height << " is not less then blockchain size = " << m_height.load(std::memory_order_relaxed); return false; }
  //get_block_hash(m_blocks[max_used_block_height].bl, max_used_block_id);
  max_used_block_id = getBlockIdByHeight(max_used_block_height);

  return true;
}

bool Blockchain::haveTransactionKeyImagesAsSpent(const Transaction &tx) {
  for (const auto& in : tx.inputs) {
    if (in.type() == typeid(KeyInput)) {
      if (have_tx_keyimg_as_spent(boost::get<KeyInput>(in).keyImage)) {
        return true;
      }
    }
  }

  return false;
}

bool Blockchain::checkTransactionInputs(const Transaction& tx, uint32_t* pmax_used_block_height) {
  Crypto::Hash tx_prefix_hash = getObjectHash(*static_cast<const TransactionPrefix*>(&tx));
  return checkTransactionInputs(tx, tx_prefix_hash, pmax_used_block_height);
}

bool Blockchain::checkTransactionInputs(const Transaction& tx, const Crypto::Hash& tx_prefix_hash, uint32_t* pmax_used_block_height) {
  size_t inputIndex = 0;
  if (pmax_used_block_height) {
    *pmax_used_block_height = 0;
  }

  Crypto::Hash transactionHash = getObjectHash(tx);
  for (const auto& txin : tx.inputs) {
    assert(inputIndex < tx.signatures.size());
    if (txin.type() == typeid(KeyInput)) {

      const KeyInput& in_to_key = boost::get<KeyInput>(txin);
      if (!(!in_to_key.outputIndexes.empty())) { logger(ERROR, BRIGHT_RED) << "empty in_to_key.outputIndexes in transaction with id " << getObjectHash(tx); return false; }

      // DB will throw on attempt to add spent keyimage, maybe not necessary here
      if (have_tx_keyimg_as_spent(in_to_key.keyImage)) {
        logger(DEBUGGING) <<
          "Key image already spent in blockchain: " << Common::podToHex(in_to_key.keyImage);
        return false;
      }

      if (!isInCheckpointZone(getCurrentBlockchainHeight())) {
        if (!check_tx_input(in_to_key, tx_prefix_hash, tx.signatures[inputIndex], pmax_used_block_height)) {
          logger(INFO, BRIGHT_WHITE) <<
            "Failed to check input in transaction " << transactionHash;
          return false;
        }
      }

      ++inputIndex;
    } else if (txin.type() == typeid(MultisignatureInput)) {
      if (!isInCheckpointZone(getCurrentBlockchainHeight())) {
        if (!validateInput(::boost::get<MultisignatureInput>(txin), transactionHash, tx_prefix_hash, tx.signatures[inputIndex])) {
          return false;
        }
      }
      ++inputIndex;
    } else {
      logger(INFO, BRIGHT_WHITE) <<
        "Transaction << " << transactionHash << " contains input of unsupported type.";
      return false;
    }
  }

  return true;
}

bool Blockchain::is_tx_spendtime_unlocked(uint64_t unlock_time) {
  if (unlock_time < m_currency.maxBlockHeight()) {
    //interpret as block index
    if (getCurrentBlockchainHeight() - 1 + m_currency.lockedTxAllowedDeltaBlocks() >= unlock_time)
      return true;
    else
      return false;
  } else {
    //interpret as time

    // compare with last block timestamp + delta seconds
    const uint64_t lastBlockTimestamp = getBlockTimestamp(getCurrentBlockchainHeight() - 1);
    if (lastBlockTimestamp + m_currency.lockedTxAllowedDeltaSeconds() >= unlock_time)
      return true;
    else
      return false;
  }

  return false;
}

bool Blockchain::is_tx_spendtime_unlocked(uint64_t unlock_time, uint32_t height) {
  if (unlock_time < m_currency.maxBlockHeight()) {
    //interpret as block index
    if (height - 1 + m_currency.lockedTxAllowedDeltaBlocks() >= unlock_time)
      return true;
  }
  
  return false;
}

bool Blockchain::check_tx_input(const KeyInput& txin, const Crypto::Hash& tx_prefix_hash, const std::vector<Crypto::Signature>& sig, uint32_t* pmax_related_block_height) {
  std::lock_guard<decltype(m_blockchain_lock)> lk(m_blockchain_lock);

  struct outputs_visitor {
    std::vector<const Crypto::PublicKey *>& m_results_collector;
    Blockchain& m_bch;
    LoggerRef logger;
    outputs_visitor(std::vector<const Crypto::PublicKey *>& results_collector, Blockchain& bch, ILogger& logger) :m_results_collector(results_collector), m_bch(bch), logger(logger, "outputs_visitor") {
    }

    bool handle_output(const Transaction& tx, const TransactionOutput& out, size_t transactionOutputIndex) {
      //check tx unlock time
      if (!m_bch.is_tx_spendtime_unlocked(tx.unlockTime)) {
        logger(INFO, BRIGHT_WHITE) <<
          "One of outputs for one of inputs have wrong tx.unlockTime = " << tx.unlockTime;
        return false;
      }

      if (out.target.type() != typeid(KeyOutput)) {
        logger(INFO, BRIGHT_WHITE) <<
          "Output have wrong type id, which=" << out.target.which();
        return false;
      }

      m_results_collector.push_back(&boost::get<KeyOutput>(out.target).key);
      return true;
    }
  };

  // additional key_image check, fix discovered by Monero Lab and suggested by "fluffypony" (bitcointalk.org)
  if (!(scalarmultKey(txin.keyImage, Crypto::EllipticCurveScalar2KeyImage(Crypto::L)) == Crypto::EllipticCurveScalar2KeyImage(Crypto::I))) {
    logger(ERROR) << "Transaction uses key image not in the valid domain";
    return false;
  }

  //check ring signature
  std::vector<const Crypto::PublicKey *> output_keys;
  outputs_visitor vi(output_keys, *this, logger.getLogger());
  if (!scanOutputKeysForIndexes(txin, vi, pmax_related_block_height)) {
    logger(INFO, BRIGHT_WHITE) <<
      "Failed to get output keys for tx with amount = " << m_currency.formatAmount(txin.amount) <<
      " and count indexes " << txin.outputIndexes.size();
    return false;
  }

  if (txin.outputIndexes.size() != output_keys.size()) {
    logger(INFO, BRIGHT_WHITE) <<
      "Output keys for tx with amount = " << txin.amount << " and count indexes " << txin.outputIndexes.size() << " returned wrong keys count " << output_keys.size();
    return false;
  }

  if (!(sig.size() == output_keys.size())) { logger(ERROR, BRIGHT_RED) << "internal error: tx signatures count=" << sig.size() << " mismatch with outputs keys count for inputs=" << output_keys.size(); return false; }
  if (isInCheckpointZone(getCurrentBlockchainHeight())) {
    return true;
  }

  bool check_tx_ring_signature = Crypto::check_ring_signature(tx_prefix_hash, txin.keyImage, output_keys, sig.data());
  if (!check_tx_ring_signature) {
    logger(ERROR) << "Failed to check ring signature for keyImage: " << txin.keyImage;
  }
  return check_tx_ring_signature;
}

uint64_t Blockchain::get_adjusted_time() {
  //TODO: add collecting median time
  return time(NULL);
}

bool Blockchain::check_block_timestamp_main(const Block& b) {
  if (b.timestamp > get_adjusted_time() + m_currency.blockFutureTimeLimit(b.majorVersion)) {
    logger(INFO, BRIGHT_WHITE) <<
      "Timestamp of block with id: " << get_block_hash(b) << ", " << b.timestamp << ", bigger than adjusted time + 28 min.";
    return false;
  }

  std::vector<uint64_t> timestamps;
  size_t offset = m_height.load(std::memory_order_relaxed) <= m_currency.timestampCheckWindow(b.majorVersion) ? 0 : m_height.load(std::memory_order_relaxed) - m_currency.timestampCheckWindow(b.majorVersion);
  //for (; offset != m_blocks.size(); ++offset) {
  //  timestamps.push_back(m_blocks[offset].bl.timestamp);
  //}
  auto middle = Common::write_varint_sqlite4(offset);
  for (DB::Cursor cur = m_db.begin(BLOCK_INDEX_PREFIX, middle); offset != m_height.load(std::memory_order_relaxed) || !cur.end(); ++offset, cur.next()) {
    auto v = cur.get_value_array();
    Crypto::Hash id;
    std::copy(v.begin(), v.end(), id.data);
    auto key = BLOCK_PREFIX + DB::to_binary_key(id.data, sizeof(id.data)) + BLOCK_SUFFIX;
    BinaryArray ba;
    m_db.get(key, ba);
    BlockEntry e;
    fromBinaryArray(e, ba);
    timestamps.push_back(e.bl.timestamp);
  }

  return check_block_timestamp(std::move(timestamps), b);
}

//------------------------------------------------------------------
// This function takes the timestamps from the most recent <n> blocks,
// where n = BLOCKCHAIN_TIMESTAMP_CHECK_WINDOW. If there are not that many
// blocks in the blockchain, the timestap is assumed to be valid. If there
// are, this function returns:
//   true if the block's timestamp is not less than the median timestamp
//       of the selected blocks
//   false otherwise
bool Blockchain::check_block_timestamp(std::vector<uint64_t> timestamps, const Block& b) {
  if (timestamps.size() < m_currency.timestampCheckWindow(b.majorVersion)) {
    return true;
  }

  uint64_t median_ts = Common::medianValue(timestamps);

  if (b.timestamp < median_ts) {
    logger(INFO, BRIGHT_WHITE) <<
      "Timestamp of block with id: " << get_block_hash(b) << ", " << b.timestamp <<
      ", less than median of last " << m_currency.timestampCheckWindow(b.majorVersion) << " blocks, " << median_ts;
    return false;
  }

  return true;
}

bool Blockchain::checkBlockVersion(const Block& b, const Crypto::Hash& blockHash) {
  uint32_t height = get_block_height(b);
  const uint8_t expectedBlockVersion = getBlockMajorVersionForHeight(height);
  if (b.majorVersion != expectedBlockVersion) {
    logger(TRACE) << "Block " << blockHash << " has wrong major version: " << static_cast<int>(b.majorVersion) <<
      ", at height " << height << " expected version is " << static_cast<int>(expectedBlockVersion);
    return false;
  }

  return true;
}

bool Blockchain::checkParentBlockSize(const Block& b, const Crypto::Hash& blockHash) {
  if (b.majorVersion == BLOCK_MAJOR_VERSION_2 || b.majorVersion == BLOCK_MAJOR_VERSION_3) {
    auto serializer = makeParentBlockSerializer(b, false, false);
    size_t parentBlockSize;
    if (!getObjectBinarySize(serializer, parentBlockSize)) {
      logger(ERROR, BRIGHT_RED) <<
        "Block " << blockHash << ": failed to determine parent block size";
      return false;
    }

    if (parentBlockSize > 2 * 1024) {
      logger(INFO, BRIGHT_WHITE) <<
        "Block " << blockHash << " contains too big parent block: " << parentBlockSize <<
        " bytes, expected no more than " << 2 * 1024 << " bytes";
      return false;
    }
  }

  return true;
}

bool Blockchain::checkCumulativeBlockSize(const Crypto::Hash& blockId, size_t cumulativeBlockSize, uint64_t height) {
  size_t maxBlockCumulativeSize = m_currency.maxBlockCumulativeSize(height);
  if (cumulativeBlockSize > maxBlockCumulativeSize) {
    logger(INFO, BRIGHT_WHITE) <<
      "Block " << blockId << " is too big: " << cumulativeBlockSize << " bytes, " <<
      "expected no more than " << maxBlockCumulativeSize << " bytes";
    return false;
  }

  return true;
}

// Returns true, if cumulativeSize is calculated precisely, else returns false.
bool Blockchain::getBlockCumulativeSize(const Block& block, size_t& cumulativeSize) {
  std::vector<Transaction> blockTxs;
  std::vector<Crypto::Hash> missedTxs;
  getTransactions(block.transactionHashes, blockTxs, missedTxs, true);

  cumulativeSize = getObjectBinarySize(block.baseTransaction);
  for (const Transaction& tx : blockTxs) {
    cumulativeSize += getObjectBinarySize(tx);
  }

  return missedTxs.empty();
}

// Precondition: m_blockchain_lock is locked.
bool Blockchain::update_next_cumulative_size_limit() {
  uint8_t nextBlockMajorVersion = getBlockMajorVersionForHeight(m_height.load(std::memory_order_relaxed));
  size_t nextBlockGrantedFullRewardZone = m_currency.blockGrantedFullRewardZoneByBlockVersion(nextBlockMajorVersion);

  std::vector<size_t> sz;
  get_last_n_blocks_sizes(sz, m_currency.rewardBlocksWindow());

  uint64_t median = Common::medianValue(sz);
  if (median <= nextBlockGrantedFullRewardZone) {
    median = nextBlockGrantedFullRewardZone;
  }

  m_current_block_cumul_sz_limit = median * 2;
  return true;
}

bool Blockchain::addNewBlock(const Block& bl, block_verification_context& bvc) {
  Crypto::Hash id;
  if (!get_block_hash(bl, id)) {
    logger(ERROR, BRIGHT_RED) <<
      "Failed to get block hash, possible block has invalid format";
    bvc.m_verification_failed = true;
    return false;
  }

  bool add_result;

  { //to avoid deadlock lets lock tx_pool for whole add/reorganize process
    std::lock_guard<decltype(m_tx_pool)> poolLock(m_tx_pool);
    std::lock_guard<decltype(m_blockchain_lock)> bcLock(m_blockchain_lock);

    if (haveBlock(id)) {
      logger(TRACE) << "block with id = " << id << " already exists";
      bvc.m_already_exists = true;
      return false;
    }

    //check that block refers to chain tail
    if (!(bl.previousBlockHash == getTailId())) {
      //chain switching or wrong block
      logger(DEBUGGING) << "handling alternative block " << Common::podToHex(id)
                        << " at height " << boost::get<BaseInput>(bl.baseTransaction.inputs.front()).blockIndex 
                        << " as it doesn't refer to chain tail " << Common::podToHex(getTailId())
                        << ", its prev. block hash: " << Common::podToHex(bl.previousBlockHash);
      bvc.m_added_to_main_chain = false;
      add_result = handle_alternative_block(bl, id, bvc);
    } else {
      add_result = pushBlock(bl, id, bvc);
      if (add_result) {
        sendMessage(BlockchainMessage(NewBlockMessage(id)));
      }
    }
  }

  if (add_result && bvc.m_added_to_main_chain) {
    m_observerManager.notify(&IBlockchainStorageObserver::blockchainUpdated);
  }

  return add_result;
}

const Blockchain::TransactionEntry Blockchain::transactionByIndex(TransactionIndex index) {
  //return m_blocks[index.block].transactions[index.transaction];
  std::string s;
  Crypto::Hash h;
  if (!m_db.get(BLOCK_INDEX_PREFIX + Common::write_varint_sqlite4(index.block), s)) {
    logger(ERROR, BRIGHT_RED) << "Blockchain::transactionByIndex, failed to get block index from DB";
  } 
  std::copy(s.begin(), s.end(), h.data);

  auto key = BLOCK_PREFIX + DB::to_binary_key(h.data, sizeof(h.data)) + BLOCK_SUFFIX;
  BinaryArray ba;
  if (!m_db.get(key, ba)) {
    logger(ERROR, BRIGHT_RED) << "Blockchain::transactionByIndex, failed to get block entry from DB";
  }
  BlockEntry e;
  if (!fromBinaryArray(e, ba)) {
    logger(ERROR, BRIGHT_RED) << "Blockchain::transactionByIndex, failed to parse block entry from DB";
  }

  return std::move(e.transactions[index.transaction]);
}

bool Blockchain::pushBlock(const Block& blockData, const Crypto::Hash& id, block_verification_context& bvc) {
  std::vector<Transaction> transactions;
  if (!loadTransactions(blockData, transactions)) {
    bvc.m_verification_failed = true;
    return false;
  }

  if (!pushBlock(blockData, transactions, id, bvc)) {
    saveTransactions(transactions);
    return false;
  }

  return true;
}

bool Blockchain::pushBlock(const Block& blockData, const std::vector<Transaction>& transactions, const Crypto::Hash& blockHash, block_verification_context& bvc) {
  std::lock_guard<decltype(m_blockchain_lock)> lk(m_blockchain_lock);

  auto blockProcessingStart = std::chrono::steady_clock::now();

  // already checked in addNewBlock
  // TODO make sure doesn't influence other cases
  /*if (m_blockIndex.hasBlock(blockHash)) {
    logger(ERROR, BRIGHT_RED) <<
      "Block " << blockHash << " already exists in blockchain.";
    bvc.m_verification_failed = true;
    //bvc.m_already_exists = true;
    return false;
  }*/

  if (!checkBlockVersion(blockData, blockHash)) {
    bvc.m_verification_failed = true;
    return false;
  }

  //if (!checkParentBlockSize(blockData, blockHash)) {
  //  bvc.m_verification_failed = true;
  //  return false;
  //}

  // Disable merged mining
  if (blockData.majorVersion >= CryptoNote::BLOCK_MAJOR_VERSION_5) {
    TransactionExtraMergeMiningTag mmTag;
    if (getMergeMiningTagFromExtra(blockData.baseTransaction.extra, mmTag)) {
      logger(ERROR, BRIGHT_RED) << "Merge mining tag was found in extra of miner transaction";
      return false;
    }
  }

  if (blockData.previousBlockHash != getTailId()) {
    logger(INFO, BRIGHT_WHITE) <<
      "Block " << blockHash << " has wrong previousBlockHash: " << blockData.previousBlockHash << ", expected: " << getTailId();
    bvc.m_verification_failed = true;
    return false;
  }

  if (!m_checkpoints.is_in_checkpoint_zone(getCurrentBlockchainHeight())) {
    // make sure block timestamp is not less than the median timestamp
    // of a set number of the most recent blocks.
    if (!check_block_timestamp_main(blockData)) {
      logger(INFO, BRIGHT_WHITE) <<
        "Block " << blockHash << " has invalid timestamp: " << blockData.timestamp;
      bvc.m_verification_failed = true;
      return false;
    }
  }

  // have to calc. current difficulty, can't skip under checkpoints
  auto targetTimeStart = std::chrono::steady_clock::now();
  difficulty_type currentDifficulty = getDifficultyForNextBlock();
  auto target_calculating_time = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - targetTimeStart).count();

  if (!(currentDifficulty)) {
    logger(ERROR, BRIGHT_RED) << "!!!!!!!!! difficulty overhead !!!!!!!!!";
    return false;
  }

  auto longhashTimeStart = std::chrono::steady_clock::now();
  Crypto::Hash proof_of_work = NULL_HASH;

  if (m_checkpoints.is_in_checkpoint_zone(getCurrentBlockchainHeight())) {
    if (!m_checkpoints.check_block(getCurrentBlockchainHeight(), blockHash)) {
      logger(ERROR, BRIGHT_RED) <<
        "CHECKPOINT VALIDATION FAILED";
      bvc.m_verification_failed = true;
      return false;
    }
  } else {
    if (!m_currency.checkProofOfWork(m_cn_context, blockData, currentDifficulty, proof_of_work)) {
      logger(INFO, BRIGHT_WHITE) <<
        "Block " << blockHash << ", has too weak proof of work: " << proof_of_work << ", expected difficulty: " << currentDifficulty;
      bvc.m_verification_failed = true;
      return false;
    }
  }

  auto longhash_calculating_time = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - longhashTimeStart).count();

  if (!m_checkpoints.is_in_checkpoint_zone(getCurrentBlockchainHeight()) && !prevalidate_miner_transaction(blockData, m_height.load(std::memory_order_relaxed))) { // blockchain height (incl. zero block)
    logger(INFO, BRIGHT_WHITE) <<
      "Block " << blockHash << " failed to pass prevalidation";
    bvc.m_verification_failed = true;
    return false;
  }

  Crypto::Hash minerTransactionHash = getObjectHash(blockData.baseTransaction);

  BlockEntry block;
  block.bl = blockData;
  

  DB::Cursor cur = m_db.rbegin(BLOCK_INDEX_PREFIX);
  m_height.store(cur.end() ? 0 : Common::integer_cast<uint32_t>(Common::read_varint_sqlite4(cur.get_suffix())) + 1, std::memory_order_relaxed);
  auto v = cur.get_value_array();
  Crypto::Hash id;
  std::copy(v.begin(), v.end(), id.data);
  auto key = BLOCK_PREFIX + DB::to_binary_key(id.data, sizeof(id.data)) + BLOCK_SUFFIX;
  BinaryArray ba;
  m_db.get(key, ba);
  BlockEntry e;
  fromBinaryArray(e, ba);

  // block.height = static_cast<uint32_t>(m_blocks.size());
  // block.height is used in pushTransaction()
  // blockchain height (incl. zero block!)
  block.height = m_height.load(std::memory_order_relaxed);

  block.transactions.resize(1);
  block.transactions[0].tx = blockData.baseTransaction;
  TransactionIndex transactionIndex = { m_height.load(std::memory_order_relaxed), static_cast<uint16_t>(0) };
  pushTransaction(block, minerTransactionHash, transactionIndex);

  size_t coinbase_blob_size = getObjectBinarySize(blockData.baseTransaction);
  size_t cumulative_block_size = coinbase_blob_size;
  uint64_t fee_summary = 0;
  for (size_t i = 0; i < transactions.size(); ++i) {
    const Crypto::Hash& tx_id = blockData.transactionHashes[i];
    block.transactions.resize(block.transactions.size() + 1);
    size_t blob_size = 0;
    uint64_t fee = 0;
    block.transactions.back().tx = transactions[i];

    blob_size = toBinaryArray(block.transactions.back().tx).size();
    fee = getInputAmount(block.transactions.back().tx) - getOutputAmount(block.transactions.back().tx);
    if (!m_checkpoints.is_in_checkpoint_zone(getCurrentBlockchainHeight()) && !checkTransactionInputs(block.transactions.back().tx)) {
      logger(INFO, BRIGHT_WHITE) <<
        "Block " << blockHash << " has at least one transaction with wrong inputs: " << tx_id;
      bvc.m_verification_failed = true;

      block.transactions.pop_back();
      popTransactions(block, minerTransactionHash);
      return false;
    }

    ++transactionIndex.transaction;
    pushTransaction(block, tx_id, transactionIndex);

    cumulative_block_size += blob_size;
    fee_summary += fee;
  }

  if (!checkCumulativeBlockSize(blockHash, cumulative_block_size, m_height.load(std::memory_order_relaxed))) {
    bvc.m_verification_failed = true;
    return false;
  }

  int64_t emissionChange = 0;
  uint64_t reward = 0;
  uint64_t already_generated_coins = cur.end() ? 0 : e.already_generated_coins;
  if (!m_checkpoints.is_in_checkpoint_zone(getCurrentBlockchainHeight()) && !validate_miner_transaction(blockData, m_height.load(std::memory_order_relaxed), cumulative_block_size, already_generated_coins, fee_summary, reward, emissionChange)) {
    logger(INFO, BRIGHT_WHITE) << "Block " << blockHash << " has invalid miner transaction";
    bvc.m_verification_failed = true;
    popTransactions(block, minerTransactionHash);
    return false;
  }

  block.block_cumulative_size = cumulative_block_size;
  block.cumulative_difficulty = currentDifficulty;
  block.already_generated_coins = already_generated_coins + emissionChange;
  if (!cur.end()) {
    block.cumulative_difficulty += e.cumulative_difficulty;
  }

  pushBlock(block, blockHash); // TODO use if (!pushBlock(block, blockHash)) return false; ??

  auto block_processing_time = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - blockProcessingStart).count();

  logger(DEBUGGING) <<
    "+++++ BLOCK SUCCESSFULLY ADDED" << ENDL << "id:\t" << blockHash
    << ENDL << "PoW:\t" << proof_of_work
    << ENDL << "HEIGHT " << block.height << ", difficulty:\t" << currentDifficulty
    << ENDL << "block reward: " << m_currency.formatAmount(reward) << ", fee = " << m_currency.formatAmount(fee_summary)
    << ", coinbase_blob_size: " << coinbase_blob_size << ", cumulative size: " << cumulative_block_size
    << ", " << block_processing_time << "(" << target_calculating_time << "/" << longhash_calculating_time << ")ms";

  bvc.m_added_to_main_chain = true;

  //m_upgradeDetectorV2.blockPushed();
  //m_upgradeDetectorV3.blockPushed();
  //m_upgradeDetectorV4.blockPushed();
  //m_upgradeDetectorV5.blockPushed();

  update_next_cumulative_size_limit();

  return true;
}

bool Blockchain::pushBlock(BlockEntry& block, const Crypto::Hash& blockHash) {
  // push to blocks storage
  //m_blocks.push_back(block);

  auto key = BLOCK_PREFIX + DB::to_binary_key(blockHash.data, sizeof(blockHash.data)) + BLOCK_SUFFIX;
  m_db.put(key, toBinaryArray(block), true);

  // push to block index
  //m_blockIndex.push(blockHash); // old
  m_db.put(BLOCK_INDEX_PREFIX + Common::write_varint_sqlite4(block.height), blockHash.as_binary_array(), true);

  // push to timestamp index
  //m_timestampIndex.add(block.bl.timestamp, blockHash); // old
  BinaryArray ba;
  TimestampEntry tse;
  tse.blocks.push_back(std::make_pair(block.height, blockHash));
  toBinaryArray(tse, ba);
  if (!m_db.get(TIMESTAMP_INDEX_PREFIX + Common::write_varint_sqlite4(block.bl.timestamp), ba)) { 
    m_db.put(TIMESTAMP_INDEX_PREFIX + Common::write_varint_sqlite4(block.bl.timestamp), ba, false);
  }
  else {
    if (!fromBinaryArray(tse, ba)) {
      throw std::runtime_error("Blockchain::pushBlock, failed to parse timestamp entry from DB");
    }
    m_db.put(TIMESTAMP_INDEX_PREFIX + Common::write_varint_sqlite4(block.bl.timestamp), ba, false);
  }

  // push to gen. txs index
  //m_generatedTransactionsIndex.add(block.bl); // old
  if (block.height > 0) {
    m_lastGeneratedTxNumber += (block.bl.transactionHashes.size() + 1); // plus miner tx
    m_db.put(GENERATED_TRANSACTIONS_INDEX_PREFIX + Common::write_varint_sqlite4(block.height), Common::write_varint_sqlite4(m_lastGeneratedTxNumber), true);
  }
 
  //assert(m_blockIndex.size() == m_blocks.size());

  // committing helps to keep memory usage low
  // commit every 1k blocks when syncing, on every block when was synced
  if (isInCheckpointZone(getCurrentBlockchainHeight()) || !m_synchronized) {
    if (block.height != 0 && block.height % 1000 == 0) { // no commit on genesis
      db_commit();
      logger(INFO, BRIGHT_MAGENTA) << "Blockchain synchronized to height " << block.height;
    }
  } else {
    logger(DEBUGGING) << "Blockchain::db_commit on single push block started...";
    db_commit();
  }

  m_height.store(block.height + 1, std::memory_order_relaxed); // +1 incl. zero block

  return true;
}

void Blockchain::popBlock() {

  DB::Cursor cur = m_db.rbegin(BLOCK_INDEX_PREFIX);
  if (cur.end()) {
    logger(ERROR, BRIGHT_RED) <<
      "Attempt to pop block from empty blockchain.";
    return;
  }
  auto v = cur.get_value_array();
  Crypto::Hash id;
  std::copy(v.begin(), v.end(), id.data);
  auto key = BLOCK_PREFIX + DB::to_binary_key(id.data, sizeof(id.data)) + BLOCK_SUFFIX;
  BinaryArray ba;
  m_db.get(key, ba);
  BlockEntry e;
  fromBinaryArray(e, ba);

  std::vector<Transaction> transactions(e.transactions.size() - 1);
  for (size_t i = 0; i < e.transactions.size() - 1; ++i) {
    transactions[i] = e.transactions[1 + i].tx;
  }

  saveTransactions(transactions);
  removeLastBlock();

  //m_upgradeDetectorV2.blockPopped();
  //m_upgradeDetectorV3.blockPopped();
  //m_upgradeDetectorV4.blockPopped();
  //m_upgradeDetectorV5.blockPopped();
}

bool Blockchain::pushTransaction(BlockEntry& block, const Crypto::Hash& transactionHash, TransactionIndex transactionIndex) {
  /*auto result = m_transactionMap.insert(std::make_pair(transactionHash, transactionIndex));
  if (!result.second) {
    logger(ERROR, BRIGHT_RED) <<
      "Duplicate transaction was pushed to blockchain.";
    return false;
  }*/

  auto tkey = TRANSACTIONS_INDEX_PREFIX + DB::to_binary_key(transactionHash.data, sizeof(transactionHash.data)); 
  try {
    m_db.put(tkey, toBinaryArray(transactionIndex), true);
  }
  catch (std::runtime_error& e) {
    logger(ERROR, BRIGHT_RED) <<
      "Duplicate transaction was pushed to blockchain: " << e.what();
    return false;
  }

  TransactionEntry& transaction = block.transactions[transactionIndex.transaction];

  if (!checkMultisignatureInputsDiff(transaction.tx)) {
    logger(ERROR, BRIGHT_RED) <<
      "Double spending transaction was pushed to blockchain.";
    //m_transactionMap.erase(transactionHash);
    auto tkey = TRANSACTIONS_INDEX_PREFIX + DB::to_binary_key(transactionHash.data, sizeof(transactionHash.data));
    m_db.del(tkey, true);

    return false;
  }

  for (size_t i = 0; i < transaction.tx.inputs.size(); ++i) {
    if (transaction.tx.inputs[i].type() == typeid(KeyInput)) {
      Crypto::KeyImage ki = ::boost::get<KeyInput>(transaction.tx.inputs[i]).keyImage;
      /*auto result = m_spent_key_images.insert(std::make_pair(::boost::get<KeyInput>(transaction.tx.inputs[i]).keyImage, block.height));
      if (!result.second) {
        logger(ERROR, BRIGHT_RED) <<
          "Double spending transaction was pushed to blockchain.";
        for (size_t j = 0; j < i; ++j) {
          m_spent_key_images.erase(::boost::get<KeyInput>(transaction.tx.inputs[i - 1 - j]).keyImage);
        }
        m_transactionMap.erase(transactionHash);
        auto tkey = TRANSACTIONS_INDEX_PREFIX + DB::to_binary_key(transactionHash.data, sizeof(transactionHash.data));
        m_db.del(tkey, true);

        return false;
      }*/
      // add to spent key images DB index
      try {
        auto kikey = SPENT_KEY_IMAGES_INDEX_PREFIX + DB::to_binary_key(ki.data, sizeof(ki.data));
        m_db.put(kikey, Common::write_varint_sqlite4((uint64_t)block.height), true);
      }
      catch (std::runtime_error& e) {
        logger(ERROR, BRIGHT_RED) <<
          "Double spending transaction was pushed to blockchain:" << e.what();
        for (size_t j = 0; j < i; ++j) {
          Crypto::KeyImage ki = ::boost::get<KeyInput>(transaction.tx.inputs[i - 1 - j]).keyImage;
          auto kikey = SPENT_KEY_IMAGES_INDEX_PREFIX + DB::to_binary_key(ki.data, sizeof(ki.data));
          m_db.del(kikey, true);
        }
        //m_transactionMap.erase(transactionHash);
        auto tkey = TRANSACTIONS_INDEX_PREFIX + DB::to_binary_key(transactionHash.data, sizeof(transactionHash.data));
        m_db.del(tkey, true);

        return false;
      }
    }
  }

  for (const auto& inv : transaction.tx.inputs) {
    if (inv.type() == typeid(MultisignatureInput)) {
      const MultisignatureInput& in = ::boost::get<MultisignatureInput>(inv);
      //auto& amountOutputs = m_multisignatureOutputs[in.amount];
      //amountOutputs[in.outputIndex].isUsed = true;

      BinaryArray ba;
      const auto key = MULTUSIGNATURE_OUTPUTS_INDEX_PREFIX + Common::write_varint_sqlite4(in.amount);
      if (m_db.get(key, ba)) {
        MultisignatureOutputEntry me;
        if (!fromBinaryArray(me, ba)) {
          throw std::runtime_error("Blockchain::pushTransaction, failed to parse multisignature outputs entry from DB");
        }
        me.multisignatureOutputs[in.outputIndex].isUsed = true;
        // just put and update doesn't work, have to delete old first
        //m_db.del(key, false);
        m_db.put(key, toBinaryArray(me), false);
      } else {
        MultisignatureOutputEntry me;
        me.multisignatureOutputs[in.outputIndex].isUsed = true;
        m_db.put(key, toBinaryArray(me), true);
      }
    }
  }

  transaction.m_global_output_indexes.resize(transaction.tx.outputs.size());
  for (uint16_t output = 0; output < transaction.tx.outputs.size(); ++output) {
    if (transaction.tx.outputs[output].target.type() == typeid(KeyOutput)) {
      //auto& amountOutputs = m_outputs[transaction.tx.outputs[output].amount]; // m_outputs
      //transaction.m_global_output_indexes[output] = static_cast<uint32_t>(amountOutputs.size()); // get size before the update
      //amountOutputs.push_back(std::make_pair<>(transactionIndex, output)); // push to m_outputs

      BinaryArray ba;
      const auto key = OUTPUTS_INDEX_PREFIX + Common::write_varint_sqlite4(transaction.tx.outputs[output].amount);
      if (m_db.get(key, ba)) {
        OutputsEntry oe;
        if (!fromBinaryArray(oe, ba)) {
          throw std::runtime_error("Blockchain::pushTransaction, failed to parse output entry from DB");
        }
        transaction.m_global_output_indexes[output] = oe.outputs.size(); // get size before the update
        oe.outputs.push_back(std::make_pair<>(transactionIndex, output));
        // just put and update doesn't work, have to delete old first
        //m_db.del(key, false);
        m_db.put(key, toBinaryArray(oe), false);
      } else {
        OutputsEntry oe;
        transaction.m_global_output_indexes[output] = oe.outputs.size(); // get size before the update
        oe.outputs.push_back(std::make_pair<>(transactionIndex, output));
        m_db.put(key, toBinaryArray(oe), true);
      }
    } else if (transaction.tx.outputs[output].target.type() == typeid(MultisignatureOutput)) {
      //auto& amountOutputs = m_multisignatureOutputs[transaction.tx.outputs[output].amount];
      //transaction.m_global_output_indexes[output] = static_cast<uint32_t>(amountOutputs.size());
      //MultisignatureOutputUsage outputUsage = { transactionIndex, output, false };
      //amountOutputs.push_back(outputUsage);

      BinaryArray ba;
      const auto key = MULTUSIGNATURE_OUTPUTS_INDEX_PREFIX + Common::write_varint_sqlite4(transaction.tx.outputs[output].amount);
      if (m_db.get(key, ba)) {
        MultisignatureOutputEntry me;
        if (!fromBinaryArray(me, ba)) {
          throw std::runtime_error("Blockchain::pushTransaction, failed to parse multisignature outputs entry from DB");
        }
        MultisignatureOutputUsage outputUsage = { transactionIndex, output, false };
        me.multisignatureOutputs.push_back(outputUsage);

        // just put and update doesn't work, have to delete old first
        //m_db.del(key, false);
        m_db.put(key, toBinaryArray(me), false);
      } else {
        MultisignatureOutputEntry me;
        MultisignatureOutputUsage outputUsage = { transactionIndex, output, false };
        me.multisignatureOutputs.push_back(outputUsage);
        m_db.put(key, toBinaryArray(me), true);
      }
    }
  }

  //m_paymentIdIndex.add(transaction.tx);
  BinaryArray ba;
  Crypto::Hash paymentId;
  if (BlockchainExplorerDataBuilder::getPaymentId(transaction.tx, paymentId)) {
    if (!m_db.get(PAYMENT_ID_INDEX_PREFIX + DB::to_binary_key(paymentId.data, sizeof(paymentId.data)), ba)) {
      PaymentIdEntry pe;
      pe.transactionHashes.push_back(transactionHash);
      toBinaryArray(pe, ba);
      try {
        m_db.put(PAYMENT_ID_INDEX_PREFIX + DB::to_binary_key(paymentId.data, sizeof(paymentId.data)), toBinaryArray(pe), true);
      }
      catch (std::runtime_error& e) {
        logger(ERROR, BRIGHT_RED) << e.what();
        return false;
      }
    }
    else {
      PaymentIdEntry pe;
      if (!fromBinaryArray(pe, ba)) {
        throw std::runtime_error("Blockchain::pushTransaction, failed to parse paymentId entry from DB");
      }
      pe.transactionHashes.push_back(transactionHash);
      try {
        //m_db.del(PAYMENT_ID_INDEX_PREFIX + DB::to_binary_key(paymentId.data, sizeof(paymentId.data)), false);
        m_db.put(PAYMENT_ID_INDEX_PREFIX + DB::to_binary_key(paymentId.data, sizeof(paymentId.data)), toBinaryArray(pe), false);
      }
      catch (std::runtime_error& e) {
        logger(ERROR, BRIGHT_RED) << e.what();
        return false;
      }
    }
  }

  return true;
}

void Blockchain::popTransaction(const Transaction& transaction, const Crypto::Hash& transactionHash) {
  TransactionIndex transactionIndex;
  if (!getTransactionIndex(transactionHash, transactionIndex)) {
    logger(ERROR, BRIGHT_RED) <<
      "Blockchain consistency broken - cannot find transactionIndex in DB";
    return;
  }

  for (size_t outputIndex = 0; outputIndex < transaction.outputs.size(); ++outputIndex) {
    const TransactionOutput& output = transaction.outputs[transaction.outputs.size() - 1 - outputIndex];
    if (output.target.type() == typeid(KeyOutput)) {
      /*auto amountOutputs = m_outputs.find(output.amount);
      if (amountOutputs == m_outputs.end()) {
        logger(ERROR, BRIGHT_RED) <<
          "Blockchain consistency broken - cannot find specific amount in outputs map.";
        continue;
      }*/

      BinaryArray ba;
      const auto key = OUTPUTS_INDEX_PREFIX + Common::write_varint_sqlite4(output.amount);
      if (!m_db.get(key, ba)) {
        logger(ERROR, BRIGHT_RED) <<
          "Blockchain consistency broken - cannot find specific amount in outputs DB index";
        continue;
      }
      OutputsEntry oe;
      if (!fromBinaryArray(oe, ba)) {
        throw std::runtime_error("Blockchain::popTransaction, failed to parse output entry from DB");
      }

      //if (amountOutputs->second.empty()) {
      if (oe.outputs.empty()) {
        logger(ERROR, BRIGHT_RED) <<
          "Blockchain consistency broken - output array for specific amount is empty.";
        continue;
      }

      //if (amountOutputs->second.back().first.block != transactionIndex.block || amountOutputs->second.back().first.transaction != transactionIndex.transaction) {
      if (oe.outputs.back().first.block != transactionIndex.block || oe.outputs.back().first.transaction != transactionIndex.transaction) {
        logger(ERROR, BRIGHT_RED) <<
          "Blockchain consistency broken - invalid transaction index.";
        continue;
      }

      //if (amountOutputs->second.back().second != transaction.outputs.size() - 1 - outputIndex) {
      if (oe.outputs.back().second != transaction.outputs.size() - 1 - outputIndex) {
        logger(ERROR, BRIGHT_RED) <<
          "Blockchain consistency broken - invalid output index.";
        continue;
      }

      //amountOutputs->second.pop_back();
      oe.outputs.pop_back();
      //if (amountOutputs->second.empty()) {
      if (oe.outputs.empty()) {
        //m_outputs.erase(amountOutputs);
        m_db.del(key, true);
      }
    } else if (output.target.type() == typeid(MultisignatureOutput)) {
      /*auto amountOutputs = m_multisignatureOutputs.find(output.amount);
      if (amountOutputs == m_multisignatureOutputs.end()) {
        logger(ERROR, BRIGHT_RED) <<
          "Blockchain consistency broken - cannot find specific amount in outputs map.";
        continue;
      }*/

      BinaryArray ba;
      const auto key = MULTUSIGNATURE_OUTPUTS_INDEX_PREFIX + Common::write_varint_sqlite4(output.amount);
      if (!m_db.get(key, ba)) {
        logger(ERROR, BRIGHT_RED) <<
          "Blockchain consistency broken - cannot find specific amount in multisignature outputs DB index";
        continue;
      }
      MultisignatureOutputEntry me;
      if (!fromBinaryArray(me, ba)) {
        throw std::runtime_error("Blockchain::popTransaction, failed to parse multisignature output entry from DB");
      }

      //if (amountOutputs->second.empty()) {
      if (me.multisignatureOutputs.empty()) {
        logger(ERROR, BRIGHT_RED) <<
          "Blockchain consistency broken - multisignature output array for specific amount is empty.";
        continue;
      }

      //if (amountOutputs->second.back().isUsed) {
      if (me.multisignatureOutputs.back().isUsed) {
        logger(ERROR, BRIGHT_RED) <<
          "Blockchain consistency broken - attempting to remove used output.";
        continue;
      }

      //if (amountOutputs->second.back().transactionIndex.block != transactionIndex.block || amountOutputs->second.back().transactionIndex.transaction != transactionIndex.transaction) {
      if (me.multisignatureOutputs.back().transactionIndex.block != transactionIndex.block || me.multisignatureOutputs.back().transactionIndex.transaction != transactionIndex.transaction) {
        logger(ERROR, BRIGHT_RED) <<
          "Blockchain consistency broken - invalid transaction index.";
        continue;
      }

      //if (amountOutputs->second.back().outputIndex != transaction.outputs.size() - 1 - outputIndex) {
      if (me.multisignatureOutputs.back().outputIndex != transaction.outputs.size() - 1 - outputIndex) {
        logger(ERROR, BRIGHT_RED) <<
          "Blockchain consistency broken - invalid output index.";
        continue;
      }

      //amountOutputs->second.pop_back();
      me.multisignatureOutputs.pop_back();
      //if (amountOutputs->second.empty()) {
      if (me.multisignatureOutputs.empty()) {
        //m_multisignatureOutputs.erase(amountOutputs);
        m_db.del(key, true);
      }
    }
  }

  for (auto& input : transaction.inputs) {
    if (input.type() == typeid(KeyInput)) {
      /*size_t count = m_spent_key_images.erase(::boost::get<KeyInput>(input).keyImage);
      if (count != 1) {
        logger(ERROR, BRIGHT_RED) <<
          "Blockchain consistency broken - cannot find spent key.";
      }*/
      Platform::DB::Value v;
      auto ki = ::boost::get<KeyInput>(input).keyImage;
      auto kikey = SPENT_KEY_IMAGES_INDEX_PREFIX + DB::to_binary_key(ki.data, sizeof(ki.data));
      if (!m_db.get(kikey, v)) {
        logger(ERROR, BRIGHT_RED) <<
          "Blockchain consistency broken - cannot find spent key.";
      }
      m_db.del(kikey, true);
    } else if (input.type() == typeid(MultisignatureInput)) {
      const MultisignatureInput& in = ::boost::get<MultisignatureInput>(input);
      //auto& amountOutputs = m_multisignatureOutputs[in.amount];
      //if (!amountOutputs[in.outputIndex].isUsed) {

      BinaryArray ba;
      const auto key = MULTUSIGNATURE_OUTPUTS_INDEX_PREFIX + Common::write_varint_sqlite4(in.amount);
      if (!m_db.get(key, ba)) {
        logger(ERROR, BRIGHT_RED) <<
          "Blockchain consistency broken - cannot find specific amount in multisignature outputs DB index";
        continue;
      }
      MultisignatureOutputEntry me;
      if (!fromBinaryArray(me, ba)) {
        throw std::runtime_error("Blockchain::popTransaction, failed to parse multisignature output entry from DB");
      }
      if (!me.multisignatureOutputs[in.outputIndex].isUsed) {
        logger(ERROR, BRIGHT_RED) <<
          "Blockchain consistency broken - multisignature output not marked as used.";
      }

      //amountOutputs[in.outputIndex].isUsed = false;
      me.multisignatureOutputs[in.outputIndex].isUsed = false;
    }
  }

  //m_paymentIdIndex.remove(transaction);
  Crypto::Hash paymentId;
  if (BlockchainExplorerDataBuilder::getPaymentId(transaction, paymentId)) {
    m_db.del(PAYMENT_ID_INDEX_PREFIX + DB::to_binary_key(paymentId.data, sizeof(paymentId.data)), false);
  }

  /*size_t count = m_transactionMap.erase(transactionHash);
  if (count != 1) {
    logger(ERROR, BRIGHT_RED) <<
      "Blockchain consistency broken - cannot find transaction by hash.";
  }*/

  auto tkey = TRANSACTIONS_INDEX_PREFIX + DB::to_binary_key(transactionHash.data, sizeof(transactionHash.data));
  try {
    m_db.del(tkey, true);
  } catch (std::runtime_error& e) {
    logger(ERROR, BRIGHT_RED) <<
      "Blockchain consistency broken - couldn't delete transaction from DB: " << e.what();
  }
}

void Blockchain::popTransactions(const BlockEntry& block, const Crypto::Hash& minerTransactionHash) {
  for (size_t i = 0; i < block.transactions.size() - 1; ++i) {
    popTransaction(block.transactions[block.transactions.size() - 1 - i].tx, block.bl.transactionHashes[block.transactions.size() - 2 - i]);
  }

  popTransaction(block.bl.baseTransaction, minerTransactionHash);
}

bool Blockchain::validateInput(const MultisignatureInput& input, const Crypto::Hash& transactionHash, const Crypto::Hash& transactionPrefixHash, const std::vector<Crypto::Signature>& transactionSignatures) {
  assert(input.signatureCount == transactionSignatures.size());
  //MultisignatureOutputsContainer::const_iterator amountOutputs = m_multisignatureOutputs.find(input.amount);
  //if (amountOutputs == m_multisignatureOutputs.end()) {
  BinaryArray ba;
  const auto key = MULTUSIGNATURE_OUTPUTS_INDEX_PREFIX + Common::write_varint_sqlite4(input.amount);
  if (!m_db.get(key, ba)) {
    logger(DEBUGGING) <<
      "Transaction << " << transactionHash << " contains multisignature input with invalid amount.";
    return false;
  }
  MultisignatureOutputEntry me;
  if (!fromBinaryArray(me, ba)) {
    throw std::runtime_error("Blockchain::validateInput, failed to parse multisignature output entry from DB");
  }

  //if (input.outputIndex >= amountOutputs->second.size()) {
  if (input.outputIndex >= me.multisignatureOutputs.size()) {
    logger(DEBUGGING) <<
      "Transaction << " << transactionHash << " contains multisignature input with invalid outputIndex.";
    return false;
  }

  //const MultisignatureOutputUsage& outputIndex = amountOutputs->second[input.outputIndex];
  const MultisignatureOutputUsage& outputIndex = me.multisignatureOutputs[input.outputIndex];
  if (outputIndex.isUsed) {
    logger(DEBUGGING) <<
      "Transaction << " << transactionHash << " contains double spending multisignature input.";
    return false;
  }

  BlockEntry e;
  if (!getBlockEntryByHeight(outputIndex.transactionIndex.block, e)) {
    logger(DEBUGGING) <<
      "Can't get block " << outputIndex.transactionIndex.block;
    return false;
  }
  const Transaction& outputTransaction = e.transactions[outputIndex.transactionIndex.transaction].tx;
  if (!is_tx_spendtime_unlocked(outputTransaction.unlockTime)) {
    logger(DEBUGGING) <<
      "Transaction << " << transactionHash << " contains multisignature input which points to a locked transaction.";
    return false;
  }

  assert(outputTransaction.outputs[outputIndex.outputIndex].amount == input.amount);
  assert(outputTransaction.outputs[outputIndex.outputIndex].target.type() == typeid(MultisignatureOutput));
  const MultisignatureOutput& output = ::boost::get<MultisignatureOutput>(outputTransaction.outputs[outputIndex.outputIndex].target);
  if (input.signatureCount != output.requiredSignatureCount) {
    logger(DEBUGGING) <<
      "Transaction << " << transactionHash << " contains multisignature input with invalid signature count.";
    return false;
  }

  size_t inputSignatureIndex = 0;
  size_t outputKeyIndex = 0;
  while (inputSignatureIndex < input.signatureCount) {
    if (outputKeyIndex == output.keys.size()) {
      logger(DEBUGGING) <<
        "Transaction << " << transactionHash << " contains multisignature input with invalid signatures.";
      return false;
    }

    if (Crypto::check_signature(transactionPrefixHash, output.keys[outputKeyIndex], transactionSignatures[inputSignatureIndex])) {
      ++inputSignatureIndex;
    }

    ++outputKeyIndex;
  }

  return true;
}

bool Blockchain::checkCheckpoints(uint32_t& lastValidCheckpointHeight) {
  std::vector<uint32_t> checkpointHeights = m_checkpoints.getCheckpointHeights();
  for (const auto& checkpointHeight : checkpointHeights) {
    if (m_height.load(std::memory_order_relaxed) <= checkpointHeight) {
      return true;
    }

    if(m_checkpoints.check_block(checkpointHeight, getBlockIdByHeight(checkpointHeight))) {
      lastValidCheckpointHeight = checkpointHeight;
    } else {
      return false;
    }
  }

  return true;
}

void Blockchain::rollbackBlockchainTo(uint32_t height) {
  while (height + 1 < m_height.load(std::memory_order_relaxed)) {
    removeLastBlock();
  }
}

void Blockchain::removeLastBlock() {

  DB::Cursor cur = m_db.rbegin(BLOCK_INDEX_PREFIX);
  if (cur.end()) {
    logger(ERROR, BRIGHT_RED) <<
      "Attempt to pop block from empty blockchain.";
    return;
  }
  uint32_t height = Common::integer_cast<uint32_t>(Common::read_varint_sqlite4(cur.get_suffix())) + 1;
  auto v = cur.get_value_array();
  Crypto::Hash blockHash;
  std::copy(v.begin(), v.end(), blockHash.data);
  auto key = BLOCK_PREFIX + DB::to_binary_key(blockHash.data, sizeof(blockHash.data)) + BLOCK_SUFFIX;
  BinaryArray ba;
  m_db.get(key, ba);
  BlockEntry e;
  fromBinaryArray(e, ba);

  logger(DEBUGGING) << "Removing last block with height " << e.height;
  popTransactions(e, getObjectHash(e.bl.baseTransaction));

  //m_timestampIndex.remove(m_blocks.back().bl.timestamp, blockHash);
  m_db.del(TIMESTAMP_INDEX_PREFIX + Common::write_varint_sqlite4(e.bl.timestamp), false);

  //m_generatedTransactionsIndex.remove(m_blocks.back().bl);
  m_lastGeneratedTxNumber -= (e.bl.transactionHashes.size() + 1);
  m_db.del(GENERATED_TRANSACTIONS_INDEX_PREFIX + Common::write_varint_sqlite4(height), false);

  //m_blocks.pop_back();
  //m_blockIndex.pop();

  //assert(m_blockIndex.size() == m_blocks.size());

  m_db.del(key, true);
  m_db.del(BLOCK_INDEX_PREFIX + Common::write_varint_sqlite4(height), true);

  uint32_t temp_height = m_height.load(std::memory_order_relaxed);
  m_height.store(temp_height--, std::memory_order_relaxed);
}

/*bool Blockchain::checkUpgradeHeight(const UpgradeDetector& upgradeDetector) {
  uint32_t upgradeHeight = upgradeDetector.upgradeHeight();
  if (upgradeHeight != UpgradeDetectorBase::UNDEF_HEIGHT && upgradeHeight + 1 < m_blocks.size()) {
    logger(INFO) << "Checking block version at " << upgradeHeight + 1;
    if (m_blocks[upgradeHeight + 1].bl.majorVersion != upgradeDetector.targetVersion()) {
      return false;
    }
  }

  return true;
}*/

bool Blockchain::getLowerBound(uint64_t timestamp, uint64_t startOffset, uint32_t& height) {
  std::lock_guard<decltype(m_blockchain_lock)> lk(m_blockchain_lock);
 
  //assert(startOffset < m_blocks.size());
  assert(startOffset < m_height.load(std::memory_order_relaxed));
  
  /*
  auto bound = std::lower_bound(m_blocks.begin() + startOffset, m_blocks.end(), timestamp - m_currency.blockFutureTimeLimit(),
    [](const BlockEntry& b, uint64_t timestamp) { return b.bl.timestamp < timestamp; });

  if (bound == m_blocks.end()) {
    return false;
  }

  height = static_cast<uint32_t>(std::distance(m_blocks.begin(), bound));
  */
  /*
  auto middle = Common::write_varint_sqlite4(timestamp);
  DB::Cursor cur = m_db.begin(TIMESTAMP_INDEX_PREFIX, middle);
  if (cur.end())
    height = m_height.load(std::memory_order_relaxed);
  auto v = cur.get_value_array();
  TimestampEntry t;
  if (!fromBinaryArray(t, v)) {
    throw std::runtime_error("Blockchain::getLowerBound, failed to parse entry from DB");
  }
  height = t.blocks[0].first - 1;
  */

  auto middle = Common::write_varint_sqlite4(startOffset);
  for (DB::Cursor cur = m_db.begin(BLOCK_INDEX_PREFIX, middle); !cur.end(); cur.next()) {
    auto v = cur.get_value_array();
    Crypto::Hash id;
    std::copy(v.begin(), v.end(), id.data);
    auto key = BLOCK_PREFIX + DB::to_binary_key(id.data, sizeof(id.data)) + BLOCK_SUFFIX;
    BinaryArray ba;
    m_db.get(key, ba);
    BlockEntry e;
    fromBinaryArray(e, ba);
    if (e.bl.timestamp < timestamp) { // > ?
      height = e.height;
      return true;
    }
  }

  return false;
}

std::vector<Crypto::Hash> Blockchain::getBlockIds(uint32_t startHeight, uint32_t maxCount) {
  std::lock_guard<decltype(m_blockchain_lock)> lk(m_blockchain_lock);
  //return m_blockIndex.getBlockIds(startHeight, maxCount);

  uint32_t count = 0;
  std::vector<Crypto::Hash> ids;
  auto middle = Common::write_varint_sqlite4(startHeight);
  for (DB::Cursor cur = m_db.begin(BLOCK_INDEX_PREFIX, middle); !cur.end(); ++count, cur.next()) {
    auto v = cur.get_value_array();
    Crypto::Hash id;
    std::copy(v.begin(), v.end(), id.data);
    ids.push_back(id);
    if (count > maxCount)
      break;
  }
  return ids;
}

bool Blockchain::getTransactionIndex(const Crypto::Hash& txId, TransactionIndex& index) {
  BinaryArray ba;
  if (!m_db.get(TRANSACTIONS_INDEX_PREFIX + DB::to_binary_key(txId.data, sizeof(txId.data)), ba))
    return false;
  if (!fromBinaryArray(index, ba)) {
    throw std::runtime_error("Blockchain::getTransactionIndex, failed to parse entry from DB");
    return false;
  }

  return true;
}

bool Blockchain::getBlockContainingTransaction(const Crypto::Hash& txId, Crypto::Hash& blockId, uint32_t& blockHeight) {
  std::lock_guard<decltype(m_blockchain_lock)> lk(m_blockchain_lock);
  /*auto it = m_transactionMap.find(txId);
  if (it == m_transactionMap.end()) {
    return false;
  } else {
    blockHeight = m_blocks[it->second.block].height;
    blockId = getBlockIdByHeight(blockHeight);
    return true;
  }*/

  TransactionIndex ti;
  if (!getTransactionIndex(txId, ti))
    return false;
  
  std::string s;
  Crypto::Hash blockHash = NULL_HASH;
  if (!m_db.get(BLOCK_INDEX_PREFIX + Common::write_varint_sqlite4(ti.block), s)) {
    throw std::runtime_error("Blockchain::getBlockContainingTransaction, failed to get block index entry from DB");
    return false;
  }
  std::copy(s.begin(), s.end(), blockHash.data);
  
  blockId = blockHash;
  blockHeight = ti.block;
  return true;

  // this sanity check may be skipped
  /*BinaryArray ba;
  auto key = BLOCK_PREFIX + DB::to_binary_key(blockHash.data, sizeof(blockHash.data)) + BLOCK_SUFFIX;
  if (!m_db.get(key, ba))
    return false;
  BlockEntry e;
  if (!fromBinaryArray(e, ba))
      return false;
  
  if (e.bl.transactionHashes[ti.transaction] == txId) {
    blockId = blockHash;
    blockHeight = e.height;

    return true;
  }

  return false;*/
}

bool Blockchain::getAlreadyGeneratedCoins(const Crypto::Hash& hash, uint64_t& generatedCoins) {
  std::lock_guard<decltype(m_blockchain_lock)> lk(m_blockchain_lock);
  /*
  // try to find block in main chain
  uint32_t height = 0;
  if (m_blockIndex.getBlockHeight(hash, height)) {
    generatedCoins = m_blocks[height].already_generated_coins;
    return true;
  }*/

  // try to find block in main chain
  BinaryArray ba;
  auto key = BLOCK_PREFIX + DB::to_binary_key(hash.data, sizeof(hash.data)) + BLOCK_SUFFIX;
  if (m_db.get(key, ba)) {
    BlockEntry e;
    if (!fromBinaryArray(e, ba))
      return false;
    generatedCoins = e.already_generated_coins;
    return true;
  }

  // try to find block in alternative chain
  //TODO DB
  auto blockByHashIterator = m_alternative_chains.find(hash);
  if (blockByHashIterator != m_alternative_chains.end()) {
    generatedCoins = blockByHashIterator->second.already_generated_coins;
    return true;
  }

  logger(DEBUGGING) << "Can't find block with hash " << hash << " to get already generated coins.";
  return false;
}

// TODO optimize this, usually you already have BlockEntry where you requesting from
// no need to query it again from DB
bool Blockchain::getBlockSize(const Crypto::Hash& hash, size_t& size) {
  std::lock_guard<decltype(m_blockchain_lock)> lk(m_blockchain_lock);

  // try to find block in main chain
  uint32_t height = 0;
  //if (m_blockIndex.getBlockHeight(hash, height)) {
  //  size = m_blocks[height].block_cumulative_size;
  //  return true;
  //}
  BinaryArray ba;
  auto key = BLOCK_PREFIX + DB::to_binary_key(hash.data, sizeof(hash.data)) + BLOCK_SUFFIX;
  if (!m_db.get(key, ba))
    return false;
  BlockEntry e;
  if (!fromBinaryArray(e, ba))
    return false;
  size = e.block_cumulative_size;

  return true;

  // try to find block in alternative chain
  auto blockByHashIterator = m_alternative_chains.find(hash);
  if (blockByHashIterator != m_alternative_chains.end()) {
    size = blockByHashIterator->second.block_cumulative_size;
    return true;
  }

  logger(DEBUGGING) << "Can't find block with hash " << hash << " to get block size.";
  return false;
}

bool Blockchain::getMultisigOutputReference(const MultisignatureInput& txInMultisig, std::pair<Crypto::Hash, size_t>& outputReference) {
  std::lock_guard<decltype(m_blockchain_lock)> lk(m_blockchain_lock);
  //MultisignatureOutputsContainer::const_iterator amountIter = m_multisignatureOutputs.find(txInMultisig.amount);
  //if (amountIter == m_multisignatureOutputs.end()) {
  BinaryArray ba;
  const auto key = MULTUSIGNATURE_OUTPUTS_INDEX_PREFIX + Common::write_varint_sqlite4(txInMultisig.amount);
  if (!m_db.get(key, ba)) {
    logger(DEBUGGING) << "Transaction contains multisignature input with invalid amount.";
    return false;
  }
  MultisignatureOutputEntry me;
  if (!fromBinaryArray(me, ba)) {
    throw std::runtime_error("Blockchain::getMultisigOutputReference, failed to parse multisignature output entry from DB");
  }

  //if (amountIter->second.size() <= txInMultisig.outputIndex) {
  if (me.multisignatureOutputs.size() <= txInMultisig.outputIndex) {
    logger(DEBUGGING) << "Transaction contains multisignature input with invalid outputIndex.";
    return false;
  }
  //const MultisignatureOutputUsage& outputIndex = amountIter->second[txInMultisig.outputIndex];
  const MultisignatureOutputUsage& outputIndex = me.multisignatureOutputs[txInMultisig.outputIndex];
  
  //const Transaction& outputTransaction = m_blocks[outputIndex.transactionIndex.block].transactions[outputIndex.transactionIndex.transaction].tx;
  std::string s;
  Crypto::Hash h = NULL_HASH;
  if (!m_db.get(BLOCK_INDEX_PREFIX + Common::write_varint_sqlite4(outputIndex.transactionIndex.block), s)) {
    logger(ERROR) << "Blockchain::getMultisigOutputReference, failed to get block index entry from DB";
    return false;
  }
  std::copy(s.begin(), s.end(), h.data);

  if (!m_db.get(BLOCK_PREFIX + DB::to_binary_key(h.data, sizeof(h.data)) + BLOCK_SUFFIX, ba)) {
    logger(ERROR) << "Blockchain::getMultisigOutputReference, failed to get block entry from DB";
    return false;
  }
  BlockEntry e;
  if (!fromBinaryArray(e, ba)) {
    throw std::runtime_error("Blockchain::getMultisigOutputReference, failed to parse block entry from DB");
  }
  const Transaction& outputTransaction = e.transactions[outputIndex.transactionIndex.transaction].tx;

  outputReference.first = getObjectHash(outputTransaction);
  outputReference.second = outputIndex.outputIndex;
  return true;
}

bool Blockchain::storeBlockchainIndices() {
  std::lock_guard<decltype(m_blockchain_lock)> lk(m_blockchain_lock);

  logger(INFO, BRIGHT_WHITE) << "Saving blockchain indices...";
  BlockchainIndicesSerializer ser(*this, getTailId(), logger.getLogger());

  if (!storeToBinaryFile(ser, appendPath(m_config_folder, m_currency.blockchainIndicesFileName()))) {
    logger(ERROR, BRIGHT_RED) << "Failed to save blockchain indices";
    return false;
  }

  return true;
}

bool Blockchain::loadBlockchainIndices() {
  std::lock_guard<decltype(m_blockchain_lock)> lk(m_blockchain_lock);
/*
  logger(INFO, BRIGHT_WHITE) << "Loading blockchain indices for BlockchainExplorer...";
  BlockchainIndicesSerializer loader(*this, get_block_hash(m_blocks.back().bl), logger.getLogger());

  loadFromBinaryFile(loader, appendPath(m_config_folder, m_currency.blockchainIndicesFileName()));

  if (!loader.loaded()) {
    logger(WARNING, BRIGHT_YELLOW) << "No actual blockchain indices for BlockchainExplorer found, rebuilding...";
    std::chrono::steady_clock::time_point timePoint = std::chrono::steady_clock::now();
*/
    //m_paymentIdIndex.clear();
    //m_timestampIndex.clear();
    //m_generatedTransactionsIndex.clear();

    /*for (uint32_t b = 0; b < m_blocks.size(); ++b) {
      if (b % 1000 == 0) {
        logger(INFO, BRIGHT_WHITE) << "Height " << b << " of " << m_blocks.size();
      }
      const BlockEntry& block = m_blocks[b];
      m_timestampIndex.add(block.bl.timestamp, get_block_hash(block.bl));
      m_generatedTransactionsIndex.add(block.bl);
      for (uint16_t t = 0; t < block.transactions.size(); ++t) {
        const TransactionEntry& transaction = block.transactions[t];
        m_paymentIdIndex.add(transaction.tx);
      }
    }*/

    // Store these indexes in db by default
/*
    std::chrono::duration<double> duration = std::chrono::steady_clock::now() - timePoint;
    logger(INFO, BRIGHT_WHITE) << "Rebuilding blockchain indices took: " << duration.count();
  }*/
  return true;
}

bool Blockchain::getGeneratedTransactionsNumber(uint32_t height, uint64_t& generatedTransactions) {
  std::lock_guard<decltype(m_blockchain_lock)> lk(m_blockchain_lock);
  //return m_generatedTransactionsIndex.find(height, generatedTransactions);

  if (height > m_height.load(std::memory_order_relaxed) - 1) {
    return false;
  }

  std::string s;
  if (!m_db.get(GENERATED_TRANSACTIONS_INDEX_PREFIX + Common::write_varint_sqlite4(height), s)) {
    return false;
  }
  generatedTransactions = Common::integer_cast<uint64_t>(Common::read_varint_sqlite4(s));
  
  return true;
}

bool Blockchain::getOrphanBlockIdsByHeight(uint32_t height, std::vector<Crypto::Hash>& blockHashes) {
  std::lock_guard<decltype(m_blockchain_lock)> lk(m_blockchain_lock);
  return m_orphanBlocksIndex.find(height, blockHashes);
  // TODO DB
}

bool Blockchain::getBlockIdsByTimestamp(uint64_t timestampBegin, uint64_t timestampEnd, uint32_t blocksNumberLimit, std::vector<Crypto::Hash>& hashes, uint32_t& blocksNumberWithinTimestamps) {
  std::lock_guard<decltype(m_blockchain_lock)> lk(m_blockchain_lock);
  //return m_timestampIndex.find(timestampBegin, timestampEnd, blocksNumberLimit, hashes, blocksNumberWithinTimestamps);

  if (timestampBegin > timestampEnd) {
    return false;
  }

  uint32_t lim = 0, nr = 0;
  auto middle = Common::write_varint_sqlite4(timestampBegin);
  for (DB::Cursor cur = m_db.begin(TIMESTAMP_INDEX_PREFIX, middle); !cur.end(); cur.next()) {
    auto v = cur.get_value_array();
    TimestampEntry t;
    if (!fromBinaryArray(t, v)) {
      throw std::runtime_error("Blockchain::getBlockIdsByTimestamp, failed to parse entry from DB");
    }
    if (lim < blocksNumberLimit) {
      for (const auto& i : t.blocks) {
        hashes.push_back(i.second);
      }
    }
    lim += (uint32_t)t.blocks.size();
    nr += (uint32_t)t.blocks.size();
    if (Common::integer_cast<uint64_t>(Common::read_varint_sqlite4(cur.get_suffix())) >= timestampEnd) {
      break;
    }
  }
  blocksNumberWithinTimestamps = nr;

  return true;
}

bool Blockchain::getTransactionIdsByPaymentId(const Crypto::Hash& paymentId, std::vector<Crypto::Hash>& transactionHashes) {
  std::lock_guard<decltype(m_blockchain_lock)> lk(m_blockchain_lock);
  //return m_paymentIdIndex.find(paymentId, transactionHashes);
  // TODO DB
  BinaryArray ba;
  if (!m_db.get(PAYMENT_ID_INDEX_PREFIX + DB::to_binary_key(paymentId.data, sizeof(paymentId.data)), ba)) {
    return false;
  }

  PaymentIdEntry pe;
  if (!fromBinaryArray(pe, ba)) {
    throw std::runtime_error("Blockchain::getTransactionIdsByPaymentId, failed to parse paymentId entry from DB");
  }
  transactionHashes = pe.transactionHashes;

  return true;
}

bool Blockchain::loadTransactions(const Block& block, std::vector<Transaction>& transactions) {
  transactions.resize(block.transactionHashes.size());
  size_t transactionSize;
  uint64_t fee;
  for (size_t i = 0; i < block.transactionHashes.size(); ++i) {
    if (!m_tx_pool.take_tx(block.transactionHashes[i], transactions[i], transactionSize, fee)) {
      tx_verification_context context;
      for (size_t j = 0; j < i; ++j) {
        if (!m_tx_pool.add_tx(transactions[i - 1 - j], context, true)) {
          throw std::runtime_error("Blockchain::loadTransactions, failed to add transaction to pool");
        }
      }

      return false;
    }
  }

  return true;
}

void Blockchain::saveTransactions(const std::vector<Transaction>& transactions) {
  tx_verification_context context;
  for (size_t i = 0; i < transactions.size(); ++i) {
    if (!m_tx_pool.add_tx(transactions[transactions.size() - 1 - i], context, true)) {
      logger(WARNING, BRIGHT_YELLOW) << "Blockchain::saveTransactions, failed to add transaction to pool";
    }
  }
}

bool Blockchain::addMessageQueue(MessageQueue<BlockchainMessage>& messageQueue) {
  return m_messageQueueList.insert(messageQueue);
}

bool Blockchain::removeMessageQueue(MessageQueue<BlockchainMessage>& messageQueue) {
  return m_messageQueueList.remove(messageQueue);
}

void Blockchain::sendMessage(const BlockchainMessage& message) {
  for (IntrusiveLinkedList<MessageQueue<BlockchainMessage>>::iterator iter = m_messageQueueList.begin(); iter != m_messageQueueList.end(); ++iter) {
    iter->push(message);
  }
}

bool Blockchain::isBlockInMainChain(const Crypto::Hash& blockId) {
  return haveBlock(blockId);
}

bool Blockchain::isInCheckpointZone(const uint32_t height) {
  return m_checkpoints.is_in_checkpoint_zone(height);
}


template<class visitor_t>
bool Blockchain::scanOutputKeysForIndexes(const KeyInput& tx_in_to_key, visitor_t& vis, uint32_t* pmax_related_block_height)
{
  std::lock_guard<std::recursive_mutex> lk(m_blockchain_lock);
  //auto it = m_outputs.find(tx_in_to_key.amount);
  //if (it == m_outputs.end() || !tx_in_to_key.outputIndexes.size())
  //  return false;

  BinaryArray ba;
  const auto key = OUTPUTS_INDEX_PREFIX + Common::write_varint_sqlite4(tx_in_to_key.amount);
  if (!m_db.get(key, ba)) {
    logger(Logging::INFO) << "Couldn't get from DB output entry for amount " << tx_in_to_key.amount;
    return false;
  }
  OutputsEntry oe;
  if (!fromBinaryArray(oe, ba)) {
    throw std::runtime_error("Blockchain::scanOutputKeysForIndexes, failed to parse output entry from DB");
  }

  std::vector<uint32_t> absolute_offsets = relative_output_offsets_to_absolute(tx_in_to_key.outputIndexes);
  //std::vector<std::pair<TransactionIndex, uint16_t>>& amount_outs_vec = it->second;
  std::vector<std::pair<TransactionIndex, uint16_t>>& amount_outs_vec = oe.outputs;
  size_t count = 0;
  for (uint64_t i : absolute_offsets) {
    if (i >= amount_outs_vec.size()) {
      logger(Logging::INFO) << "Wrong index in transaction inputs: " << i << ", expected maximum " << amount_outs_vec.size() - 1;
      return false;
    }

    //auto tx_it = m_transactionMap.find(amount_outs_vec[i].first);
    //if (!(tx_it != m_transactionMap.end())) { logger(ERROR, BRIGHT_RED) << "Wrong transaction id in output indexes: " << Common::podToHex(amount_outs_vec[i].first); return false; }

    const TransactionEntry te = transactionByIndex(amount_outs_vec[i].first);

    if (!(amount_outs_vec[i].second < te.tx.outputs.size())) {
      logger(Logging::ERROR, Logging::BRIGHT_RED)
        << "Wrong index in transaction outputs: "
        << amount_outs_vec[i].second << ", expected less then "
        << te.tx.outputs.size();
      return false;
    }

    if (!vis.handle_output(te.tx, te.tx.outputs[amount_outs_vec[i].second], amount_outs_vec[i].second)) {
      logger(Logging::INFO) << "Failed to handle_output for output no = " << count << ", with absolute offset " << i;
      return false;
    }

    if (count++ == absolute_offsets.size() - 1 && pmax_related_block_height) {
      if (*pmax_related_block_height < amount_outs_vec[i].first.block) {
        *pmax_related_block_height = amount_outs_vec[i].first.block;
      }
    }
  }

  return true;
}

bool Blockchain::scanOutputkeysForIndices(const KeyInput& txInToKey, std::list<std::pair<Crypto::Hash, size_t>>& outputReferences) {
  struct outputs_visitor
  {
    std::list<std::pair<Crypto::Hash, size_t>>& m_resultsCollector;
    outputs_visitor(std::list<std::pair<Crypto::Hash, size_t>>& resultsCollector) :m_resultsCollector(resultsCollector) {}
    bool handle_output(const Transaction& tx, const TransactionOutput& out, size_t transactionOutputIndex)
    {
      m_resultsCollector.push_back(std::make_pair(getObjectHash(tx), transactionOutputIndex));
      return true;
    }
  };

  outputs_visitor vi(outputReferences);

  return scanOutputKeysForIndexes(txInToKey, vi);
}

}
