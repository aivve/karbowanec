// Copyright (c) 2012-2016, The CryptoNote developers, The Bytecoin developers
// Copyright (c) 2014-2018, The Monero Project
// Copyright (c) 2016, The Forknote developers
// Copyright (c) 2018-2023 Conceal Network & Conceal Devs
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

#include "BuiltinExplorer.h"

#include "RpcServer.h"
#include "version.h"

#include <ctime>
#include <list>
#include <string>
#include <vector>

#include <crypto/crypto.h>
#include <crypto/random.h>
#include "BlockchainExplorerData.h"
#include "Common/StringTools.h"
#include "CryptoNoteCore/AccountNumber.h"
#include "CryptoNoteCore/Core.h"
#include "CryptoNoteCore/CryptoNoteBasicImpl.h"
#include "CryptoNoteCore/CryptoNoteTools.h"
#include "CryptoNoteCore/CryptoNoteFormatUtils.h"
#include "CryptoNoteCore/TransactionExtra.h"
#include "CryptoNoteCore/TransactionUtils.h"
#include "CryptoNoteProtocol/ICryptoNoteProtocolQuery.h"
#include "P2p/NetNode.h"

#include "CoreRpcServerErrorCodes.h"
#include "JsonRpc.h"

#undef ERROR

namespace CryptoNote {

namespace {

// Common page chrome — opening <html>…<body> with the SVG logo and the
// node version, plus the closing tags. Held here because every explorer
// page renders them; previously lived at the top of RpcServer.cpp.
const std::string index_start =
R"(<!DOCTYPE html><html><head><meta http-equiv='refresh' content='60'/><style>* { font-family: monospace; } .wrap { word-break: break-all; word-wrap: break-word; } table.counter tbody tr td:first-child { text-align: right; }</style></head><body><svg xmlns="http://www.w3.org/2000/svg" xml:space="preserve" version="1.1" style="vertical-align:middle; padding-right: 10px; shape-rendering:geometricPrecision; text-rendering:geometricPrecision; image-rendering:optimizeQuality; fill-rule:evenodd; clip-rule:evenodd" viewBox="0 0 2500000 2500000" xmlns:xlink="http://www.w3.org/1999/xlink" width="64px" height="64px">
<g><circle fill="#0AACFC" cx="1250000" cy="1250000" r="1214062" /><path fill="#FFED00" d="M1251219 1162750c18009,-3203 34019,-10006 48025,-20412 14009,-10407 27215,-28016 39622,-52029l275750 -538290c10803,-18010 24012,-32419 39218,-43625 15210,-10806 33219,-16410 53232,-16410l174893 0 -343384 633144c-15209,26016 -32419,47228 -51628,63635 -19613,16409 -41225,28815 -64838,37221 36822,9604 67638,25213 92854,47225 24812,21610 48425,52025 70437,91247l330578 668363 -192503 0c-38822,0 -70041,-21213 -93653,-63235l-270947 -566303c-14006,-25215 -29216,-43225 -45622,-54034 -16409,-10803 -37222,-17206 -62034,-18809l0 287359 -151281 0 0 -288559 -111263 0 0 703581 -213716 0 0 -1540835 213716 0 0 673166 111263 0 0 -332981 151281 0 0 330581z"/></g></svg>
Karbo node v. )" PROJECT_VERSION_LONG R"( &bull; )";

const std::string index_finish = " </body></html>";

// Format a per-output amount in the explorer's tables. CT confidential
// outputs use the CT_CONFIDENTIAL_OUTPUT_AMOUNT sentinel in their wire
// amount field; render them as "hidden" so the explorer doesn't show
// the sentinel as if it were the real amount.
std::string formatExplorerAmount(const Currency& currency, uint64_t amount) {
  return amount == parameters::CT_CONFIDENTIAL_OUTPUT_AMOUNT ? "hidden" : currency.formatAmount(amount);
}

// True if any output on this tx is a ConfidentialOutput. Used to suppress
// the public "sum of outputs" line on CT txs (where the public sum does
// not include hidden amounts).
bool hasConfidentialOutput(const TransactionDetails& tx) {
  return std::any_of(tx.outputs.begin(), tx.outputs.end(), [](const transactionOutputDetails2& out) {
    return out.output.target.type() == typeid(ConfidentialOutput);
  });
}

std::string maskedAmountToHex(const std::array<uint8_t, 8>& amount) {
  return Common::toHex(amount.data(), amount.size());
}

// Render a fixed-size array of POD fields (GK proof's I[6] / A[6] /
// B[6] / Q[6] etc.) as a numbered list.
template <typename T, size_t N>
void appendPodArray(std::string& body, const std::string& label, const T (&values)[N]) {
  body += "  <li>" + label + "\n";
  body += "    <ol>\n";
  for (size_t i = 0; i < N; ++i) {
    body += "      <li class=\"wrap\">" + Common::podToHex(values[i]) + "</li>\n";
  }
  body += "    </ol>\n";
  body += "  </li>\n";
}

} // anonymous namespace

BuiltinExplorer::BuiltinExplorer(Core& core,
                                 NodeServer& p2p,
                                 const ICryptoNoteProtocolQuery& protocolQuery,
                                 BlockchainExplorerDataBuilder& explorerBuilder,
                                 RpcServer& parent)
  : m_core(core)
  , m_p2p(p2p)
  , m_protocolQuery(protocolQuery)
  , m_explorerBuilder(explorerBuilder)
  , m_parent(parent)
{}

// ── Status pages ────────────────────────────────────────────────────────

bool BuiltinExplorer::on_get_index(const COMMAND_HTTP::request& /*req*/, COMMAND_HTTP::response& res) {
  const std::time_t uptime = std::time(nullptr) - m_core.getStartTime();
  const std::string uptime_str = std::to_string((unsigned int)floor(uptime / 60.0 / 60.0 / 24.0)) + "d " + std::to_string((unsigned int)floor(fmod((uptime / 60.0 / 60.0), 24.0))) + "h "
    + std::to_string((unsigned int)floor(fmod((uptime / 60.0), 60.0))) + "m " + std::to_string((unsigned int)fmod(uptime, 60.0)) + "s";
  uint32_t top_block_index = m_core.getCurrentBlockchainHeight() - 1;
  uint32_t top_known_block_index = std::max(static_cast<uint32_t>(1), m_protocolQuery.getObservedHeight()) - 1;
  size_t outConn = m_p2p.get_outgoing_connections_count();
  size_t incConn = m_p2p.get_connections_count() - outConn;
  Crypto::Hash last_block_hash = m_core.getBlockIdByHeight(top_block_index);
  size_t white_peerlist_size = m_p2p.getPeerlistManager().get_white_peers_count();
  size_t grey_peerlist_size = m_p2p.getPeerlistManager().get_gray_peers_count();
  size_t alt_blocks_count = m_core.getAlternativeBlocksCount();
  size_t total_tx_count = m_core.getBlockchainTotalTransactions() - top_block_index + 1;
  size_t tx_pool_count = m_core.getPoolTransactionsCount();

  const std::string body = index_start + (m_core.currency().isTestnet() ? "testnet" : "mainnet") +
    "<ul>" +
      "<li>" + "Synchronization status: " + std::to_string(top_block_index) + "/" + std::to_string(top_known_block_index) +
      "<li>" + "Last block hash: " + Common::podToHex(last_block_hash) + "</li>" +
      "<li>" + "Difficulty: " + std::to_string(m_core.getNextBlockDifficulty()) + "</li>" +
      "<li>" + "Alt. blocks: " + std::to_string(alt_blocks_count) + "</li>" +
      "<li>" + "Total transactions in network: " + std::to_string(total_tx_count) + "</li>" +
      "<li>" + "Transactions in pool: " + std::to_string(tx_pool_count) + "</li>" +
      "<li>" + "Connections:" +
        "<ul>" +
          "<li>" + "RPC: " + std::to_string(m_parent.getRpcConnectionsCount()) + "</li>" +
          "<li>" + "OUT: " + std::to_string(outConn) + "</li>" +
          "<li>" + "INC: " + std::to_string(incConn) + "</li>" +
        "</ul>" +
      "</li>" +
      "<li>" + "Peers: " + std::to_string(white_peerlist_size) + " white, " + std::to_string(grey_peerlist_size) + " grey" + "</li>" +
      "<li>" + "Uptime: " + uptime_str + "</li>" +
    "</ul>" +
    index_finish;

  res = body;

  return true;
}

bool BuiltinExplorer::on_get_supply(const COMMAND_HTTP::request& /*req*/, COMMAND_HTTP::response& res) {
  std::string already_generated_coins = m_core.currency().formatAmount(m_core.getTotalGeneratedAmount());
  res = already_generated_coins;

  return true;
}

bool BuiltinExplorer::on_get_payment_id(const COMMAND_HTTP::request& /*req*/, COMMAND_HTTP::response& res) {
  Crypto::Hash result;
  Random::randomBytes(32, result.data);
  res = Common::podToHex(result);

  return true;
}

// ── Block-explorer pages ───────────────────────────────────────────────

bool BuiltinExplorer::on_get_explorer(const COMMAND_EXPLORER::request& req, COMMAND_EXPLORER::response& res) {
  uint32_t top_block_index = m_core.getCurrentBlockchainHeight() - 1;
  std::string body = index_start + (m_core.currency().isTestnet() ? "testnet" : "mainnet") +
    "\n<p>" + "Height: <b>" + std::to_string(top_block_index) + "</b>" +
    " &bull; " + "Difficulty: <b>" + std::to_string(m_core.getNextBlockDifficulty()) + "</b>" +
    " &bull; " + "Alt. blocks: <b>" + std::to_string(m_core.getAlternativeBlocksCount()) + "</b>" +
    " &bull; " + "Transactions: <b>" + std::to_string(m_core.getBlockchainTotalTransactions() - top_block_index + 1) + "</b>" +
    " &bull; " + "Emission: <b>" + m_core.currency().formatAmount(m_core.getTotalGeneratedAmount()) + "</b>" +
    " &bull; " + "Next reward: <b>" + m_core.currency().formatAmount(m_core.currency().calculateReward(m_core.getTotalGeneratedAmount(), m_core.getCurrentBlockchainHeight())) + "</b>" +
    "</p>\n";

  const uint32_t print_blocks_count = 10;
  uint32_t req_height = std::max<uint32_t>(req.height == 0 ? top_block_index : req.height, print_blocks_count);
  uint32_t last_height = req_height - print_blocks_count;
  if (last_height < print_blocks_count)
      last_height = 0;

  // Search
  body += R"(
  <form style='padding: 10px;' name='searchform' action='javascript:handleSearch()'>
    <input type='text' name='search' id='txt_search' size='80' placeholder='Search by block height/hash, tx hash, payment id, account number, address...'>
    <input type='submit' value='Search'>
  </form>
  <script>
  function handleSearch() {
    var search_str = document.getElementById('txt_search').value;
    var xhr = new XMLHttpRequest();
    xhr.open('POST', '/json_rpc', true);
    xhr.setRequestHeader('Content-Type', 'application/json');
    xhr.send(JSON.stringify({
      method: 'search',
      params: {
        query: search_str
      }
    }));
    xhr.onload = function() {
      var data = JSON.parse(this.responseText);
      if (data.result) {
        window.location.href = data.result.result;
      } else if (data.error) {
        alert(data.error.message);
      }
    }
  }
  </script>)";

  // show mempool only on home page
  if (req_height == top_block_index) {
    auto pool = m_core.getMemoryPool();
    if (!pool.empty()) {
      body += "<h2>Transaction pool</h2>";
      body += "<table cellpadding=\"10px\">\n";
      body += "  <thead>\n";
      body += "  <tr>\n";
      body += "    <th>Date</th><th>Hash</th><th>Amount</th><th>Fee</th><th>Size</th>\n";
      body += "  </tr>\n";
      body += "</thead>\n";
      body += "<tbody>\n";
      for (const CryptoNote::tx_memory_pool::TransactionDetails& txd : pool) {
        time_t rawtime = (const time_t)txd.receiveTime;
        struct tm* timeinfo;
        timeinfo = gmtime(&rawtime);
        std::string txHashStr = Common::podToHex(txd.id);

        body += "  <tr>\n";
        body += "    <td>";
        body += asctime(timeinfo);
        body.pop_back(); // remove newline after asctime
        body += "</td>\n    <td>";
        body += "<a class=\"wrap\" href=\"/explorer/tx/" + txHashStr + "\">";
        body += txHashStr;
        body += "</a>";
        body += "</td>\n    <td>";
        body += m_core.currency().formatAmount(getOutputAmount(txd.tx));
        body += "</td>\n    <td>";
        body += m_core.currency().formatAmount(txd.fee);
        body += "</td>\n    <td>";
        body += std::to_string(txd.blobSize);
        body += "</td>\n    <td>";
        body += "  </tr>\n";
      }
      body += "</tbody>\n";
      body += "</table>\n";
    }
  }

  // list last 10 blocks with txs
  body += "<h2>Blocks</h2>";
  body += "<table cellpadding=\"10px\">\n";
  body += "  <thead>\n";
  body += "  <tr>\n";
  body += "    <th>Height</th><th>Date</th><th>Hash</th><th>Size</th><th>Difficulty</th><th>Txs</th>\n";
  body += "  </tr>\n";
  body += "</thead>\n";
  body += "<tbody>\n";

  for (uint32_t i = req_height; i > last_height; i--) {
    Crypto::Hash blockHash = m_core.getBlockIdByHeight(i);
    Block blk;
    if (!m_core.getBlockByHash(blockHash, blk)) {
      throw JsonRpc::JsonRpcError{ CORE_RPC_ERROR_CODE_INTERNAL_ERROR,
        "Internal error: can't get block by height. Height = " + std::to_string(i) + '.' };
    }

    time_t rawtime = (const time_t)blk.timestamp;
    struct tm* timeinfo;
    timeinfo = gmtime(&rawtime);

    difficulty_type blockDifficulty;
    m_core.getBlockDifficulty(static_cast<uint32_t>(i), blockDifficulty);
    size_t tx_cumulative_block_size;
    m_core.getBlockSize(blockHash, tx_cumulative_block_size);
    size_t blokBlobSize = getObjectBinarySize(blk);
    size_t minerTxBlobSize = getObjectBinarySize(blk.baseTransaction);
    uint64_t blockSize = blokBlobSize + tx_cumulative_block_size - minerTxBlobSize;

    body += "  <tr>\n";
    body += "    <td>";
    body += std::to_string(i);
    body += "</td>\n    <td>";
    body += asctime(timeinfo);
    body.pop_back(); // remove newline after asctime
    body += "</td>\n    <td>";
    body += "<a class=\"wrap\" href=\"/explorer/block/" + Common::podToHex(blockHash) + "\">";
    body += Common::podToHex(blockHash);
    body += "</a>";
    body += "</td>\n    <td>";
    body += std::to_string(blockSize);
    body += "</td>\n    <td>";
    body += std::to_string(blockDifficulty);
    body += "</td>\n    <td>";
    body += std::to_string(blk.transactionHashes.size() + 1);
    body += "</td>\n";
    body += "  </tr>\n";

    if (i == 0)
      break;
  }

  body += "</tbody>\n";
  body += "</table>\n";

  uint32_t curr_page = req_height == 0 ? 0 : (top_block_index - req_height) / print_blocks_count;
  uint32_t total_pages = top_block_index / print_blocks_count;
  uint32_t next_page = req_height - print_blocks_count;
  uint32_t prev_page = std::min<uint32_t>(req_height + print_blocks_count, top_block_index);

  body += "<p>";
  if (curr_page != 0) {
    if (prev_page <= top_block_index - print_blocks_count) {
      body += "<a href=\"/explorer/height/";
      body += std::to_string(prev_page);
      body += "\">previous page</a> | ";
    }
    body += "<a href=\"/explorer/\">first page</a> | ";
  }
  body += "current page: ";
  body += std::to_string(curr_page);
  body += " / ";
  body += std::to_string(total_pages);
  if (req_height != 0 && req_height > print_blocks_count) {
    body += " | <a href=\"/explorer/height/";
    body += std::to_string(next_page);
    body += "\">next page</a></p>";
  }

  body += index_finish;

  res = body;

  return true;
}

bool BuiltinExplorer::on_explorer_search(const COMMAND_RPC_EXPLORER_SEARCH::request& req, COMMAND_RPC_EXPLORER_SEARCH::response& res) {
  // Try account number format (H-I-C)
  AccountNumber acctNum;
  if (AccountNumber::fromString(req.query, acctNum)) {
    AccountPublicAddress address;
    if (m_core.resolveAccountNumber(acctNum.blockHeight, acctNum.txIndex, address)) {
      res.result = "/explorer/account/" + req.query;
      res.status = CORE_RPC_STATUS_OK;
      return true;
    }
  }

  // Try as address
  {
    AccountPublicAddress address;
    uint64_t prefix;
    if (parseAccountAddressString(prefix, address, req.query)) {
      res.result = "/explorer/address/" + req.query;
      res.status = CORE_RPC_STATUS_OK;
      return true;
    }
  }

  Crypto::Hash hash;

  if (req.query.size() < 64) {
    // assume it's height
    uint32_t height = static_cast<uint32_t>(std::stoul(req.query));
    hash = m_core.getBlockIdByHeight(height);
  }
  else if (!parse_hash256(req.query, hash)) {
    throw JsonRpc::JsonRpcError{
      CORE_RPC_ERROR_CODE_WRONG_PARAM, "Failed to parse query: " + req.query };
  }

  // check if it's block
  if (m_core.have_block(hash)) {
    res.result = "/explorer/block/" + Common::podToHex(hash);
    res.status = CORE_RPC_STATUS_OK;
    return true;
  }

  // check if it's tx
  if (m_core.haveTransaction(hash)) {
    res.result = "/explorer/tx/" + Common::podToHex(hash);
    res.status = CORE_RPC_STATUS_OK;
    return true;
  }

  // check if it's payment id
  std::vector<Crypto::Hash> txHashes = m_core.getTransactionHashesByPaymentId(hash);
  if (!txHashes.empty()) {
    res.result = "/explorer/payment_id/" + Common::podToHex(hash);
    res.status = CORE_RPC_STATUS_OK;
    return true;
  }

  throw JsonRpc::JsonRpcError{ CORE_RPC_ERROR_CODE_WRONG_PARAM, "Not found" };

  return true;
}

bool BuiltinExplorer::on_get_explorer_block_by_hash(const COMMAND_EXPLORER_GET_BLOCK_DETAILS_BY_HASH::request& req, COMMAND_EXPLORER_GET_BLOCK_DETAILS_BY_HASH::response& res) {
  try {
    Crypto::Hash block_hash;
    if (!parse_hash256(req.hash, block_hash)) {
      throw JsonRpc::JsonRpcError{
        CORE_RPC_ERROR_CODE_WRONG_PARAM,
        "Failed to parse hex representation of block hash. Hex = " + req.hash + '.' };
    }
    Block blk;
    if (!m_core.getBlockByHash(block_hash, blk)) {
      throw JsonRpc::JsonRpcError{
        CORE_RPC_ERROR_CODE_INTERNAL_ERROR,
        "Internal error: can't get block by hash. Hash = " + req.hash + '.' };
    }

    Crypto::Hash blockHash = get_block_hash(blk);
    uint32_t blockIndex = boost::get<BaseInput>(blk.baseTransaction.inputs.front()).blockIndex;

    std::string body = index_start + (m_core.currency().isTestnet() ? "testnet" : "mainnet") + "\n<p>";

    body += "<a href=\"/explorer/\">Home</a>";
    body += "<hr />";

    body += "<h2>Block <span class=\"wrap\">" + Common::podToHex(blockHash) + "</span></h2>\n";

    body += "<ul>\n";
    body += "  <li>\n";
    body += "    Index: " + std::to_string(blockIndex) + "\n";
    body += "  </li>\n";
    body += "  <li>\n";
    time_t rawtime = (const time_t)blk.timestamp;
    struct tm* timeinfo;
    timeinfo = gmtime(&rawtime);
    body += "    Time: " + std::to_string(blk.timestamp) + " &bull; ";
    body += asctime(timeinfo);
    body += "  </li>\n";
    body += "  <li>\n";
    body += "    \tVersion: " + std::to_string(blk.majorVersion) + "." + std::to_string(blk.minorVersion) + "\n";
    body += "  </li>\n";
    body += "  <li>\n";
    Crypto::Hash tmpHash = m_core.getBlockIdByHeight(blockIndex);
    bool isOrphaned = blockHash != tmpHash;
    body += "    \tOrphan: ";
    if (isOrphaned)
      body += "YES\n";
    else
      body += "NO\n";
    body += "  </li>\n";
    body += "  <li>\n";
    size_t tx_cumulative_block_size;
    m_core.getBlockSize(blockHash, tx_cumulative_block_size);
    size_t blokBlobSize = getObjectBinarySize(blk);
    size_t minerTxBlobSize = getObjectBinarySize(blk.baseTransaction);
    size_t blockSize = blokBlobSize + tx_cumulative_block_size - minerTxBlobSize;
    body += "    \tSize: " + std::to_string(blockSize) + "\n";
    body += "  </li>\n";
    body += "  <li>\n";
    uint64_t blockDifficulty = 0;
    if (!m_core.getBlockDifficulty(blockIndex, blockDifficulty)) {
      throw JsonRpc::JsonRpcError{
        CORE_RPC_ERROR_CODE_INTERNAL_ERROR,
        "Internal error: can't calcualate difficulty for block " + req.hash + '.' };
    }
    body += "    Difficulty: " + std::to_string(blockDifficulty) + "\n";
    body += "  </li>\n";
    body += "  <li>\n";
    body += "    Previous block: ";
    body += "<a class=\"wrap\" href=\"/explorer/block/" + Common::podToHex(blk.previousBlockHash) + "\">";
    body += Common::podToHex(blk.previousBlockHash);
    body += "</a>\n";
    body += "  </li>\n";
    if (blk.majorVersion >= BLOCK_MAJOR_VERSION_5) {
      body += "  <li>\n";
      body += "    Miner signature: <span class=\"wrap\">" + Common::podToHex(blk.signature) + "</span>";
      body += "  </li>\n";
    }
    body += "</ul>";

    body += "<h3>Transactions</h3>\n";

    // simple list of tx hashes without details, add coinbase first
    body += "<ol>\n";
    body += "  <li>\n";
    Crypto::Hash coinbaseHash = getObjectHash(blk.baseTransaction);
    std::string txHashStr = Common::podToHex(coinbaseHash);
    body += "    <a class=\"wrap\" href=\"/explorer/tx/" + txHashStr + "\">";
    body += txHashStr;
    body += "</a>";
    body += "  </li>\n";

    for (const auto& t : blk.transactionHashes) {
      body += "  <li>\n";
      body += "    <a class=\"wrap\" href=\"/explorer/tx/" + Common::podToHex(t) + "\">";
      body += Common::podToHex(t);
      body += "    </a>";
      body += "  </li>\n";
    }

    body += "</ol>\n";

    body += index_finish;

    res = body;
  }
  catch (std::system_error& e) {
    throw JsonRpc::JsonRpcError{ CORE_RPC_ERROR_CODE_INTERNAL_ERROR, e.what() };
    return false;
  }
  catch (std::exception& e) {
    throw JsonRpc::JsonRpcError{ CORE_RPC_ERROR_CODE_INTERNAL_ERROR, "Error: " + std::string(e.what()) };
    return false;
  }

  return true;
}

bool BuiltinExplorer::on_get_explorer_tx_by_hash(const COMMAND_EXPLORER_GET_TRANSACTION_DETAILS_BY_HASH::request& req, COMMAND_EXPLORER_GET_TRANSACTION_DETAILS_BY_HASH::response& res) {
  try {
    std::list<Crypto::Hash> missed_txs;
    std::list<Transaction> txs;
    std::vector<Crypto::Hash> hashes;
    Crypto::Hash tx_hash;
    if (!parse_hash256(req.hash, tx_hash)) {
      throw JsonRpc::JsonRpcError{
        CORE_RPC_ERROR_CODE_WRONG_PARAM,
        "Failed to parse hex representation of transaction hash. Hex = " + req.hash + '.' };
    }
    hashes.push_back(tx_hash);
    m_core.getTransactions(hashes, txs, missed_txs, true);

    if (txs.empty() || !missed_txs.empty()) {
      std::string hash_str = Common::podToHex(missed_txs.back());
      throw JsonRpc::JsonRpcError{ CORE_RPC_ERROR_CODE_WRONG_PARAM,
        "transaction wasn't found. Hash = " + hash_str + '.' };
    }

    TransactionDetails transactionsDetails;
    if (!m_explorerBuilder.fillTransactionDetails(txs.back(), transactionsDetails)) {
      throw JsonRpc::JsonRpcError{ CORE_RPC_ERROR_CODE_INTERNAL_ERROR,
        "Internal error: can't fill transaction details." };
    }

    std::string body = index_start + (m_core.currency().isTestnet() ? "testnet" : "mainnet") + "\n<p>";

    body += "<a href=\"/explorer/\">Home</a>";
    body += "<hr />";

    body += "<h2>Transaction <span class=\"wrap\">" + Common::podToHex(transactionsDetails.hash) + "</span></h2>\n";

    body += "<ul>\n";
    if (transactionsDetails.inBlockchain) {
      body += "  <li>\n";
      body += "    In block: ";
      body += "<a class=\"wrap\" href=\"/explorer/block/" + Common::podToHex(transactionsDetails.blockHash) + "\">";
      body += std::to_string(transactionsDetails.blockHeight) + " (" + Common::podToHex(transactionsDetails.blockHash) + ")";
      body += "    </a>\n";
      body += "  </li>\n";
      body += "  <li>\n";
      time_t rawtime = (const time_t)transactionsDetails.timestamp;
      struct tm* timeinfo;
      timeinfo = gmtime(&rawtime);
      body += "    First confirmation time: ";
      body += asctime(timeinfo);
      body += "  </li>\n";
    }
    else {
      body += "  <li>\n";
      body += "    Unconfirmed\n";
      body += "  </li>\n";
    }
    body += "  <li>\n";
    const bool txHasConfidentialOutput = hasConfidentialOutput(transactionsDetails);
    if (txHasConfidentialOutput && transactionsDetails.totalOutputsAmount == 0) {
      body += "    Sum of outputs: hidden (confidential)\n";
    } else if (txHasConfidentialOutput) {
      body += "    Public output amount: " + m_core.currency().formatAmount(transactionsDetails.totalOutputsAmount)
        + " (confidential outputs hidden)\n";
    } else {
      body += "    Sum of outputs: " + m_core.currency().formatAmount(transactionsDetails.totalOutputsAmount) + "\n";
    }
    body += "  </li>\n";
    body += "  <li>\n";
    body += "    Fee: " + m_core.currency().formatAmount(transactionsDetails.fee) + "\n";
    body += "  </li>\n";
    body += "  <li>\n";
    body += "    Size: " + std::to_string(transactionsDetails.size) + "\n";
    body += "  </li>\n";
    body += "  <li>\n";
    body += "    Inputs: " + std::to_string(transactionsDetails.inputs.size()) + "\n";
    body += "  </li>\n";
    body += "  <li>\n";
    body += "    Outputs: " + std::to_string(transactionsDetails.outputs.size()) + "\n";
    body += "  </li>\n";
    body += "  <li>\n";
    body += "    Unlock time: " + std::to_string(transactionsDetails.unlockTime) + "\n";
    body += "  </li>\n";
    body += "  <li>\n";
    body += "    Version: " + std::to_string(transactionsDetails.version) + "\n";
    body += "  </li>\n";
    body += "  <li>\n";
    if (transactionsDetails.minMixin == transactionsDetails.mixin) {
      body += "    Mixin count: " + std::to_string(transactionsDetails.mixin) + "\n";
    } else {
      // Mixed ring sizes (e.g. ring-1 coinbase shielding + ring-N normal CT)
      body += "    Mixin count: "
           + std::to_string(transactionsDetails.minMixin) + ".."
           + std::to_string(transactionsDetails.mixin)
           + " (varies per input - see Inputs table)\n";
    }
    body += "  </li>\n";
    body += "  <li>\n";
    body += "    Public key: <span class=\"wrap\">" + Common::podToHex(transactionsDetails.extra.publicKey) + "</span>\n";
    body += "  </li>\n";
    if (transactionsDetails.hasPaymentId) {
      body += "  <li>\n";
      body += "    Payment ID: <span class=\"wrap\">" + Common::podToHex(transactionsDetails.paymentId) + "</span>\n";
      body += "  </li>\n";
    }
    body += "  <li>\n";
    body += "    Extra size: " + std::to_string(transactionsDetails.extra.size) + "\n";
    body += "  </li>\n";
    if (!transactionsDetails.extra.raw.empty()) {
      body += "  <li>\n";
      body += "    Extra raw: <span class=\"wrap\">" + Common::toHex(transactionsDetails.extra.raw) + "</span>\n";
      body += "  </li>\n";
    }
    // Check for account registration in tx extra
    {
      TransactionExtraAccountRegistration reg;
      if (getAccountRegistrationFromExtra(txs.back().extra, reg)) {
        AccountPublicAddress regAddr;
        regAddr.spendPublicKey = reg.spendPublicKey;
        regAddr.viewPublicKey = reg.viewPublicKey;
        std::string regAddrStr = m_core.currency().accountAddressAsString(regAddr);

        body += "  <li>\n";
        body += "    Account registration: <a class=\"wrap\" href=\"/explorer/address/" + regAddrStr + "\">"
             + regAddrStr + "</a>\n";
        body += "  </li>\n";
        body += "  <li>\n";
        body += "    Spend public key: <span class=\"wrap\">" + Common::podToHex(reg.spendPublicKey) + "</span>\n";
        body += "  </li>\n";
        body += "  <li>\n";
        body += "    View public key: <span class=\"wrap\">" + Common::podToHex(reg.viewPublicKey) + "</span>\n";
        body += "  </li>\n";

        // Show resulting account number if tx is in blockchain
        if (transactionsDetails.inBlockchain) {
          // Find tx index within block
          uint32_t txIdx = 0;
          uint32_t bh = transactionsDetails.blockHeight;
          AccountPublicAddress checkAddr;
          // Walk through possible indices to find this registration
          for (uint32_t i = 0; i < 1000; ++i) {
            if (m_core.resolveAccountNumber(bh, i, checkAddr)) {
              if (checkAddr.spendPublicKey == reg.spendPublicKey &&
                  checkAddr.viewPublicKey == reg.viewPublicKey) {
                txIdx = i;
                AccountNumber an{bh, txIdx};
                body += "  <li>\n";
                body += "    Account number: <a href=\"/explorer/account/" + an.toString() + "\">"
                     + an.toString() + "</a>\n";
                body += "  </li>\n";
                break;
              }
            }
          }
        }
      }
    }
    body += "</ul>\n";

    body += "<h3>Inputs</h3>\n";

    body += "<table class=\"counter\" cellpadding=\"10px\">\n";
    body += "  <thead>\n";
    body += "  <tr>\n";
    body += "    <th>No</th><th>Amount</th><th>Key image</th><th>Pseudo commitment</th><th>Output indexes (references)<br />Key Offset - Output No</th>\n";
    body += "  </tr>\n";
    body += "</thead>\n";
    body += "<tbody>\n";
    for (size_t i = 0; i < transactionsDetails.inputs.size(); ++i) {
      const auto& in = transactionsDetails.inputs[i];
      body += "  <tr>\n";
      body += "    <td>" + std::to_string(i) + ")</td>";
      body += "    <td>";
      if (in.type() == typeid(BaseInputDetails)) {
        BaseInputDetails c = boost::get<BaseInputDetails>(in);
        body += m_core.currency().formatAmount(c.amount);
        body += "</td>\n    <td colspan=\"3\">coinbase</td>\n";
      }
      else if (in.type() == typeid(KeyInputDetails)) {
        KeyInputDetails k = boost::get<KeyInputDetails>(in);
        body += m_core.currency().formatAmount(k.input.amount);
        body += "</td>\n    <td class=\"wrap\">";
        body += Common::podToHex(k.input.keyImage);
        body += "</td>\n    <td>-";
        body += "</td>\n    <td>";
        if (k.outputs.empty()) {
          body += "references unavailable";
        } else {
          for (size_t n = 0; n < k.outputs.size(); ++n) {
            body += "    <a href=\"/explorer/tx/" + Common::podToHex(k.outputs[n].transactionHash) + "\">";
            body += (n < k.input.outputIndexes.size() ? std::to_string(k.input.outputIndexes[n]) : "?"); // key_offset
            body += " - " + std::to_string(k.outputs[n].number) +"</a>"; // tx output reference
            if (n + 1 < k.outputs.size()) body += ", ";
          }
        }
        body += "    </td>\n";
      }
      else if (in.type() == typeid(ConfidentialInputDetails)) {
        ConfidentialInputDetails c = boost::get<ConfidentialInputDetails>(in);
        // Mixed-bucket rings: report unique buckets used by ring members.
        std::set<uint64_t> bucketSet;
        for (const auto& m : c.ringMembers) bucketSet.insert(m.amount);
        if (bucketSet.size() == 1) {
          body += formatExplorerAmount(m_core.currency(), *bucketSet.begin());
        } else {
          body += "mixed (" + std::to_string(bucketSet.size()) + " buckets)";
        }
        body += "</td>\n    <td class=\"wrap\">";
        body += Common::podToHex(c.keyImage);
        body += "</td>\n    <td class=\"wrap\">";
        body += Common::podToHex(c.pseudoCommitment);
        body += "</td>\n    <td>";
        if (c.outputs.empty()) {
          body += "ring of " + std::to_string(c.mixin) + " (references unavailable)";
        } else {
          for (size_t k = 0; k < c.outputs.size(); ++k) {
            body += "    <a href=\"/explorer/tx/" + Common::podToHex(c.outputs[k].transactionHash) + "\">";
            if (k < c.ringMembers.size()) {
              body += std::to_string(c.ringMembers[k].outputIndex) + " ";
            }
            body += " - " + std::to_string(c.outputs[k].number) + "</a>";
            if (k + 1 < c.outputs.size()) body += ", ";
          }
        }
        body += "    </td>\n";
      }
      body += "  </tr>\n";
    }
    body += "</tbody>\n";
    body += "</table>\n";

    body += "<h3>Outputs</h3>\n";

    body += "<table class=\"counter\" cellpadding=\"10px\">\n";
    body += "  <thead>\n";
    body += "  <tr>\n";
    body += "    <th>No</th><th>Type</th><th>Amount</th><th>Public key (stealth address)</th><th>Commitment</th><th>Masked amount</th><th>Global index</th>\n";
    body += "  </tr>\n";
    body += "</thead>\n";
    body += "<tbody>\n";
    for (size_t i = 0; i < transactionsDetails.outputs.size(); ++i) {
      const auto& o = transactionsDetails.outputs[i];
      body += "  <tr>\n";
      body += "    <td>" + std::to_string(i) + ")</td>";
      body += "    <td>";
      if (o.output.target.type() == typeid(ConfidentialOutput)) {
        body += "Confidential";
      } else {
        body += "Key";
      }
      body += "</td>\n    <td>";
      if (o.output.target.type() == typeid(ConfidentialOutput)) {
        body += "hidden";
      } else {
        body += m_core.currency().formatAmount(o.output.amount);
      }
      body += "</td>\n    <td class=\"wrap\">";
      if (o.output.target.type() == typeid(KeyOutput)) {
        KeyOutput ko = boost::get<KeyOutput>(o.output.target);
        body += Common::podToHex(ko.key);
        body += "</td>\n    <td>-";
        body += "</td>\n    <td>-";
      } else if (o.output.target.type() == typeid(ConfidentialOutput)) {
        ConfidentialOutput co = boost::get<ConfidentialOutput>(o.output.target);
        body += Common::podToHex(co.targetKey);
        body += "</td>\n    <td class=\"wrap\">";
        body += Common::podToHex(co.commitment);
        body += "</td>\n    <td class=\"wrap\">";
        body += maskedAmountToHex(co.maskedAmount);
      }
      body += "</td>\n    <td>";
      body += std::to_string(o.globalIndex);
      body += "    </td>\n";
      body += "  </tr>\n";
    }
    body += "</tbody>\n";
    body += "</table>\n";

    // Unified "Input signatures" section. Each input gets one entry whose
    // body is either the legacy ring signature (v1 tx, or v2 KeyInput
    // shielding) or the Triptych spend proof (v2 ConfidentialInput).
    // An empty CTInputSignature slot (all vectors empty) marks a v2
    // KeyInput; we fall back to tx.signatures[i] for that slot.
    const auto& sigs   = transactionsDetails.signatures;
    const auto& ctSigs = transactionsDetails.ctSignatures;
    const size_t inputCount = std::max(sigs.size(), ctSigs.size());
    if (inputCount > 0) {
      body += "<h3>Input signatures</h3>\n";
      body += "<ol>\n";

      auto dumpPointArray =
          [&](const std::vector<Crypto::EllipticCurvePoint>& arr, const char* label) {
        body += "        <li>" + std::string(label) + "\n          <ol>\n";
        for (const auto& p : arr) {
          body += "            <li><span class=\"wrap\">" + Common::podToHex(p) + "</span></li>\n";
        }
        body += "          </ol>\n        </li>\n";
      };
      auto dumpScalarArray =
          [&](const std::vector<Crypto::EllipticCurveScalar>& arr, const char* label) {
        body += "        <li>" + std::string(label) + "\n          <ol>\n";
        for (const auto& s : arr) {
          body += "            <li><span class=\"wrap\">" + Common::podToHex(s) + "</span></li>\n";
        }
        body += "          </ol>\n        </li>\n";
      };

      for (size_t i = 0; i < inputCount; ++i) {
        const bool hasCt = i < ctSigs.size();
        const bool ctSlotEmpty = hasCt
            && ctSigs[i].I_bits.empty() && ctSigs[i].Q_P.empty()
            && ctSigs[i].A.empty()      && ctSigs[i].B.empty();
        const bool useTriptych = hasCt && !ctSlotEmpty;

        body += "  <li>\n";
        body += "    <details open>\n";

        if (useTriptych) {
          const auto& signature = ctSigs[i];
          const size_t n = signature.I_bits.size();
          const size_t ring_size = (n == 0) ? 1 : (static_cast<size_t>(1) << n);
          body += "      <summary>Input " + std::to_string(i) +
                  " &mdash; Triptych spend proof"
                  " (n=" + std::to_string(n) +
                  ", ring size " + std::to_string(ring_size) + ")</summary>\n";
          body += "      <ul>\n";
          dumpPointArray(signature.I_bits, "I_bits");
          dumpPointArray(signature.A,      "A");
          dumpPointArray(signature.B,      "B");
          dumpPointArray(signature.Q_P,    "Q_P");
          dumpPointArray(signature.Q_M,    "Q_M");
          dumpPointArray(signature.Q_U,    "Q_U");
          dumpScalarArray(signature.z,  "z");
          dumpScalarArray(signature.za, "za");
          dumpScalarArray(signature.zb, "zb");
          body += "        <li>f_P: <span class=\"wrap\">" + Common::podToHex(signature.f_P) + "</span></li>\n";
          body += "        <li>f_M: <span class=\"wrap\">" + Common::podToHex(signature.f_M) + "</span></li>\n";
          body += "        <li>f_U: <span class=\"wrap\">" + Common::podToHex(signature.f_U) + "</span></li>\n";
          body += "      </ul>\n";
        } else {
          // Legacy ring signature: v1 transparent input, or v2 KeyInput
          // shielding into the CT pool (CT slot is empty, n=0xFF).
          const bool isV2KeyInput = (transactionsDetails.version == TRANSACTION_VERSION_CT);
          body += "      <summary>Input " + std::to_string(i) +
                  " &mdash; " +
                  (isV2KeyInput ? "transparent shielding (legacy ring signature)"
                                : "legacy ring signature") +
                  "</summary>\n";
          body += "      <ol>\n";
          if (i < sigs.size()) {
            for (const auto& s1 : sigs[i]) {
              body += "        <li class=\"wrap\">" + Common::podToHex(s1) + "</li>\n";
            }
          }
          body += "      </ol>\n";
        }

        body += "    </details>\n";
        body += "  </li>\n";
      }
      body += "</ol>\n";
    }

    if (transactionsDetails.version == TRANSACTION_VERSION_CT) {
      body += "<h3>CT kernel</h3>\n";
      body += "<ul>\n";
      body += "  <li>Excess commitment: <span class=\"wrap\">" + Common::podToHex(transactionsDetails.kernel.excessCommitment) + "</span></li>\n";
      body += "  <li>Signature e: <span class=\"wrap\">" + Common::podToHex(transactionsDetails.kernel.sigE) + "</span></li>\n";
      body += "  <li>Signature s: <span class=\"wrap\">" + Common::podToHex(transactionsDetails.kernel.sigS) + "</span></li>\n";
      body += "</ul>\n";

      if (!transactionsDetails.ctProofs.empty()) {
        body += "<h3>CT output proofs</h3>\n";
        body += "<ol>\n";
        for (size_t i = 0; i < transactionsDetails.ctProofs.size(); ++i) {
          const auto& proof = transactionsDetails.ctProofs[i];
          body += "  <li>\n";
          body += "    <details>\n";
          body += "      <summary>Output " + std::to_string(i) + " GK denomination proof</summary>\n";
          body += "      <ul>\n";
          appendPodArray(body, "I", proof.I);
          appendPodArray(body, "A", proof.A);
          appendPodArray(body, "B", proof.B);
          appendPodArray(body, "Q", proof.Q);
          appendPodArray(body, "z", proof.z);
          appendPodArray(body, "za", proof.za);
          appendPodArray(body, "zb", proof.zb);
          body += "        <li>f: <span class=\"wrap\">" + Common::podToHex(proof.f) + "</span></li>\n";
          body += "      </ul>\n";
          body += "    </details>\n";
          body += "  </li>\n";
        }
        body += "</ol>\n";
      }
    }

    body += index_finish;

    res = body;
  }
  catch (std::system_error& e) {
    throw JsonRpc::JsonRpcError{ CORE_RPC_ERROR_CODE_INTERNAL_ERROR, e.what() };
    return false;
  }
  catch (std::exception& e) {
    throw JsonRpc::JsonRpcError{ CORE_RPC_ERROR_CODE_INTERNAL_ERROR, "Error: " + std::string(e.what()) };
    return false;
  }

  return true;
}


bool BuiltinExplorer::on_get_explorer_txs_by_payment_id(const COMMAND_EXPLORER_GET_TRANSACTIONS_BY_PAYMENT_ID::request& req, COMMAND_EXPLORER_GET_TRANSACTIONS_BY_PAYMENT_ID::response& res) {
  Crypto::Hash paymentId;
  if (!parse_hash256(req.payment_id, paymentId)) {
    throw JsonRpc::JsonRpcError{
      CORE_RPC_ERROR_CODE_WRONG_PARAM,
      "Failed to parse Payment ID: " + req.payment_id + '.' };
  }

  std::vector<Crypto::Hash> txHashes = m_core.getTransactionHashesByPaymentId(paymentId);

  if (txHashes.empty())
    return false;

  std::string body = index_start + (m_core.currency().isTestnet() ? "testnet" : "mainnet") + "\n<p>";

  body += "<a href=\"/explorer/\">Home</a>";
  body += "<hr />";

  body += "<h2>Payment ID <span class=\"wrap\">" + Common::podToHex(paymentId) + "</span></h2>\n";

  body += "<h3>Transactions with this Payment ID:</h3>\n";

  // simple list of tx hashes without details
  body += "<ol>\n";
  for (const auto& tx : txHashes) {
    std::string txHashStr = Common::podToHex(tx);
    body += "  <li>\n";
    body += "    <a class=\"wrap\" href=\"/explorer/tx/" + txHashStr + "\">";
    body += txHashStr;
    body += "    </a>";
    body += "  </li>\n";
  }
  body += "</ol>\n";

  body += index_finish;

  res = body;

  return true;
}

bool BuiltinExplorer::on_get_explorer_account_number(const COMMAND_EXPLORER_GET_ACCOUNT_NUMBER::request& req, COMMAND_EXPLORER_GET_ACCOUNT_NUMBER::response& res) {
  AccountNumber acctNum;
  if (!AccountNumber::fromString(req.account_number, acctNum)) {
    return false;
  }

  AccountPublicAddress address;
  if (!m_core.resolveAccountNumber(acctNum.blockHeight, acctNum.txIndex, address)) {
    return false;
  }

  std::string addressStr = m_core.currency().accountAddressAsString(address);

  std::string body = index_start + (m_core.currency().isTestnet() ? "testnet" : "mainnet") + "\n<p>";
  body += "<a href=\"/explorer/\">Home</a>";
  body += "<hr />";

  body += "<h2>Account Number " + acctNum.toString() + "</h2>\n";

  body += "<ul>\n";
  body += "  <li>\n";
  body += "    Block height: <a href=\"/explorer/block/" + std::to_string(acctNum.blockHeight) + "\">"
       + std::to_string(acctNum.blockHeight) + "</a>\n";
  body += "  </li>\n";
  body += "  <li>\n";
  body += "    Transaction index: " + std::to_string(acctNum.txIndex) + "\n";
  body += "  </li>\n";
  body += "  <li>\n";
  body += "    Address: <a class=\"wrap\" href=\"/explorer/address/" + addressStr + "\">"
       + addressStr + "</a>\n";
  body += "  </li>\n";
  body += "  <li>\n";
  body += "    Spend public key: <span class=\"wrap\">" + Common::podToHex(address.spendPublicKey) + "</span>\n";
  body += "  </li>\n";
  body += "  <li>\n";
  body += "    View public key: <span class=\"wrap\">" + Common::podToHex(address.viewPublicKey) + "</span>\n";
  body += "  </li>\n";
  body += "</ul>\n";

  body += index_finish;
  res = body;
  return true;
}

bool BuiltinExplorer::on_get_explorer_address(const COMMAND_EXPLORER_GET_ADDRESS::request& req, COMMAND_EXPLORER_GET_ADDRESS::response& res) {
  AccountPublicAddress address;
  uint64_t prefix;
  if (!parseAccountAddressString(prefix, address, req.address)) {
    return false;
  }

  bool validSpend = Crypto::check_key(address.spendPublicKey);
  bool validView = Crypto::check_key(address.viewPublicKey);

  std::string body = index_start + (m_core.currency().isTestnet() ? "testnet" : "mainnet") + "\n<p>";
  body += "<a href=\"/explorer/\">Home</a>";
  body += "<hr />";

  body += "<h2>Address</h2>\n";
  body += "<p class=\"wrap\">" + req.address + "</p>\n";

  body += "<h3>Validation</h3>\n";
  body += "<ul>\n";
  body += "  <li>Spend public key: <span class=\"wrap\">" + Common::podToHex(address.spendPublicKey) + "</span>"
       + (validSpend ? " &#10004;" : " &#10008; <b>INVALID</b>") + "</li>\n";
  body += "  <li>View public key: <span class=\"wrap\">" + Common::podToHex(address.viewPublicKey) + "</span>"
       + (validView ? " &#10004;" : " &#10008; <b>INVALID</b>") + "</li>\n";
  body += "</ul>\n";

  // Look up registered account number
  {
    uint32_t blockHeight, txIndex;
    if (m_core.getAccountNumber(address, blockHeight, txIndex)) {
      AccountNumber an{blockHeight, txIndex};
      std::string anStr = an.toString();
      body += "<h3>Account Number</h3>\n";
      body += "<p><a href=\"/explorer/account/" + anStr + "\">" + anStr + "</a>"
              " (block <a href=\"/explorer/block/" + std::to_string(blockHeight) + "\">"
              + std::to_string(blockHeight) + "</a>)</p>\n";
    } else {
      body += "<p>No account number registered for this address.</p>\n";
    }
  }

  body += index_finish;
  res = body;
  return true;
}

} // namespace CryptoNote
