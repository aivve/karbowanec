// Copyright (c) 2012-2016, The CryptoNote developers, The Bytecoin developers
// Copyright (c) 2016-2026, Karbo developers
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

#include <algorithm>
#include <cassert>
#include <future>
#include "crypto/crypto.h"
#include "crypto/random.h"
#include "CryptoNoteCore/Account.h"
#include "CryptoNoteCore/CryptoNoteTools.h"
#include "CryptoNoteCore/CryptoNoteSerialization.h"

#include "WalletLegacy/WalletTransactionSender.h"
#include "WalletLegacy/WalletUtils.h"

#include "CryptoNoteCore/CryptoNoteBasicImpl.h"
#include "CryptoNoteCore/CryptoNoteFormatUtils.h"
#include "CryptoNoteCore/CryptoNoteTools.h"
#include "CryptoNoteConfig.h"
#include "Denominations.h"
#include "crypto/transaction_balance.h"

#include <Logging/LoggerGroup.h>

using namespace Crypto;

namespace {

using namespace CryptoNote;

uint64_t countNeededMoney(uint64_t fee, const std::vector<WalletLegacyTransfer>& transfers) {
  uint64_t needed_money = fee;
  for (auto& transfer: transfers) {
    throwIf(transfer.amount == 0, error::ZERO_DESTINATION);
    throwIf(transfer.amount < 0, error::WRONG_AMOUNT);

    needed_money += transfer.amount;
    throwIf(static_cast<int64_t>(needed_money) < transfer.amount, error::SUM_OVERFLOW);
  }

  return needed_money;
}

void createChangeDestinations(const AccountPublicAddress& address, uint64_t neededMoney, uint64_t foundMoney, CryptoNote::TxBuildOutput& changeDts) {
  if (neededMoney < foundMoney) {
    changeDts.destination = address;
    changeDts.amount = foundMoney - neededMoney;
  }
}

std::shared_ptr<WalletLegacyEvent> makeCompleteEvent(WalletUserTransactionsCache& transactionCache, size_t transactionId, std::error_code ec) {
  transactionCache.updateTransactionSendingState(transactionId, ec);
  return std::make_shared<WalletSendTransactionCompletedEvent>(transactionId, ec);
}

bool isTransparentNonCanonicalCtAmount(const TransactionOutputInformation& output) {
  return output.type != TransactionTypes::OutputType::Confidential &&
         output.amount % CryptoNote::MIN_CT_DENOMINATION != 0;
}

} //namespace

namespace CryptoNote {

WalletTransactionSender::WalletTransactionSender(const Currency& currency, WalletUserTransactionsCache& transactionsCache, AccountKeys keys, ITransfersContainer& transfersContainer, INode& node) :
  m_currency(currency),
  m_node(node),
  m_transactionsCache(transactionsCache),
  m_isStoping(false),
  m_keys(keys),
  m_transferDetails(transfersContainer),
  m_upperTransactionSizeLimit(m_currency.maxTransactionSizeLimit()) {
}

void WalletTransactionSender::stop() {
  m_isStoping = true;
}

bool WalletTransactionSender::validateDestinationAddress(const std::string& address) {
  AccountPublicAddress ignore;
  return m_currency.parseAccountAddressString(address, ignore);
}

void WalletTransactionSender::validateTransfersAddresses(const std::vector<WalletLegacyTransfer>& transfers) {
  for (const WalletLegacyTransfer& tr : transfers) {
    if (!validateDestinationAddress(tr.address)) {
      throw std::system_error(make_error_code(error::BAD_ADDRESS));
    }
  }
}

uint64_t WalletTransactionSender::resolveSpendableAmount(const TransactionOutputInformation& output) const {
  return output.amount;
}

bool WalletTransactionSender::isCoinbaseOutput(const TransactionOutputInformation& output) const {
  if (output.type == TransactionTypes::OutputType::Confidential) {
    return false;
  }

  TransactionInformation txInfo;
  return m_transferDetails.getTransactionInformation(output.transactionHash, txInfo) &&
         txInfo.totalAmountIn == 0 &&
         txInfo.blockHeight >= m_currency.upgradeHeight(CryptoNote::BLOCK_MAJOR_VERSION_5);
}

std::vector<uint64_t> WalletTransactionSender::chooseInputMixins(
  const std::list<TransactionOutputInformation>& selectedTransfers,
  uint64_t requestedMixin,
  bool useCT) const {

  std::vector<uint64_t> inputMixins(selectedTransfers.size(), requestedMixin);
  if (!useCT) {
    return inputMixins;
  }

  const uint64_t minCtMixin = CryptoNote::parameters::CT_MIN_RING_SIZE - 1;
  const uint64_t maxCtMixin = CryptoNote::parameters::CT_MAX_RING_SIZE - 1;
  const uint64_t normalMixin = std::max<uint64_t>(requestedMixin, minCtMixin);

  size_t i = 0;
  for (const auto& output : selectedTransfers) {
    if (isCoinbaseOutput(output)) {
      inputMixins[i++] = 0;
    } else {
      if (normalMixin > maxCtMixin) {
        throw std::system_error(make_error_code(error::MIXIN_COUNT_TOO_BIG));
      }
      inputMixins[i++] = normalMixin;
    }
  }

  return inputMixins;
}

bool WalletTransactionSender::hasMixinInputs(const std::vector<uint64_t>& inputMixins) const {
  return std::any_of(inputMixins.begin(), inputMixins.end(), [](uint64_t inputMixin) { return inputMixin != 0; });
}

uint64_t WalletTransactionSender::maxInputMixin(const std::vector<uint64_t>& inputMixins) const {
  return inputMixins.empty() ? 0 : *std::max_element(inputMixins.begin(), inputMixins.end());
}

void WalletTransactionSender::checkIfEnoughMixins(
  const std::vector<COMMAND_RPC_GET_RANDOM_OUTPUTS_FOR_AMOUNTS::outs_for_amount>& outs,
  const std::vector<uint64_t>& inputMixins) const {

  if (outs.size() != inputMixins.size()) {
    throw std::system_error(make_error_code(error::MIXIN_COUNT_TOO_BIG));
  }

  for (size_t i = 0; i < inputMixins.size(); ++i) {
    if (inputMixins[i] == 0) {
      continue;
    }
    if (outs[i].outs.size() < inputMixins[i] + 1) {
      throw std::system_error(make_error_code(error::MIXIN_COUNT_TOO_BIG));
    }
  }
}

std::shared_ptr<WalletRequest> WalletTransactionSender::makeSendRequest(TransactionId& transactionId, std::deque<std::shared_ptr<WalletLegacyEvent>>& events,
    const std::vector<WalletLegacyTransfer>& transfers, const std::list<TransactionOutputInformation>& selectedOuts, uint64_t fee, const std::string& extra, uint64_t mixIn, uint64_t unlockTimestamp) {

  using namespace CryptoNote;

  throwIf(transfers.empty(), error::ZERO_DESTINATION);
  validateTransfersAddresses(transfers);
  uint64_t neededMoney = countNeededMoney(fee, transfers);
  const bool useCT = m_currency.currentTransactionVersion(m_node.getLastLocalBlockHeight()) == CryptoNote::TRANSACTION_VERSION_CT;

  std::shared_ptr<SendTransactionContext> context = std::make_shared<SendTransactionContext>();

  if (selectedOuts.size() > 0) {
    for (auto& out : selectedOuts) {
      context->foundMoney += resolveSpendableAmount(out);
    }
    context->selectedTransfers = selectedOuts;
  }
  else {
    context->foundMoney = selectTransfersToSend(
      neededMoney,
      0 == mixIn,
      context->dustPolicy.dustThreshold,
      context->selectedTransfers,
      useCT);
  }

  throwIf(context->foundMoney < neededMoney, error::WRONG_AMOUNT);

  transactionId = m_transactionsCache.addNewTransaction(neededMoney, fee, extra, transfers, unlockTimestamp);
  context->transactionId = transactionId;
  context->mixIn = mixIn;
  context->inputMixins = chooseInputMixins(context->selectedTransfers, mixIn, useCT);

  if (hasMixinInputs(context->inputMixins)) {
    std::shared_ptr<WalletRequest> request = makeGetRandomOutsRequest(context);
    return request;
  }

  return doSendTransaction(context, events);
}

std::string WalletTransactionSender::makeRawTransaction(TransactionId& transactionId, std::deque<std::shared_ptr<WalletLegacyEvent>>& events,
  const std::vector<WalletLegacyTransfer>& transfers, const std::list<CryptoNote::TransactionOutputInformation>& selectedOuts,
  uint64_t fee, const std::string& extra, uint64_t mixIn, uint64_t unlockTimestamp)
{

  std::string raw_tx;

  using namespace CryptoNote;

  throwIf(transfers.empty(), error::ZERO_DESTINATION);
  validateTransfersAddresses(transfers);
  uint64_t neededMoney = countNeededMoney(fee, transfers);
  const bool useCT = m_currency.currentTransactionVersion(m_node.getLastLocalBlockHeight()) == CryptoNote::TRANSACTION_VERSION_CT;

  std::shared_ptr<SendTransactionContext> context = std::make_shared<SendTransactionContext>();

  if (selectedOuts.size() > 0) {
    for (auto& out : selectedOuts) {
      context->foundMoney += resolveSpendableAmount(out);
    }
    context->selectedTransfers = selectedOuts;
  }
  else {
     context->foundMoney = selectTransfersToSend(
       neededMoney,
       0 == mixIn,
       context->dustPolicy.dustThreshold,
       context->selectedTransfers,
       useCT);
  }

  throwIf(context->foundMoney < neededMoney, error::WRONG_AMOUNT);

  // add tx to wallet cache to prevent reuse of outputs used in this tx
  transactionId = m_transactionsCache.addNewTransaction(neededMoney, fee, extra, transfers, unlockTimestamp);
  context->transactionId = transactionId;
  context->mixIn = mixIn;
  context->inputMixins = chooseInputMixins(context->selectedTransfers, mixIn, useCT);

  if (hasMixinInputs(context->inputMixins)) {
    uint64_t outsCount = maxInputMixin(context->inputMixins) + 1; // add one to make possible (if need) to skip real output key
    std::vector<uint64_t> amounts;

    for (const auto& td : context->selectedTransfers) {
      amounts.push_back(td.type == TransactionTypes::OutputType::Confidential
        ? CryptoNote::parameters::CT_CONFIDENTIAL_OUTPUT_AMOUNT
        : td.amount);
    }

    auto queryAmountsCompleted = std::promise<std::error_code>();
    auto queryAmountsWaitFuture = queryAmountsCompleted.get_future();

    m_node.getRandomOutsByAmounts(std::move(amounts),
      outsCount,
      std::ref(context->outs),
      [&queryAmountsCompleted](std::error_code ec) {
      auto detachedPromise = std::move(queryAmountsCompleted);
      detachedPromise.set_value(ec);
    });

    queryAmountsWaitFuture.get();

    checkIfEnoughMixins(context->outs, context->inputMixins);
  }

  // instead of doSendTransaction prepare tx here to prevent relay
  try
  {
    WalletLegacyTransaction& transaction = m_transactionsCache.getTransaction(context->transactionId);

    std::vector<TxBuildInput> inputs;
    prepareInputs(context->selectedTransfers, context->outs, inputs, context->inputMixins);

    TxBuildOutput changeDts;
    changeDts.amount = 0;
    uint64_t totalAmount = -transaction.totalAmount;
    createChangeDestinations(m_keys.address, totalAmount, context->foundMoney, changeDts);

    std::vector<TxBuildOutput> splittedDests;
    splitDestinations(transaction.firstTransferId, transaction.transferCount, changeDts, context->dustPolicy, splittedDests);

    auto itx = buildTransaction(inputs, splittedDests, m_keys.viewSecretKey,
        transaction.extra, transaction.unlockTime, m_upperTransactionSizeLimit, context->tx_key);
    Transaction tx;
    if (!fromBinaryArray(tx, itx->getTransactionData()))
      throw std::system_error(make_error_code(error::INTERNAL_WALLET_ERROR));

    getObjectHash(tx, transaction.hash);

    m_transactionsCache.updateTransaction(context->transactionId, tx, totalAmount, context->selectedTransfers, context->tx_key);

    notifyBalanceChanged(events);

    raw_tx = Common::toHex(toBinaryArray(tx));

    events.push_back(makeCompleteEvent(m_transactionsCache, context->transactionId, std::error_code()));
  }
  catch (std::system_error& ec) {
    events.push_back(makeCompleteEvent(m_transactionsCache, context->transactionId, ec.code()));
  }
  catch (std::exception&) {
    events.push_back(makeCompleteEvent(m_transactionsCache, context->transactionId, make_error_code(error::INTERNAL_WALLET_ERROR)));
  }

  return raw_tx;
}

std::shared_ptr<WalletRequest> WalletTransactionSender::makeGetRandomOutsRequest(std::shared_ptr<SendTransactionContext> context) {
  uint64_t outsCount = maxInputMixin(context->inputMixins) + 1;// add one to make possible (if need) to skip real output key
  std::vector<uint64_t> amounts;

  for (const auto& td : context->selectedTransfers) {
    amounts.push_back(td.type == TransactionTypes::OutputType::Confidential
      ? CryptoNote::parameters::CT_CONFIDENTIAL_OUTPUT_AMOUNT
      : td.amount);
  }

  return std::make_shared<WalletGetRandomOutsByAmountsRequest>(amounts, outsCount, context, std::bind(&WalletTransactionSender::sendTransactionRandomOutsByAmount,
      this, context, std::placeholders::_1, std::placeholders::_2, std::placeholders::_3));
}

void WalletTransactionSender::sendTransactionRandomOutsByAmount(std::shared_ptr<SendTransactionContext> context, std::deque<std::shared_ptr<WalletLegacyEvent>>& events,
    boost::optional<std::shared_ptr<WalletRequest> >& nextRequest, std::error_code ec) {
  
  if (m_isStoping) {
    ec = make_error_code(error::TX_CANCELLED);
  }

  if (ec) {
    events.push_back(makeCompleteEvent(m_transactionsCache, context->transactionId, ec));
    return;
  }

  try {
    checkIfEnoughMixins(context->outs, context->inputMixins);
  } catch (const std::system_error&) {
    events.push_back(makeCompleteEvent(m_transactionsCache, context->transactionId, make_error_code(error::MIXIN_COUNT_TOO_BIG)));
    return;
  }

  std::shared_ptr<WalletRequest> req = doSendTransaction(context, events);
  if (req)
    nextRequest = req;
}

std::shared_ptr<WalletRequest> WalletTransactionSender::doSendTransaction(std::shared_ptr<SendTransactionContext> context, std::deque<std::shared_ptr<WalletLegacyEvent>>& events) {
  if (m_isStoping) {
    events.push_back(makeCompleteEvent(m_transactionsCache, context->transactionId, make_error_code(error::TX_CANCELLED)));
    return std::shared_ptr<WalletRequest>();
  }

  try
  {
    WalletLegacyTransaction& transaction = m_transactionsCache.getTransaction(context->transactionId);

    std::vector<TxBuildInput> inputs;
    prepareInputs(context->selectedTransfers, context->outs, inputs, context->inputMixins);

    uint64_t totalAmount = -transaction.totalAmount;
    uint64_t changeAmount = context->foundMoney > totalAmount ? context->foundMoney - totalAmount : 0;

    const bool useCT = m_currency.currentTransactionVersion(m_node.getLastLocalBlockHeight()) == CryptoNote::TRANSACTION_VERSION_CT;

    Transaction tx;
    if (useCT) {
      // CT path: canonical denomination decomposition + confidential transaction.
      // Sub-floor change residue is absorbed into fee so no new CT dust is ever created.

      // Reject non-canonical destination amounts up-front.
      for (TransferId idx = transaction.firstTransferId;
           idx < transaction.firstTransferId + transaction.transferCount; ++idx) {
        const WalletLegacyTransfer& de = m_transactionsCache.getTransfer(idx);
        const uint64_t amt = static_cast<uint64_t>(de.amount);
        if (amt == 0 || amt % CryptoNote::MIN_CT_DENOMINATION != 0) {
          throw std::system_error(make_error_code(error::WRONG_AMOUNT),
            "Confidential transactions require amounts to be a multiple of 0.01 KRB");
        }
      }

      // Split change into canonical + sub-floor residue; fold residue into the fee.
      uint64_t changeCanonical = (changeAmount / CryptoNote::MIN_CT_DENOMINATION)
                                * CryptoNote::MIN_CT_DENOMINATION;
      uint64_t dustResidue = changeAmount - changeCanonical;
      uint64_t fee = transaction.fee + dustResidue;

      // Decompose each destination into canonical denominations
      std::vector<CTBuildOutput> ctOutputs;
      for (TransferId idx = transaction.firstTransferId;
           idx < transaction.firstTransferId + transaction.transferCount; ++idx) {
        WalletLegacyTransfer& de = m_transactionsCache.getTransfer(idx);
        AccountPublicAddress addr;
        if (!m_currency.parseAccountAddressString(de.address, addr))
          throw std::system_error(make_error_code(error::BAD_ADDRESS));
        auto denoms = decomposeAmount(static_cast<uint64_t>(de.amount));
        for (uint64_t d : denoms) {
          ctOutputs.push_back(CTBuildOutput{addr, d});
        }
      }

      // Canonical change output (residue is in fee, not here).
      if (changeCanonical > 0) {
        auto changeDenoms = decomposeAmount(changeCanonical);
        for (uint64_t d : changeDenoms) {
          ctOutputs.push_back(CTBuildOutput{m_keys.address, d});
        }
      }

      // Convert TxBuildInput → CTBuildInput
      std::vector<CTBuildInput> ctInputs;
      for (auto& inp : inputs) {
        CTBuildInput cti;
        const auto& ki = inp.keyInfo;
        cti.ringAmount = ki.amount;
        for (const auto& gout : ki.outputs) {
          cti.ringPubkeys.push_back(gout.targetKey);
          cti.ringOutputIndexes.push_back(gout.outputIndex);
        }
        // Ring commitments must be computed exactly as core validation does.
        TransactionOutput ringMemberOutput;
        ringMemberOutput.amount = ki.amount;
        ringMemberOutput.target = KeyOutput();
        for (size_t k = 0; k < ki.outputs.size(); ++k) {
          Crypto::EllipticCurvePoint ringCommit{};
          if (ki.outputs[k].isConfidential) {
            ringCommit = ki.outputs[k].commitment;
          } else {
            if (!Crypto::transparent_amount_to_commitment(ringMemberOutput.amount, ringCommit)) {
              throw std::runtime_error("Failed to compute CT ring commitment for input " + std::to_string(ctInputs.size()));
            }
          }
          cti.ringCommitments.push_back(ringCommit);
        }
        cti.realIndex = ki.realOutput.transactionIndex;
        if (cti.realIndex >= ki.outputs.size()) {
          throw std::system_error(make_error_code(error::INTERNAL_WALLET_ERROR));
        }
        // Derive ephemeral spend key
        KeyPair ephKeys;
        Crypto::KeyImage dummy_ki;
        CryptoNote::generate_key_image_helper(inp.senderKeys,
            ki.realOutput.transactionPublicKey,
            ki.realOutput.outputInTransaction,
            ephKeys, dummy_ki);
        cti.spendPrivkey = ephKeys.secretKey;
        cti.realBlinding = ki.realOutputBlinding;
        cti.amount = ki.realOutputAmount;
        ctInputs.push_back(std::move(cti));
      }

      tx = buildConfidentialTransaction(ctInputs, ctOutputs, m_keys.viewSecretKey,
                                         fee, transaction.extra, context->tx_key);
    } else {
      // Pre-fork: transparent transaction path (original)
      TxBuildOutput changeDts;
      changeDts.amount = 0;
      createChangeDestinations(m_keys.address, totalAmount, context->foundMoney, changeDts);

      std::vector<TxBuildOutput> splittedDests;
      splitDestinations(transaction.firstTransferId, transaction.transferCount, changeDts, context->dustPolicy, splittedDests);

      auto itx = buildTransaction(inputs, splittedDests, m_keys.viewSecretKey,
          transaction.extra, transaction.unlockTime, m_upperTransactionSizeLimit, context->tx_key);
      if (!fromBinaryArray(tx, itx->getTransactionData()))
        throw std::system_error(make_error_code(error::INTERNAL_WALLET_ERROR));
    }

    getObjectHash(tx, transaction.hash);
    transaction.secretKey = context->tx_key;

    m_transactionsCache.updateTransaction(context->transactionId, tx, totalAmount, context->selectedTransfers, context->tx_key);

    notifyBalanceChanged(events);

    return std::make_shared<WalletRelayTransactionRequest>(tx, std::bind(&WalletTransactionSender::relayTransactionCallback, this, context,
        std::placeholders::_1, std::placeholders::_2, std::placeholders::_3));
  }
  catch(std::system_error& ec) {
    events.push_back(makeCompleteEvent(m_transactionsCache, context->transactionId, ec.code()));
  }
  catch(std::exception&) {
    events.push_back(makeCompleteEvent(m_transactionsCache, context->transactionId, make_error_code(error::INTERNAL_WALLET_ERROR)));
  }

  return std::shared_ptr<WalletRequest>();
}

void WalletTransactionSender::relayTransactionCallback(std::shared_ptr<SendTransactionContext> context, std::deque<std::shared_ptr<WalletLegacyEvent>>& events,
                                                       boost::optional<std::shared_ptr<WalletRequest> >& nextRequest, std::error_code ec) {
  if (m_isStoping) {
    return;
  }

  events.push_back(makeCompleteEvent(m_transactionsCache, context->transactionId, ec));
}


void WalletTransactionSender::splitDestinations(TransferId firstTransferId, size_t transfersCount, const TxBuildOutput& changeDts,
  const TxDustPolicy& dustPolicy, std::vector<TxBuildOutput>& splittedDests) {
  uint64_t dust = 0;

  digitSplitStrategy(firstTransferId, transfersCount, changeDts, dustPolicy.dustThreshold, splittedDests, dust);

  throwIf(dustPolicy.dustThreshold < dust, error::INTERNAL_WALLET_ERROR);
  if (0 != dust && !dustPolicy.addToFee) {
    splittedDests.push_back(TxBuildOutput{dustPolicy.addrForDust, dust});
  }
}


void WalletTransactionSender::digitSplitStrategy(TransferId firstTransferId, size_t transfersCount,
  const TxBuildOutput& change_dst, uint64_t dust_threshold,
  std::vector<TxBuildOutput>& splitted_dsts, uint64_t& dust) {
  splitted_dsts.clear();
  dust = 0;

  for (TransferId idx = firstTransferId; idx < firstTransferId + transfersCount; ++idx) {
    WalletLegacyTransfer& de = m_transactionsCache.getTransfer(idx);

    AccountPublicAddress addr;
    if (!m_currency.parseAccountAddressString(de.address, addr)) {
      throw std::system_error(make_error_code(error::BAD_ADDRESS));
    }

    decompose_amount_into_digits(de.amount, dust_threshold,
      [&](uint64_t chunk) { splitted_dsts.push_back(TxBuildOutput{addr, chunk}); },
      [&](uint64_t a_dust) { splitted_dsts.push_back(TxBuildOutput{addr, a_dust}); });
  }

  decompose_amount_into_digits(change_dst.amount, dust_threshold,
    [&](uint64_t chunk) { splitted_dsts.push_back(TxBuildOutput{change_dst.destination, chunk}); },
    [&](uint64_t a_dust) { dust = a_dust; } );
}


void WalletTransactionSender::prepareInputs(
  const std::list<TransactionOutputInformation>& selectedTransfers,
  std::vector<COMMAND_RPC_GET_RANDOM_OUTPUTS_FOR_AMOUNTS::outs_for_amount>& outs,
  std::vector<TxBuildInput>& inputs, const std::vector<uint64_t>& inputMixins) {

  size_t i = 0;
  assert(inputMixins.size() == selectedTransfers.size());

  for (const auto& td: selectedTransfers) {
    inputs.resize(inputs.size() + 1);
    TxBuildInput& inp = inputs.back();
    const uint64_t inputMixin = inputMixins[i];

    const bool realIsConfidential = td.type == TransactionTypes::OutputType::Confidential;
    inp.keyInfo.amount = realIsConfidential
      ? CryptoNote::parameters::CT_CONFIDENTIAL_OUTPUT_AMOUNT
      : td.amount;
    inp.senderKeys = m_keys;

    TransactionInformation txInfo;
    bool haveTxInfo = m_transferDetails.getTransactionInformation(td.transactionHash, txInfo);
    const bool realIsCoinbase = haveTxInfo && txInfo.totalAmountIn == 0;
    const uint32_t realBlockHeight = haveTxInfo ? txInfo.blockHeight : 0;

    //paste mixin transaction
    if (inputMixin != 0 && outs.size()) {
      std::sort(outs[i].outs.begin(), outs[i].outs.end(),
        [](const COMMAND_RPC_GET_RANDOM_OUTPUTS_FOR_AMOUNTS::out_entry& a, const COMMAND_RPC_GET_RANDOM_OUTPUTS_FOR_AMOUNTS::out_entry& b){ return a.global_amount_index < b.global_amount_index; });
      for (auto& daemon_oe: outs[i].outs) {
        if (td.globalOutputIndex == daemon_oe.global_amount_index)
          continue;
        TransactionTypes::GlobalOutput go;
        go.outputIndex = static_cast<uint32_t>(daemon_oe.global_amount_index);
        go.targetKey = daemon_oe.out_key;
        go.commitment = daemon_oe.commitment;
        go.blockHeight = daemon_oe.block_height;
        go.isCoinbase = daemon_oe.is_coinbase != 0;
        go.isConfidential = daemon_oe.output_type == static_cast<uint8_t>(TransactionTypes::OutputType::Confidential);
        inp.keyInfo.outputs.push_back(go);
        if (inp.keyInfo.outputs.size() >= inputMixin)
          break;
      }
    }

    //paste real transaction to the random index
    auto it_to_insert = std::find_if(inp.keyInfo.outputs.begin(), inp.keyInfo.outputs.end(),
      [&](const TransactionTypes::GlobalOutput& a) { return a.outputIndex >= td.globalOutputIndex; });

    TransactionTypes::GlobalOutput real_go;
    real_go.outputIndex = td.globalOutputIndex;
    real_go.targetKey = td.outputKey;
    real_go.commitment = td.commitment;
    real_go.blockHeight = realBlockHeight;
    real_go.isCoinbase = realIsCoinbase;
    real_go.isConfidential = realIsConfidential;

    auto inserted_it = inp.keyInfo.outputs.insert(it_to_insert, real_go);

    inp.keyInfo.realOutput.transactionPublicKey = td.transactionPublicKey;
    inp.keyInfo.realOutput.transactionIndex = inserted_it - inp.keyInfo.outputs.begin();
    inp.keyInfo.realOutput.outputInTransaction = td.outputInTransaction;
    if (realIsConfidential) {
      inp.keyInfo.realOutputAmount = td.amount;
      inp.keyInfo.realOutputBlinding = td.blindingFactor;
    } else {
      TransactionOutput transparentOutput;
      transparentOutput.amount = td.amount;
      transparentOutput.target = KeyOutput();
      inp.keyInfo.realOutputAmount = transparentOutput.amount;
      std::memset(&inp.keyInfo.realOutputBlinding, 0, sizeof(inp.keyInfo.realOutputBlinding));
    }
    inp.keyInfo.realOutputIsConfidential = realIsConfidential;
    ++i;
  }
}

void WalletTransactionSender::notifyBalanceChanged(std::deque<std::shared_ptr<WalletLegacyEvent>>& events) {
  uint64_t unconfirmedOutsAmount = m_transactionsCache.unconfrimedOutsAmount();
  uint64_t change = unconfirmedOutsAmount - m_transactionsCache.unconfirmedTransactionsAmount();

  uint64_t actualBalance = m_transferDetails.balance(ITransfersContainer::IncludeDefault) - unconfirmedOutsAmount;
  uint64_t pendingBalance = m_transferDetails.balance(ITransfersContainer::IncludeAllLocked) + change;

  events.push_back(std::make_shared<WalletActualBalanceUpdatedEvent>(actualBalance));
  events.push_back(std::make_shared<WalletPendingBalanceUpdatedEvent>(pendingBalance));
}

namespace {

template<typename URNG, typename T>
T popRandomValue(URNG& randomGenerator, std::vector<T>& vec) {
  assert(!vec.empty());

  if (vec.empty()) {
    return T();
  }

  std::uniform_int_distribution<size_t> distribution(0, vec.size() - 1);
  size_t idx = distribution(randomGenerator);

  T res = vec[idx];
  if (idx + 1 != vec.size()) {
    vec[idx] = vec.back();
  }
  vec.resize(vec.size() - 1);

  return res;
}

}


uint64_t WalletTransactionSender::selectTransfersToSend(
  uint64_t neededMoney,
  bool addUnmixable,
  uint64_t dust,
  std::list<TransactionOutputInformation>& selectedTransfers,
  bool includeNonCanonical) {

  std::vector<size_t> unusedTransfers;
  std::vector<size_t> unusedDust;
  std::vector<size_t> unusedUnmixable;
  std::vector<size_t> nonCanonicalOutputs;

  std::vector<TransactionOutputInformation> outputs;
  m_transferDetails.getOutputs(outputs, ITransfersContainer::IncludeDefault);
  std::vector<uint64_t> spendableAmounts(outputs.size(), 0);
  std::vector<uint8_t> used(outputs.size(), 0);

  for (size_t i = 0; i < outputs.size(); ++i) {
    const auto& out = outputs[i];
    if (!m_transactionsCache.isUsed(out)) {
      const uint64_t spendableAmount = resolveSpendableAmount(out);
      spendableAmounts[i] = spendableAmount;
      if (isTransparentNonCanonicalCtAmount(out)) {
        nonCanonicalOutputs.push_back(i);
        if (includeNonCanonical) {
          continue;
        }
      }

      if (out.type == TransactionTypes::OutputType::Confidential || is_valid_decomposed_amount(out.amount)) {
        if (dust < spendableAmount) {
          unusedTransfers.push_back(i);
        } else {
          unusedDust.push_back(i);
        }
      } else {
        unusedUnmixable.push_back(i);
      }
    }
  }

  uint64_t foundMoney = 0;
  auto selectOutput = [&](size_t idx) {
    if (used[idx]) {
      return false;
    }
    used[idx] = 1;
    selectedTransfers.push_back(outputs[idx]);
    foundMoney += spendableAmounts[idx];
    return true;
  };
  std::mt19937 urng = Random::generator();

  // Phase 1: prefer canonical / mixable inputs.
  while (foundMoney < neededMoney &&
         selectedTransfers.size() < CryptoNote::parameters::CT_MAX_INPUTS &&
         (!unusedTransfers.empty() || !unusedDust.empty() || (addUnmixable && !unusedUnmixable.empty()))) {
    size_t idx;
    if (addUnmixable && !unusedUnmixable.empty()) {
      idx = popRandomValue(urng, unusedUnmixable);
    } else {
      idx = !unusedTransfers.empty() ? popRandomValue(urng, unusedTransfers) : popRandomValue(urng, unusedDust);
    }
    selectOutput(idx);
  }

  // Phase 2: if a shortfall remains, fall back to non-canonical inputs (sub-floor
  // residue, V5+ coinbase whose single output is not a multiple of MIN_CT_DENOMINATION,
  // etc.). The CT path absorbs the residue into the fee. Bounded only by CT_MAX_INPUTS.
  //
  // Sort descending by spendable amount and pick largest-first instead of random:
  // a wallet with a mix of large V5+ coinbase outputs (~tens of KRB each) and many
  // tiny sub-floor pieces (sub-cent) would otherwise random-walk through the dust
  // and exhaust CT_MAX_INPUTS long before covering the spend.
  if (includeNonCanonical && foundMoney < neededMoney && !nonCanonicalOutputs.empty()) {
    std::sort(nonCanonicalOutputs.begin(), nonCanonicalOutputs.end(),
              [&spendableAmounts](size_t a, size_t b) {
                return spendableAmounts[a] > spendableAmounts[b];
              });
    for (size_t idx : nonCanonicalOutputs) {
      if (foundMoney >= neededMoney ||
          selectedTransfers.size() >= CryptoNote::parameters::CT_MAX_INPUTS) {
        break;
      }
      selectOutput(idx);
    }
  }

  return foundMoney;
}

} /* namespace CryptoNote */
