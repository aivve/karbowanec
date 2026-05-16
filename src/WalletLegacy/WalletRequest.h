// Copyright (c) 2012-2016, The CryptoNote developers, The Bytecoin developers
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

#include "INode.h"
// #include "WalletSynchronizationContext.h"
#include "WalletLegacy/WalletSendTransactionContext.h"
#include "WalletLegacy/WalletLegacyEvent.h"

#include <boost/optional.hpp>

#include <deque>
#include <functional>
#include <memory>

namespace CryptoNote {

class WalletRequest
{
public:
  typedef std::function<void(std::deque<std::shared_ptr<WalletLegacyEvent>>& events, boost::optional<std::shared_ptr<WalletRequest> >& nextRequest, std::error_code ec)> Callback;

  virtual ~WalletRequest() {};

  virtual void perform(INode& node, std::function<void (WalletRequest::Callback, std::error_code)> cb) = 0;
};

class WalletGetRandomOutsByAmountsRequest: public WalletRequest
{
public:
  WalletGetRandomOutsByAmountsRequest(const std::vector<uint64_t>& amounts, uint64_t outsCount, std::shared_ptr<SendTransactionContext> context, Callback cb) :
    m_amounts(amounts), m_outsCount(outsCount), m_context(context), m_cb(cb) {};

  virtual ~WalletGetRandomOutsByAmountsRequest() {};

  virtual void perform(INode& node, std::function<void (WalletRequest::Callback, std::error_code)> cb) override
  {
    node.getRandomOutsByAmounts(std::move(m_amounts), m_outsCount, std::ref(m_context->outs), std::bind(cb, m_cb, std::placeholders::_1));
  };

private:
  std::vector<uint64_t> m_amounts;
  uint64_t m_outsCount;
  std::shared_ptr<SendTransactionContext> m_context;
  Callback m_cb;
};

// Second-stage request for CT cross-bucket decoy sampling. The caller pre-
// filters zero-bucket inputs (the daemon rejects amount=0) and records the
// original-index map in the context; this request writes the daemon's raw
// response into context->mixingOutsRaw. The follow-up callback in
// WalletTransactionSender expands raw → mixingOuts using the index map.
class WalletGetMixingOutsByAmountsRequest: public WalletRequest
{
public:
  WalletGetMixingOutsByAmountsRequest(const std::vector<uint64_t>& amounts, uint64_t outsCount, std::shared_ptr<SendTransactionContext> context, Callback cb) :
    m_amounts(amounts), m_outsCount(outsCount), m_context(context), m_cb(cb) {};

  virtual ~WalletGetMixingOutsByAmountsRequest() {};

  virtual void perform(INode& node, std::function<void (WalletRequest::Callback, std::error_code)> cb) override
  {
    node.getRandomOutsByAmounts(std::move(m_amounts), m_outsCount, std::ref(m_context->mixingOutsRaw), std::bind(cb, m_cb, std::placeholders::_1));
  };

private:
  std::vector<uint64_t> m_amounts;
  uint64_t m_outsCount;
  std::shared_ptr<SendTransactionContext> m_context;
  Callback m_cb;
};

class WalletRelayTransactionRequest: public WalletRequest
{
public:
  WalletRelayTransactionRequest(const CryptoNote::Transaction& tx, Callback cb) : m_tx(tx), m_cb(cb) {};
  virtual ~WalletRelayTransactionRequest() {};

  virtual void perform(INode& node, std::function<void (WalletRequest::Callback, std::error_code)> cb) override
  {
    node.relayTransaction(m_tx, std::bind(cb, m_cb, std::placeholders::_1));
  }

private:
  CryptoNote::Transaction m_tx;
  Callback m_cb;
};

} //namespace CryptoNote
