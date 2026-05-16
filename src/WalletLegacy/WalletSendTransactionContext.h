// Copyright (c) 2012-2016, The CryptoNote developers, The Bytecoin developers
// Copyright (c) 2018, Karbo developers
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

#include <list>
#include <vector>

#include "CryptoNoteCore/CryptoNoteBasic.h"
#include "IWalletLegacy.h"
#include "ITransfersContainer.h"

namespace CryptoNote {

struct TxDustPolicy
{
  uint64_t dustThreshold;
  bool addToFee;
  CryptoNote::AccountPublicAddress addrForDust;

  TxDustPolicy(uint64_t a_dust_threshold = 0, bool an_add_to_fee = true, CryptoNote::AccountPublicAddress an_addr_for_dust = CryptoNote::AccountPublicAddress())
    : dustThreshold(a_dust_threshold), addToFee(an_add_to_fee), addrForDust(an_addr_for_dust) {}
};

struct SendTransactionContext
{
  TransactionId transactionId;
  std::vector<CryptoNote::COMMAND_RPC_GET_RANDOM_OUTPUTS_FOR_AMOUNTS::outs_for_amount> outs;
  uint64_t foundMoney;
  std::list<TransactionOutputInformation> selectedTransfers;
  TxDustPolicy dustPolicy;
  uint64_t mixIn;
  std::vector<uint64_t> inputMixins;
  // Cross-bucket mixed-ring sampling state. `mixingBuckets[i]` is the
  // alternative bucket for CT input i (0 if no mixing). `mixingOuts[i]`
  // is the daemon's per-input mixing decoy response, parallel to
  // selectedTransfers. Both populated only on the CT path.
  //
  // The daemon's getRandomOutsByAmounts rejects amount=0 and inputs that
  // skip cross-bucket mixing leave mixingBuckets[i]=0, so the request must
  // be filtered before going on the wire. mixingOriginalIndex maps each
  // entry of the filtered request back to its slot in selectedTransfers;
  // mixingOutsRaw holds the daemon's raw response (filtered shape) until
  // the callback expands it back into mixingOuts.
  std::vector<uint64_t> mixingBuckets;
  std::vector<CryptoNote::COMMAND_RPC_GET_RANDOM_OUTPUTS_FOR_AMOUNTS::outs_for_amount> mixingOuts;
  std::vector<CryptoNote::COMMAND_RPC_GET_RANDOM_OUTPUTS_FOR_AMOUNTS::outs_for_amount> mixingOutsRaw;
  std::vector<size_t> mixingOriginalIndex;
  Crypto::SecretKey tx_key = NULL_SECRET_KEY;
};

} //namespace CryptoNote
