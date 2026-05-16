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

#include "CryptoNoteCore/CryptoNoteBasic.h"

namespace CryptoNote {

  struct BlockInfo {
    uint32_t height;
    Crypto::Hash id;

    BlockInfo() {
      clear();
    }

    void clear() {
      height = 0;
      id = CryptoNote::NULL_HASH;
    }

    bool empty() const {
      return id == CryptoNote::NULL_HASH;
    }
  };

  // Where a transaction is being validated from. Lets the validator skip
  // expensive cryptographic checks (Triptych, GK proofs, balance kernel, ring
  // resolution) for transactions delivered inside an already-trusted
  // checkpointed block, while still running cheap structural sanity checks.
  enum class TxValidationContext {
    Mempool,            // Untrusted broadcast tx; full validation.
    Block,              // Tx is part of a block outside the checkpoint zone; full validation.
    CheckpointedBlock,  // Tx is part of a block whose hash is covered by a confirmed checkpoint;
                        // skip expensive crypto, keep structural + double-spend checks.
  };

  class ITransactionValidator {
  public:
    virtual ~ITransactionValidator() {}

    virtual bool checkTransactionInputs(const CryptoNote::Transaction& tx, BlockInfo& maxUsedBlock,
                                        TxValidationContext context) = 0;
    virtual bool checkTransactionInputs(const CryptoNote::Transaction& tx, BlockInfo& maxUsedBlock,
                                        BlockInfo& lastFailed, TxValidationContext context) = 0;
    virtual bool haveSpentKeyImages(const CryptoNote::Transaction& tx) = 0;
    virtual bool checkTransactionSize(size_t blobSize) = 0;
  };

}
