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

#include "TransactionUtils.h"

#include <cstring>
#include <unordered_set>

#include "crypto/crypto.h"
#include "crypto/ct_ecdh.h"
#include "CryptoNoteCore/Account.h"
#include "CryptoNoteFormatUtils.h"
#include "TransactionExtra.h"

using namespace Crypto;

namespace CryptoNote {

bool checkInputsKeyimagesDiff(const CryptoNote::TransactionPrefix& tx) {
  std::unordered_set<Crypto::KeyImage> ki;
  for (const auto& in : tx.inputs) {
    if (in.type() == typeid(KeyInput)) {
      if (!ki.insert(boost::get<KeyInput>(in).keyImage).second)
        return false;
    }
  }
  return true;
}

// TransactionInput helper functions

size_t getRequiredSignaturesCount(const TransactionInput& in) {
  if (in.type() == typeid(KeyInput)) {
    return boost::get<KeyInput>(in).outputIndexes.size();
  }
  return 0;
}

uint64_t getTransactionInputAmount(const TransactionInput& in) {
  if (in.type() == typeid(KeyInput)) {
    return boost::get<KeyInput>(in).amount;
  }
  return 0;
}

TransactionTypes::InputType getTransactionInputType(const TransactionInput& in) {
  if (in.type() == typeid(KeyInput)) {
    return TransactionTypes::InputType::Key;
  }
  if (in.type() == typeid(BaseInput)) {
    return TransactionTypes::InputType::Generating;
  }
  if (in.type() == typeid(ConfidentialInput)) {
    return TransactionTypes::InputType::Confidential;
  }
  return TransactionTypes::InputType::Invalid;
}

const TransactionInput& getInputChecked(const CryptoNote::TransactionPrefix& transaction, size_t index) {
  if (transaction.inputs.size() <= index) {
    throw std::runtime_error("Transaction input index out of range");
  }
  return transaction.inputs[index];
}

const TransactionInput& getInputChecked(const CryptoNote::TransactionPrefix& transaction, size_t index, TransactionTypes::InputType type) {
  const auto& input = getInputChecked(transaction, index);
  if (getTransactionInputType(input) != type) {
    throw std::runtime_error("Unexpected transaction input type");
  }
  return input;
}

// TransactionOutput helper functions

TransactionTypes::OutputType getTransactionOutputType(const TransactionOutputTarget& out) {
  if (out.type() == typeid(KeyOutput)) {
    return TransactionTypes::OutputType::Key;
  }
  if (out.type() == typeid(ConfidentialOutput)) {
    return TransactionTypes::OutputType::Confidential;
  }
  return TransactionTypes::OutputType::Invalid;
}

const TransactionOutput& getOutputChecked(const CryptoNote::TransactionPrefix& transaction, size_t index) {
  if (transaction.outputs.size() <= index) {
    throw std::runtime_error("Transaction output index out of range");
  }
  return transaction.outputs[index];
}

const TransactionOutput& getOutputChecked(const CryptoNote::TransactionPrefix& transaction, size_t index, TransactionTypes::OutputType type) {
  const auto& output = getOutputChecked(transaction, index);
  if (getTransactionOutputType(output.target) != type) {
    throw std::runtime_error("Unexpected transaction output target type");
  }
  return output;
}

bool isOutToKey(const Crypto::PublicKey& spendPublicKey, const Crypto::PublicKey& outKey, const Crypto::KeyDerivation& derivation, size_t keyIndex) {
  Crypto::PublicKey pk;
  derive_public_key(derivation, keyIndex, spendPublicKey, pk);
  return pk == outKey;
}

bool findOutputsToAccount(const CryptoNote::TransactionPrefix& transaction, const AccountPublicAddress& addr,
                          const SecretKey& viewSecretKey, std::vector<uint32_t>& out, uint64_t& amount) {
  AccountKeys keys;
  keys.address = addr;
  // only view secret key is used, spend key is not needed
  keys.viewSecretKey = viewSecretKey;

  Crypto::PublicKey txPubKey = getTransactionPublicKeyFromExtra(transaction.extra);

  amount = 0;
  size_t keyIndex = 0;
  uint32_t outputIndex = 0;

  Crypto::KeyDerivation derivation;
  generate_key_derivation(txPubKey, keys.viewSecretKey, derivation);

  for (const TransactionOutput& o : transaction.outputs) {
    if (o.target.type() == typeid(KeyOutput)) {
      if (is_out_to_acc(keys, boost::get<KeyOutput>(o.target), derivation, keyIndex)) {
        out.push_back(outputIndex);
        amount += o.amount;
      }
      ++keyIndex;
    } else if (o.target.type() == typeid(ConfidentialOutput)) {
      // CT outputs use the same stealth-address scheme as transparent KeyOutput
      // (targetKey = Hs(8aR||idx)*G + B). Holders of the view secret key can
      // detect ownership via ECDH and recover the hidden amount via the masked-amount
      // ECDH unmask + commitment check. This lets node operators with their view-secret
      // configured (e.g. masternode-fee enforcement) check CT-paid fees correctly.
      const ConfidentialOutput& ctOut = boost::get<ConfidentialOutput>(o.target);
      if (isOutToKey(addr.spendPublicKey, ctOut.targetKey, derivation, keyIndex)) {
        Crypto::MaskedAmount masked;
        std::memcpy(masked.data, ctOut.maskedAmount.data(), sizeof(masked.data));
        const Crypto::PublicKey& commitmentPK =
            reinterpret_cast<const Crypto::PublicKey&>(ctOut.commitment);
        uint64_t recoveredAmount = 0;
        Crypto::EllipticCurveScalar blindingFactor;
        if (Crypto::decrypt_and_verify_output(viewSecretKey, txPubKey, keyIndex,
                                               masked, commitmentPK,
                                               recoveredAmount, blindingFactor)) {
          out.push_back(outputIndex);
          amount += recoveredAmount;
        }
      }
      ++keyIndex;
    }
    ++outputIndex;
  }

  return true;
}

}
