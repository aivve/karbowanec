// Copyright (c) 2018-2026, Karbo developers
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

#include <algorithm>
#include <array>
#include <cstdint>
#include <stdexcept>
#include <vector>

namespace CryptoNote {

// Canonical CT denomination set in current Karbo atomic units (1 KRB = 10^12 au).
// Floor = 10^10 au (= 0.01 KRB) so confidential dust is structurally impossible:
// any value below the floor cannot be expressed as a CT output and must remain
// transparent or be absorbed into fee.
// 7 decades of 9 entries each (1-9 × 10^10 through 1-9 × 10^16) plus the
// 100,000 KRB cap (10^17 au) = 64 entries, sorted ascending.
static constexpr size_t DENOMINATION_COUNT = 64;

static constexpr std::array<uint64_t, DENOMINATION_COUNT> DENOMINATIONS = {{
  // 0.01 .. 0.09 KRB
  UINT64_C(10000000000),    UINT64_C(20000000000),    UINT64_C(30000000000),
  UINT64_C(40000000000),    UINT64_C(50000000000),    UINT64_C(60000000000),
  UINT64_C(70000000000),    UINT64_C(80000000000),    UINT64_C(90000000000),
  // 0.1 .. 0.9 KRB
  UINT64_C(100000000000),   UINT64_C(200000000000),   UINT64_C(300000000000),
  UINT64_C(400000000000),   UINT64_C(500000000000),   UINT64_C(600000000000),
  UINT64_C(700000000000),   UINT64_C(800000000000),   UINT64_C(900000000000),
  // 1 .. 9 KRB
  UINT64_C(1000000000000),  UINT64_C(2000000000000),  UINT64_C(3000000000000),
  UINT64_C(4000000000000),  UINT64_C(5000000000000),  UINT64_C(6000000000000),
  UINT64_C(7000000000000),  UINT64_C(8000000000000),  UINT64_C(9000000000000),
  // 10 .. 90 KRB
  UINT64_C(10000000000000), UINT64_C(20000000000000), UINT64_C(30000000000000),
  UINT64_C(40000000000000), UINT64_C(50000000000000), UINT64_C(60000000000000),
  UINT64_C(70000000000000), UINT64_C(80000000000000), UINT64_C(90000000000000),
  // 100 .. 900 KRB
  UINT64_C(100000000000000), UINT64_C(200000000000000), UINT64_C(300000000000000),
  UINT64_C(400000000000000), UINT64_C(500000000000000), UINT64_C(600000000000000),
  UINT64_C(700000000000000), UINT64_C(800000000000000), UINT64_C(900000000000000),
  // 1,000 .. 9,000 KRB
  UINT64_C(1000000000000000), UINT64_C(2000000000000000), UINT64_C(3000000000000000),
  UINT64_C(4000000000000000), UINT64_C(5000000000000000), UINT64_C(6000000000000000),
  UINT64_C(7000000000000000), UINT64_C(8000000000000000), UINT64_C(9000000000000000),
  // 10,000 .. 90,000 KRB
  UINT64_C(10000000000000000), UINT64_C(20000000000000000), UINT64_C(30000000000000000),
  UINT64_C(40000000000000000), UINT64_C(50000000000000000), UINT64_C(60000000000000000),
  UINT64_C(70000000000000000), UINT64_C(80000000000000000), UINT64_C(90000000000000000),
  // 100,000 KRB cap
  UINT64_C(100000000000000000)
}};

// Smallest CT denomination. Sub-floor amounts cannot become CT outputs;
// they remain transparent or are absorbed into transaction fees.
static constexpr uint64_t MIN_CT_DENOMINATION = DENOMINATIONS[0];

// Returns true if amount is one of the 64 canonical denominations.
inline bool isCanonicalDenomination(uint64_t amount) {
  auto it = std::lower_bound(DENOMINATIONS.begin(), DENOMINATIONS.end(), amount);
  return it != DENOMINATIONS.end() && *it == amount;
}

// Returns the index of amount in DENOMINATIONS [0..63], or -1 if not found.
inline int denominationIndex(uint64_t amount) {
  auto it = std::lower_bound(DENOMINATIONS.begin(), DENOMINATIONS.end(), amount);
  if (it != DENOMINATIONS.end() && *it == amount) {
    return static_cast<int>(std::distance(DENOMINATIONS.begin(), it));
  }
  return -1;
}

// Greedy decomposition of amount into canonical denominations (descending).
// Returns the list of denomination values whose sum equals amount.
// Throws std::invalid_argument if amount is 0 or not exactly representable.
inline std::vector<uint64_t> decomposeAmount(uint64_t amount) {
  if (amount == 0) {
    throw std::invalid_argument("Cannot decompose zero amount");
  }

  std::vector<uint64_t> result;
  uint64_t remaining = amount;

  // Iterate denominations from largest to smallest
  for (int i = static_cast<int>(DENOMINATION_COUNT) - 1; i >= 0 && remaining > 0; --i) {
    uint64_t denom = DENOMINATIONS[static_cast<size_t>(i)];
    while (remaining >= denom) {
      result.push_back(denom);
      remaining -= denom;
    }
  }

  if (remaining != 0) {
    throw std::invalid_argument("Amount is not exactly representable with canonical denominations");
  }

  return result;
}

} // namespace CryptoNote
