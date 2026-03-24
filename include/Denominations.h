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

// Canonical denomination set for post-redenomination Karbo (1 KRB = 100 au).
// 7 decades of 9 entries each (1-9 × 10^0 through 1-9 × 10^6) plus the
// 100,000 KRB cap (10,000,000 au) = 64 entries, sorted ascending.
static constexpr size_t DENOMINATION_COUNT = 64;

static constexpr std::array<uint64_t, DENOMINATION_COUNT> DENOMINATIONS = {{
  // 1 .. 9
  1, 2, 3, 4, 5, 6, 7, 8, 9,
  // 10 .. 90
  10, 20, 30, 40, 50, 60, 70, 80, 90,
  // 100 .. 900
  100, 200, 300, 400, 500, 600, 700, 800, 900,
  // 1,000 .. 9,000
  1000, 2000, 3000, 4000, 5000, 6000, 7000, 8000, 9000,
  // 10,000 .. 90,000
  10000, 20000, 30000, 40000, 50000, 60000, 70000, 80000, 90000,
  // 100,000 .. 900,000
  100000, 200000, 300000, 400000, 500000, 600000, 700000, 800000, 900000,
  // 1,000,000 .. 9,000,000
  1000000, 2000000, 3000000, 4000000, 5000000, 6000000, 7000000, 8000000, 9000000,
  // 10,000,000 (= 100,000 KRB cap)
  10000000
}};

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
