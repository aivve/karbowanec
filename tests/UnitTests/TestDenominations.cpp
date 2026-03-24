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

#include "gtest/gtest.h"

#include <algorithm>
#include <cstdint>
#include <numeric>
#include <vector>

#include "Denominations.h"

namespace {

// --- Denomination table sanity ---

TEST(Denominations, TableHas64Entries) {
  ASSERT_EQ(CryptoNote::DENOMINATION_COUNT, 64u);
  ASSERT_EQ(CryptoNote::DENOMINATIONS.size(), 64u);
}

TEST(Denominations, TableIsSortedAscending) {
  for (size_t i = 1; i < CryptoNote::DENOMINATIONS.size(); ++i) {
    EXPECT_LT(CryptoNote::DENOMINATIONS[i - 1], CryptoNote::DENOMINATIONS[i])
      << "at index " << i;
  }
}

TEST(Denominations, FirstIsOneLastIsTenMillion) {
  EXPECT_EQ(CryptoNote::DENOMINATIONS.front(), 1u);
  EXPECT_EQ(CryptoNote::DENOMINATIONS.back(), 10000000u);
}

// --- isCanonicalDenomination ---

TEST(Denominations, AllTableEntriesAreCanonical) {
  for (auto d : CryptoNote::DENOMINATIONS) {
    EXPECT_TRUE(CryptoNote::isCanonicalDenomination(d)) << d;
  }
}

TEST(Denominations, NonCanonicalAmountsRejected) {
  EXPECT_FALSE(CryptoNote::isCanonicalDenomination(0));
  EXPECT_FALSE(CryptoNote::isCanonicalDenomination(11));
  EXPECT_FALSE(CryptoNote::isCanonicalDenomination(15));
  EXPECT_FALSE(CryptoNote::isCanonicalDenomination(99));
  EXPECT_FALSE(CryptoNote::isCanonicalDenomination(101));
  EXPECT_FALSE(CryptoNote::isCanonicalDenomination(10000001));
  EXPECT_FALSE(CryptoNote::isCanonicalDenomination(20000000));
}

// --- denominationIndex ---

TEST(Denominations, IndexOfFirstAndLast) {
  EXPECT_EQ(CryptoNote::denominationIndex(1), 0);
  EXPECT_EQ(CryptoNote::denominationIndex(10000000), 63);
}

TEST(Denominations, IndexOfKnownValues) {
  EXPECT_EQ(CryptoNote::denominationIndex(5), 4);
  EXPECT_EQ(CryptoNote::denominationIndex(50), 13);
  EXPECT_EQ(CryptoNote::denominationIndex(500), 22);
  EXPECT_EQ(CryptoNote::denominationIndex(5000000), 58);
}

TEST(Denominations, IndexOfNonCanonicalReturnsMinusOne) {
  EXPECT_EQ(CryptoNote::denominationIndex(0), -1);
  EXPECT_EQ(CryptoNote::denominationIndex(11), -1);
  EXPECT_EQ(CryptoNote::denominationIndex(20000000), -1);
}

// --- decomposeAmount ---

TEST(Denominations, DecomposeMinimum) {
  auto v = CryptoNote::decomposeAmount(1);
  ASSERT_EQ(v.size(), 1u);
  EXPECT_EQ(v[0], 1u);
}

TEST(Denominations, DecomposeMaxSingle) {
  auto v = CryptoNote::decomposeAmount(10000000);
  ASSERT_EQ(v.size(), 1u);
  EXPECT_EQ(v[0], 10000000u);
}

TEST(Denominations, DecomposeEleven) {
  // 11 = 10 + 1
  auto v = CryptoNote::decomposeAmount(11);
  ASSERT_EQ(v.size(), 2u);
  EXPECT_EQ(v[0], 10u);
  EXPECT_EQ(v[1], 1u);

  uint64_t sum = 0;
  for (auto d : v) sum += d;
  EXPECT_EQ(sum, 11u);
}

TEST(Denominations, DecomposeComplex) {
  // 12345678 = 10000000 + 2000000 + 300000 + 40000 + 5000 + 600 + 70 + 8
  auto v = CryptoNote::decomposeAmount(12345678);
  ASSERT_EQ(v.size(), 8u);

  std::vector<uint64_t> expected = {10000000, 2000000, 300000, 40000, 5000, 600, 70, 8};
  EXPECT_EQ(v, expected);

  uint64_t sum = std::accumulate(v.begin(), v.end(), uint64_t{0});
  EXPECT_EQ(sum, 12345678u);
}

TEST(Denominations, DecomposeZeroThrows) {
  EXPECT_THROW(CryptoNote::decomposeAmount(0), std::invalid_argument);
}

TEST(Denominations, DecomposeAllSameDigit) {
  // 9999999 = 9000000 + 900000 + 90000 + 9000 + 900 + 90 + 9
  auto v = CryptoNote::decomposeAmount(9999999);
  ASSERT_EQ(v.size(), 7u);

  std::vector<uint64_t> expected = {9000000, 900000, 90000, 9000, 900, 90, 9};
  EXPECT_EQ(v, expected);

  uint64_t sum = std::accumulate(v.begin(), v.end(), uint64_t{0});
  EXPECT_EQ(sum, 9999999u);
}

TEST(Denominations, DecomposeAllDenominationsAreCanonical) {
  auto v = CryptoNote::decomposeAmount(12345678);
  for (auto d : v) {
    EXPECT_TRUE(CryptoNote::isCanonicalDenomination(d)) << d;
  }
}

TEST(Denominations, DecomposeLargeMultipleOfMax) {
  // 30000000 = 10000000 * 3
  auto v = CryptoNote::decomposeAmount(30000000);
  ASSERT_EQ(v.size(), 3u);
  for (auto d : v) {
    EXPECT_EQ(d, 10000000u);
  }
}

} // anonymous namespace
