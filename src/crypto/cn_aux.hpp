// Copyright (c) 2019 fireice-uk
// Copyright (c) 2019 The Circle Foundation
// Copyright (c) 2020, The Karbo Developers
//
// Distributed under the MIT/X11 software license, see the accompanying
// file COPYING or http://www.opensource.org/licenses/mit-license.php.

#pragma once
#include "soft_aes.hpp"

#if defined(_WIN32) || defined(_WIN64)
#include <intrin.h>
#include <malloc.h>
#define HAS_WIN_INTRIN_API
#endif

#if defined(__ARM_FEATURE_SIMD32) || defined(__ARM_NEON)
#include "sse2neon.h"
#endif

#if !defined(_LP64) && !defined(_WIN64) && !defined(ARM)
inline uint64_t _umul128(uint64_t multiplier, uint64_t multiplicand, uint64_t* product_hi)
{
  // multiplier   = ab = a * 2^32 + b
  // multiplicand = cd = c * 2^32 + d
  // ab * cd = a * c * 2^64 + (a * d + b * c) * 2^32 + b * d
  uint64_t a = multiplier >> 32;
  uint64_t b = multiplier & 0xFFFFFFFF;
  uint64_t c = multiplicand >> 32;
  uint64_t d = multiplicand & 0xFFFFFFFF;

  uint64_t ac = a * c;
  uint64_t ad = a * d;
  uint64_t bc = b * c;
  uint64_t bd = b * d;

  uint64_t adbc = ad + bc;
  uint64_t adbc_carry = adbc < ad ? 1 : 0;

  // multiplier * multiplicand = product_hi * 2^64 + product_lo
  uint64_t product_lo = bd + (adbc << 32);
  uint64_t product_lo_carry = product_lo < bd ? 1 : 0;
  *product_hi = ac + (adbc >> 32) + (adbc_carry << 32) + product_lo_carry;

  return product_lo;
}
#else
#if defined(__GNUC__) && !defined(ARM)
#pragma GCC target ("aes,sse2")
#include <x86intrin.h>
#endif
#if !defined(HAS_WIN_INTRIN_API) && !defined(ARM)
#include <cpuid.h>
inline uint64_t _umul128(uint64_t a, uint64_t b, uint64_t* hi)
{
  unsigned __int128 r = (unsigned __int128)a * (unsigned __int128)b;
  *hi = r >> 64;
  return (uint64_t)r;
}
#endif
#endif

inline void cpuid(uint32_t eax, int32_t ecx, int32_t val[4])
{
	val[0] = 0;
	val[1] = 0;
	val[2] = 0;
	val[3] = 0;

#if defined(HAS_WIN_INTRIN_API)
	__cpuidex(val, eax, ecx);
#else
	__cpuid_count(eax, ecx, val[0], val[1], val[2], val[3]);
#endif
}

inline bool hw_check_aes()
{
#if defined(ARM)
  return true; // just use soft on ARM
#endif
	int32_t cpu_info[4];
	cpuid(1, 0, cpu_info);
	return (cpu_info[2] & (1 << 25)) == 0;
}

// This will shift and xor tmp1 into itself as 4 32-bit vals such as
// sl_xor(a1 a2 a3 a4) = a1 (a2^a1) (a3^a2^a1) (a4^a3^a2^a1)
inline __m128i sl_xor(__m128i tmp1)
{
	__m128i tmp4;
	tmp4 = _mm_slli_si128(tmp1, 0x04);
	tmp1 = _mm_xor_si128(tmp1, tmp4);
	tmp4 = _mm_slli_si128(tmp4, 0x04);
	tmp1 = _mm_xor_si128(tmp1, tmp4);
	tmp4 = _mm_slli_si128(tmp4, 0x04);
	tmp1 = _mm_xor_si128(tmp1, tmp4);
	return tmp1;
}

template<bool SOFT_AES>
inline void aes_round(__m128i key, __m128i& x0, __m128i& x1, __m128i& x2, __m128i& x3, __m128i& x4, __m128i& x5, __m128i& x6, __m128i& x7)
{
	if(SOFT_AES)
	{
		x0 = soft_aesenc(x0, key);
		x1 = soft_aesenc(x1, key);
		x2 = soft_aesenc(x2, key);
		x3 = soft_aesenc(x3, key);
		x4 = soft_aesenc(x4, key);
		x5 = soft_aesenc(x5, key);
		x6 = soft_aesenc(x6, key);
		x7 = soft_aesenc(x7, key);
	}
	else
	{
#if !defined(ARM)
		x0 = _mm_aesenc_si128(x0, key);
		x1 = _mm_aesenc_si128(x1, key);
		x2 = _mm_aesenc_si128(x2, key);
		x3 = _mm_aesenc_si128(x3, key);
		x4 = _mm_aesenc_si128(x4, key);
		x5 = _mm_aesenc_si128(x5, key);
		x6 = _mm_aesenc_si128(x6, key);
		x7 = _mm_aesenc_si128(x7, key);
#endif
	}
}
