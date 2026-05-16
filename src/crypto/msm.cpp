// Copyright (c) 2016-2026, The Karbo developers
//
// Implementation of msm.h. See the header for the public contract;
// this file holds the Pippenger and naïve sum-is-identity routines
// and the small ge_p3 helpers they rely on (kept private here so the
// rest of crypto/ doesn't have to take a dependency on the MSM
// internals).

#include "msm.h"

#include <cstring>
#include <vector>

namespace Crypto {

namespace {

// ── Small ge_p3 helpers (private to this TU) ────────────────────────────

void msm_point_identity(ge_p3* out) {
  static const unsigned char identity_bytes[32] = {
    0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00
  };
  ge_frombytes_vartime(out, identity_bytes);
}

void msm_point_add(ge_p3* out, const ge_p3* a, const ge_p3* b) {
  ge_cached b_cached;
  ge_p3_to_cached(&b_cached, b);
  ge_p1p1 r;
  ge_add(&r, a, &b_cached);
  ge_p1p1_to_p3(out, &r);
}

bool msm_scalarmult(ge_p3* out, const unsigned char s[32], const ge_p3* P) {
  ge_p2 r;
  ge_scalarmult(&r, s, P);
  unsigned char bytes[32];
  ge_tobytes(bytes, &r);
  return ge_frombytes_vartime(out, bytes) == 0;
}

// Doubling for ge_p3 → ge_p3. ref10's ge_p3_dbl is file-static; we
// replicate the same two-step (p3→p2→p2_dbl→p1p1→p3) here. Used heavily
// in the Pippenger window-combine phase.
void p3_dbl(ge_p3* r, const ge_p3* p) {
  ge_p2 q;
  ge_p3_to_p2(&q, p);
  ge_p1p1 t;
  ge_p2_dbl(&t, &q);
  ge_p1p1_to_p3(r, &t);
}

// Add ge_cached q to ge_p3 p, store in r. Saves the per-call
// ge_p3_to_cached conversion when the same input point is added into
// many buckets across many windows.
void p3_add_cached(ge_p3* r, const ge_p3* p, const ge_cached* q) {
  ge_p1p1 t;
  ge_add(&t, p, q);
  ge_p1p1_to_p3(r, &t);
}

// Windowed Pippenger multi-scalar multiplication.
//
//   result = Σ_{i=0..n-1} scalars[i] · points[i]
//
// c=4 bit window (16 buckets per window, 64 windows over 256 bits).
// Bucket sums use the standard running-sum trick:
//   Σ_d d·bucket[d]  =  Σ_d (Σ_{d'≥d} bucket[d'])
// computed in O(2^c) adds per window instead of O((2^c)^2).
//
// Caller must ensure points are valid prime-subgroup points. Scalars
// don't need to be reduced; the bit extraction below only looks at the
// low 256 bits.
//
// Performance characteristics (single thread, ref10):
//   - Constant per-window overhead: ~30 adds + 15 bucket inits + 4 doublings
//   - Per-input cost: ~64 adds (one per window) amortized
//   - Breakeven vs naïve seq scalarmult: n ≈ 20-30 inputs
//   - Asymptotic speedup over n sequential ge_scalarmult calls: ~3-5×
//     for n in the few-hundreds-to-thousands range.
void ge_msm_pippenger(ge_p3& result, const std::vector<MSMTerm>& terms) {
  // Window c=4. Theoretical cost model 256N/c + 2^c · 256/c suggests
  // c=5 or c=6 should be faster for our batch sizes (~1000-8000 terms),
  // but c=4 is empirically the fastest with ref10's primitives:
  //   * c=4 windows align to byte nibbles → no cross-byte bit extraction
  //   * fewer buckets keeps the running-sum loop hot in L1/L2 cache
  //   * larger c also costs more in the per-window bucket-init pass
  // Measured: c=5 was ~1% slower than c=4 at all sizes we care about.
  constexpr int c = 4;
  constexpr int num_buckets = 1 << c;
  constexpr int num_windows = (256 + c - 1) / c;

  if (terms.empty()) {
    msm_point_identity(&result);
    return;
  }

  const size_t n = terms.size();

  std::vector<ge_cached> cached(n);
  for (size_t i = 0; i < n; ++i) {
    ge_p3_to_cached(&cached[i], &terms[i].point);
  }

  ge_p3 buckets[num_buckets];

  msm_point_identity(&result);

  for (int w = num_windows - 1; w >= 0; --w) {
    const int bit_pos = w * c;
    const int byte_pos = bit_pos / 8;
    const int shift = bit_pos % 8;
    const unsigned int mask = (1u << c) - 1u;

    for (int d = 1; d < num_buckets; ++d) msm_point_identity(&buckets[d]);

    for (size_t i = 0; i < n; ++i) {
      unsigned int bits = static_cast<unsigned int>(terms[i].scalar.data[byte_pos]);
      if (byte_pos + 1 < 32) {
        bits |= static_cast<unsigned int>(terms[i].scalar.data[byte_pos + 1]) << 8;
      }
      const unsigned int digit = (bits >> shift) & mask;
      if (digit != 0) {
        p3_add_cached(&buckets[digit], &buckets[digit], &cached[i]);
      }
    }

    ge_p3 running, window_sum;
    msm_point_identity(&running);
    msm_point_identity(&window_sum);
    for (int d = num_buckets - 1; d >= 1; --d) {
      msm_point_add(&running, &running, &buckets[d]);
      msm_point_add(&window_sum, &window_sum, &running);
    }

    if (w < num_windows - 1) {
      for (int k = 0; k < c; ++k) {
        ge_p3 doubled;
        p3_dbl(&doubled, &result);
        result = doubled;
      }
    }
    msm_point_add(&result, &result, &window_sum);
  }
}

// Encoded identity (x=0, y=1).
const unsigned char k_identity_bytes[32] = {
  0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
  0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
  0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
  0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00
};

} // anonymous namespace

bool msm_naive_sum_is_identity(const std::vector<MSMTerm>& terms,
                               const unsigned char extraG[32],
                               const unsigned char extraH[32],
                               const ge_p3& H_p3) {
  ge_p3 acc;
  msm_point_identity(&acc);

  for (const auto& term : terms) {
    ge_p3 scaled;
    if (!msm_scalarmult(&scaled, term.scalar.data, &term.point)) return false;
    msm_point_add(&acc, &acc, &scaled);
  }

  if (sc_isnonzero(extraG)) {
    ge_p3 gG;
    ge_scalarmult_base(&gG, extraG);
    msm_point_add(&acc, &acc, &gG);
  }
  if (sc_isnonzero(extraH)) {
    ge_p3 gH;
    if (!msm_scalarmult(&gH, extraH, &H_p3)) return false;
    msm_point_add(&acc, &acc, &gH);
  }

  unsigned char acc_bytes[32];
  ge_p3_tobytes(acc_bytes, &acc);
  return std::memcmp(acc_bytes, k_identity_bytes, 32) == 0;
}

bool msm_pippenger_sum_is_identity(const std::vector<MSMTerm>& terms,
                                   const unsigned char extraG[32],
                                   const unsigned char extraH[32],
                                   const ge_p3& H_p3) {
  std::vector<MSMTerm> all = terms;
  all.reserve(terms.size() + 2);

  if (sc_isnonzero(extraG)) {
    MSMTerm gTerm;
    std::memcpy(gTerm.scalar.data, extraG, 32);
    // ge_p3 form of the basepoint G via ge_scalarmult_base(scalar=1).
    unsigned char one[32];
    sc_0(one);
    one[0] = 1;
    ge_scalarmult_base(&gTerm.point, one);
    all.push_back(gTerm);
  }
  if (sc_isnonzero(extraH)) {
    MSMTerm hTerm;
    std::memcpy(hTerm.scalar.data, extraH, 32);
    hTerm.point = H_p3;
    all.push_back(hTerm);
  }

  ge_p3 sum;
  ge_msm_pippenger(sum, all);

  unsigned char sum_bytes[32];
  ge_p3_tobytes(sum_bytes, &sum);
  return std::memcmp(sum_bytes, k_identity_bytes, 32) == 0;
}

} // namespace Crypto
