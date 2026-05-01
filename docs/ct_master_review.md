# Confidential Transactions (Pubkey-Referenced Rings) Review vs Master

Date: 2026-05-01
Branch under review: `work`
Head reviewed: `89b1950` (feature) + `154c2bb` (review artifact)
Master baseline used: merge point `80c2bac` (explicit `Merge branch 'master' into dev/ct_15` in history).

## Executive summary

- **Architecture direction is correct**: ring-member references by output pubkey are significantly more reorg-robust than global index references.
- **Consensus safety posture is good**: block validation is chain-only (`allowMempool=false`), so mempool state cannot alter consensus decisions.
- **Critical hardening needed (now implemented in this branch)**:
  1. reject mempool pubkey-index collisions instead of silently overwriting,
  2. widen mempool output index type from `uint16_t` to `uint32_t`.
- **Remaining requirement before production**: targeted regression/integration tests for mempool chaining, cascade eviction, reorg transitions, and collision handling.

## Review method and coverage

The following implementation areas were reviewed end-to-end:

1. **Transaction model and serialization**
   - `ConfidentialInput` schema and wire encoding for pubkey-based ring members.
2. **Core/consensus validation**
   - ring invariants, binding of pubkey/commitment pairs, and signature sizing checks.
3. **Blockchain storage/indexing**
   - LMDB pubkey index lifecycle: insert, lookup, rollback/remove.
4. **Mempool policy and dependency handling**
   - pubkey fallback resolution, ancestor handling, and cascade removals.
5. **Wallet construction/signing path**
   - ring sorting and `realIndex` remapping before CT signing.
6. **Explorer/RPC-facing CT fields**
   - mixin and CT visibility touchpoints impacted by ring representation changes.

## Findings

### A) Consensus and chain validation: strong

- `ConfidentialInput` ring members are now pubkey-referenced and commitments are aligned by index.
- Consensus checks validate ring non-emptiness, size consistency, and strict lexicographic ordering of pubkeys.
- Signature vector sizing is validated against ring size.
- Block-path CT validation remains chain-only (no mempool fallback), preserving deterministic consensus behavior.

### B) Storage/indexing: strong with clear rollback model

- LMDB pubkey index (`output_pubkey_idx`) provides stable output identity independent of global positional indexes.
- Connect/disconnect paths include insertion/removal hooks for pubkey index entries.

### C) Mempool fallback/chaining: good design, two concrete hardening issues were present

#### C1) Collision policy in mempool pubkey index (**fixed in this branch**)

Previous behavior: “last writer wins” overwrite in `m_pubkey_to_output`.

Risk: if duplicate output pubkeys appear in pool state, a dependent tx could resolve to wrong parent output.

Fix implemented:
- Duplicate pubkey inside same tx => reject.
- Duplicate pubkey already indexed by different tx => reject.
- On rejection, pool insertion is rolled back (spent-key-image insertions and tx indices removed).

#### C2) `uint16_t` output index in mempool lookup (**fixed in this branch**)

Previous behavior: mempool output index was stored and returned as `uint16_t`.

Risk: potential truncation boundary at >65535 outputs.

Fix implemented:
- Mempool pubkey index now stores `uint32_t` output index.
- Lookup APIs updated accordingly.
- CT resolver uses separate mempool index variable (`uint32_t`) and safe size checks.

## Files changed for fixes

- `src/CryptoNoteCore/TransactionPool.h`
- `src/CryptoNoteCore/TransactionPool.cpp`
- `src/CryptoNoteCore/Blockchain.cpp`

## Production readiness

Current status after fixes:
- **Consensus path**: acceptable.
- **Mempool chaining policy**: materially improved and safer.
- **Not yet production-ready until tests below pass**.

## Required test plan before production

1. **Mempool chain resolution**: A->B where B references A outputs via ring pubkeys.
2. **Cascade eviction**: remove A and ensure B/C descendants are evicted in dependency order.
3. **Parent mined transition**: mine A while B remains in pool; B revalidates via chain index.
4. **Reorg cycle**: A mined -> reorg out -> back in; ensure resolver/index remains consistent.
5. **Collision rejection**: inject or craft duplicate output pubkey attempts; verify deterministic rejection and rollback.
6. **Boundary index test**: stress output indexing behavior with large output counts (policy/limits permitting).

## Final verdict

The CT pubkey-referenced ring migration is directionally sound and addresses the core reorg fragility of index-referenced rings. With the implemented mempool collision/type fixes and the required regression suite, this implementation can move from integration-hardening to production candidacy.
