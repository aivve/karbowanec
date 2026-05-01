# Confidential Transactions (Pubkey-Referenced Rings) Review vs Master

Date: 2026-05-01
Branch under review: `work` (used as `dev/ct_17` equivalent in this checkout)
Head reviewed: `f8c1b80`
Master baseline used: `8fce081` (master parent in merge `80c2bac`)

## Executive summary

- **Core direction is sound**: hidden amounts + Pedersen commitments + MLSAG + per-output GK membership proofs is a coherent CT design for a CryptoNote-derived chain.
- **Primary privacy goal is met by design**: counterparties can no longer observe sender wallet wealth from transparent transfer amounts.
- **Denomination-set tradeoff is intentional and accepted**: the project explicitly prefers simpler cryptography/math and implementation over Bulletproof-style full-range machinery.
- **Dust exclusion from CT is intentional**: sub-floor residue is expected to be absorbed as miner fee rather than represented as confidential dust outputs.
- **Production readiness focus should now be robustness/testing**, not redesign of denomination model.

## Scope and method

Compared `8fce081..f8c1b80` with focus on:

1. CT transaction format/serialization and validation plumbing.
2. Commitment/proof model for hidden amounts.
3. Pubkey-referenced ring member resolution across chain + mempool.
4. Wallet construction path and denomination decomposition behavior.
5. Reorg/mempool safety properties.

## What is strong

### 1) Consensus validation separation is correct

- Chain/block verification is deterministic and does not depend on mempool-only state.
- CT ring member references by output pubkey avoid fragile global-index coupling and are better under reorg churn.

### 2) Commitment and membership construction is internally consistent

- Outputs carry Pedersen commitments and masked amounts.
- GK proofs bind each commitment to one of exactly 64 canonical denominations (`GK_N = 64`), and prover-side checks enforce index/value consistency.
- Fixed-denomination membership proofs provide a tractable and auditable anti-inflation path.

### 3) Recent mempool hardening addresses concrete safety classes

- Duplicate pubkey collisions in mempool index are rejected (instead of silent overwrite).
- Mempool CT output index widened to `uint32_t`, removing `uint16_t` truncation risk for resolver paths.

## Denomination-set model: accepted design constraints

The project intentionally uses an allowed denomination set instead of full-range Bulletproofs.

### A) Cryptographic complexity tradeoff (accepted)

- Simpler proving/verification and lower implementation complexity.
- Smaller cryptographic surface area to audit compared with a new full range-proof stack.
- Better fit for current project goals and engineering capacity.

### B) Privacy objective in scope

- Goal in scope: receiver should not learn sender wallet wealth from visible tx amounts.
- CT commitments + MLSAG + masked amounts deliver that core improvement over transparent-value CryptoNote behavior.

### C) Dust policy is explicit and intentional

- `MIN_CT_DENOMINATION = 0.01 KRB` (10^10 au) sets a hard floor for confidential outputs.
- Amount residue below floor is intentionally converted into additional miner fee (no confidential dust outputs).
- This should be kept deterministic across wallet/node paths so tx construction behavior is predictable.

## Required robustness gates before production

1. **Denomination decomposition invariants**
   - Representable amounts round-trip exactly.
   - Non-representable sub-floor components are handled by deterministic fee absorption.
2. **Adversarial mempool/reorg integration tests**
   - A->B->C dependency chains, parent mined/unmined transitions, cascade eviction, duplicate pubkey rejection.
3. **Wallet/node consistency checks**
   - Ensure identical tx-building decisions for denomination decomposition and fee handling across supported backends.
4. **Capacity/performance validation**
   - Verify CT verification/serialization behavior under high-output transactions and realistic mempool pressure.

## Final verdict

For the stated goals (hide transfer amounts and prevent counterparties from inferring sender wealth from transparent amounts, while keeping cryptography simpler than Bulletproof-based designs), the implementation direction is correct and near production-candidate.

Remaining work is primarily engineering hardening (test depth, determinism, and stress validation), not a change in cryptographic approach.
