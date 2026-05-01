# Confidential Transactions (Pubkey-Referenced Rings) Review vs Master

Date: 2026-05-01
Branch under review: `work` (used as `dev/ct_17` equivalent in this checkout)
Head reviewed: `f8c1b80`
Master baseline used: `8fce081` (master parent in merge `80c2bac`)

## Executive summary

- **Core direction is sound**: hidden amounts + Pedersen commitments + MLSAG + per-output GK membership proofs is a coherent CT design for a CryptoNote-derived chain.
- **Production safety improved materially** on this branch by hardening pubkey-ring mempool resolution (collision rejection + wider mempool out index).
- **Main residual production risk is economic/privacy policy, not cryptographic plumbing**: the fixed 64-denomination lattice leaks amount structure compared with full range proofs and pushes sub-floor residue into transparent change/fees.
- **Verdict**: technically close, but **not yet “production-ready”** until denomination-side edge cases and policy/test coverage are closed.

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

### 3) Recent mempool hardening addresses a real safety class

- Duplicate pubkey collisions in mempool index are rejected (instead of silent overwrite).
- Mempool CT output index widened to `uint32_t`, removing `uint16_t` truncation risk for resolver paths.

## Critical review of the “allowed denomination set” model

This is the key architectural tradeoff of this CT rollout.

### A) Security correctness: acceptable

Using a fixed denomination universe plus membership proof **does prove** outputs are in an allowed set and prevents arbitrary committed values. That gives strong anti-inflation structure when combined with balance checks and MLSAG spend authorization.

### B) Privacy leakage: materially higher than modern CT with range proofs

A 64-value set leaks value class information:

- Every output is known to be one of 64 exact amounts.
- Multi-output composition patterns become statistically distinctive.
- Repeated wallet decomposition heuristics can fingerprint payment behavior over time.

This is a **known tradeoff**, but it should be treated as product-level privacy debt vs Bulletproof-style range proofs.

### C) Representability and UX/economic edge cases

`MIN_CT_DENOMINATION = 0.01 KRB` (10^10 au) means any sub-floor remainder cannot be encoded as CT output.

Implications:
- Wallets must either (1) keep transparent residue, or (2) burn residue into fee.
- If decomposition/fallback policy is inconsistent across wallet/server versions, users can see confusing failures or privacy regressions.
- Attackers can use dust-shaping around denomination boundaries to increase metadata leakage.

### D) Supply/parameter rigidity risk

The denomination list is consensus-critical static data. Any later change is a hard-fork event requiring carefully orchestrated migration/testing. This is manageable, but should be explicitly documented as consensus surface.

## Must-fix / must-prove before production

1. **Add invariant tests for denomination decomposition and reconstruction**
   - Property tests: representable amounts round-trip exactly.
   - Explicit failure tests for non-representable sub-floor amounts.
2. **Codify residue policy at consensus/tx-construction boundaries**
   - One deterministic rule for “transparent change vs fee absorption.”
3. **Adversarial mempool/reorg integration tests**
   - A->B->C dependency chains, parent mined/unmined transitions, cascade eviction, duplicate pubkey rejection.
4. **Document privacy envelope honestly**
   - User-facing docs should state denomination-set leakage characteristics.
5. **Capacity tests on large CT txs**
   - Ensure verification cost and serialization limits are safe under worst-case output fanout.

## Final verdict

For the stated goal (bringing hidden amounts + MLSAG to a previously transparent-amount CryptoNote-like base), this implementation is **architecturally credible and substantially hardened**.

However, because the denomination-set model introduces meaningful privacy/UX policy edges, and because these edges are consensus-adjacent in practice, I would rate current state as:

- **Consensus cryptography/pathing**: near-ready.
- **Operational and policy robustness**: needs one more hardening/testing cycle.
- **Production readiness**: **Not yet**; likely ready after the above test/policy gates pass.
