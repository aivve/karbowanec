# Confidential Transactions (CT) Comprehensive Review

Date: 2026-05-01

## Scope audited (entire CT surface, not only example files)

I reviewed all CT-related code paths reachable from transaction creation, serialization, mempool admission, consensus validation, wallet accounting, and tests:

- **Core protocol/types/serialization**
  - `include/CryptoNote.h`
  - `src/CryptoNoteCore/Transaction*.{h,cpp}`
  - `src/CryptoNoteCore/TransactionPrefixImpl.cpp`
  - `src/CryptoNoteCore/TransactionApi*.{h,cpp}`
- **CT cryptography primitives**
  - `src/crypto/pedersen.{h,cpp}`
  - `src/crypto/gk_proof.{h,cpp}`
  - `src/crypto/mlsag.{h,cpp}`
  - `src/crypto/ct_ecdh.{h,cpp}`
  - `src/crypto/transaction_balance.{h,cpp}`
- **Consensus/mempool enforcement**
  - `src/CryptoNoteCore/Blockchain.cpp`
  - `src/CryptoNoteCore/TransactionPool.{h,cpp}`
  - `src/CryptoNoteCore/TransactionUtils.{h,cpp}`
- **Wallet/build pipeline**
  - `src/Wallet/TransactionBuilder.{h,cpp}`
  - `src/WalletLegacy/WalletTransactionSender.cpp`
  - `src/WalletLegacy/WalletUserTransactionsCache.{h,cpp}`
- **Tests and integration coverage**
  - `tests/test_gk_proof.cpp`
  - `tests/test_mlsag.cpp`
  - `tests/test_transaction_balance.cpp`
  - `tests/test_ct_integration.cpp`
  - CT-related `UnitTests` and `CoreTests` transaction/pool checks

## What was verified across codebase

1. **Type/layout safety and protocol boundaries**
   - CT-specific structures are isolated in transaction v4 body/prefix model and separated from legacy signatures.
2. **Crypto soundness gates**
   - Pedersen subgroup/identity checks are applied in verify-sensitive paths.
   - GK/MLSAG scalar canonical checks and subgroup checks are present and now hardened.
3. **Consensus invariants**
   - Input/output CT shape checks, ring/member mapping, commitment checks, and proof verification are enforced in blockchain validation.
4. **Mempool/relay consistency**
   - Pool acceptance path mirrors consensus-critical checks where required to avoid delayed-invalid propagation.
5. **Wallet construction & decoding**
   - CT outputs/inputs, pseudo-commitments, and proofs flow correctly through builder and wallet caches.
6. **Regression tests**
   - Unit tests cover positive/negative GK/MLSAG cases; added hardening negatives for identity commitment/key image.

## Additional hardening now in code

- MLSAG transcript hash is domain-separated with a fixed protocol tag (`MLSAG-KarboCT-v1`) in both sign/verify paths.
- Key image validation (non-identity, subgroup-safe via pedersen point validity) is enforced before MLSAG precomputation.
- GK prover now applies strict upfront validation parity with verifier for commitment/ring point validity.

## Production-readiness status

- **Assessment:** CT implementation is now substantially hardened and much closer to production readiness.
- **Remaining release requirement:** run full CI-grade suite (unit + integration + long-run/fuzz/property + consensus regression vectors) in an environment with full dependencies (Boost toolchain, test infra).

