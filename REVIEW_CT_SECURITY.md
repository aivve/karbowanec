# CT Production-Readiness Review (dev/ct_17 vs master)

Date: 2026-05-01
Reviewer: Codex

## Scope

Focused deep review of the confidential transaction cryptography core and integration points:
- `src/crypto/gk_proof.h/.cpp`
- `src/crypto/mlsag.h/.cpp`
- related CT plumbing in pool/validation/tests (spot checks)

Because this local checkout currently has only branch `work` and no `master`/`dev/ct_17` refs, this review evaluates the CT implementation present in `work` (which already contains `dev/ct_17` merge commits) as a production-readiness gate.

## Executive Summary

Current CT implementation is **close but not production-ready**.

Cryptographic implementation quality is decent (domain-separated FS challenge, scalar range checks, subgroup checks in GK verification), but there are still high-impact hardening gaps and engineering risks that should be closed before mainnet production.

## High-priority findings

### 1) MLSAG verification does not enforce key-image subgroup/non-identity constraints

`mlsag_verify` decodes `key_image` and precomputes without checking canonical subgroup membership / torsion or identity element rejection.

Why this matters:
- Linkability and soundness assumptions require key images in the prime-order subgroup and not identity.
- Accepting malformed or low-order points can enable signature malleability/edge-case verification acceptance in some Ed25519 constructions.

Evidence:
- `src/crypto/mlsag.cpp`: `ge_frombytes_vartime(&image_p3, ...)` is checked, but no explicit subgroup/identity validation before use.

Recommended action:
- Add an explicit key-image validity guard consistent with existing `check_key_image`/`point_valid_for_*` primitives used elsewhere in codebase.
- Reject identity key image and low-order points.

### 2) GK prover path lacks early subgroup/canonical validation on public input commitment C

`gk_prove` relies on `compute_derived_ring` decode and proceeds, while verifier is stricter (subgroup checks on A/B/Q and D[k]).

Why this matters:
- A malformed C should be rejected consistently early in both prover and verifier paths.
- In wallet/backend mixed environments, being permissive in one path and strict in another increases consensus split / UX risk.

Evidence:
- `src/crypto/gk_proof.cpp`: `gk_prove` checks denomination index/value and decode via `compute_derived_ring`, but no explicit subgroup check on decoded `C_p3`.

Recommended action:
- Add subgroup/canonical checks for `C` in prover path to mirror verifier strictness.

### 3) Missing explicit transcript binding to protocol/version network params beyond string tag

GK challenge uses a domain tag + tx hash + points, which is good. MLSAG round hash includes only `message||L1||R1||L2`.

Why this matters:
- Cross-protocol replay/mixup risks are reduced if all transcript hashes include explicit domain separators and versioning constants.
- Future upgrades become safer with deterministic versioned transcript tags.

Evidence:
- `src/crypto/mlsag.cpp`: `mlsag_round_hash` hashes only concatenation without domain string.

Recommended action:
- Prefix MLSAG challenge hash with domain/version tag (e.g., `"MLSAG-KarboCT-v1"`) and keep backward compatibility with clear fork-height gating if needed.

## Medium-priority findings

1. **Variable-time operations on verification path** (`*_vartime`) are acceptable for public inputs, but code comments should clearly state side-channel rationale to avoid accidental secret-path reuse.
2. **Memory scrubbing asymmetry**: GK prover scrubs some secrets; MLSAG signer leaves `alpha1/alpha2/z` and challenge vector in memory. Consider cleanup for defense-in-depth.
3. **Ring size policy**: implementation accepts arbitrary `ring_size`; protocol-level min/max and uniformity constraints should be enforced in transaction validation (DoS and anonymity-set quality).

## Architecture observations

- CT stack is modular: amount commitments (`pedersen`), denomination proof (`gk_proof`), spend proof (`mlsag`), and balance checks are separated cleanly.
- This separation is positive for auditing, but consensus-critical checks must remain centralized in validation paths and mirrored in mempool acceptance.

## Production readiness decision

**Decision: NOT READY (pending hardening fixes).**

Release gate criteria to pass before production:
1. Add key-image subgroup/identity checks in MLSAG verify/sign input validation.
2. Add strict canonical/subgroup input validation parity in GK prove/verify paths.
3. Add transcript domain separation/versioning for MLSAG (with deterministic fork migration plan).
4. Re-run CT unit/integration/fuzz/property tests and consensus-vector regression before release candidate.

