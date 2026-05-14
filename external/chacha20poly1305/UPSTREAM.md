# chacha20poly1305 (IETF, RFC 8439)

A focused, self-contained implementation of ChaCha20-Poly1305 IETF (12-byte
nonce) used by the Karbo PQ transaction family for output payload encryption
(spec §2 / §6.5).

## Components

- `chacha20poly1305.h` — public C API
- `chacha20poly1305.c` — single-file implementation

## Sources

- **ChaCha20**: RFC 8439 §2.3–§2.4 reference code (Daniel J. Bernstein, public
  domain). The pseudocode in the RFC compiles to working C with only mechanical
  type substitutions.
- **Poly1305**: poly1305-donna, the 32-bit reference implementation by Andrew
  Moon (https://github.com/floodyberry/poly1305-donna), released into the
  public domain. We embed only the 32-bit `poly1305_donna.h`-style core
  inlined for self-containment.
- **AEAD construction**: RFC 8439 §2.8 (encrypt) and §2.8 (verify).

## Status

- KAT-verified against RFC 8439 §2.8.2 (the canonical test vector).
- Constant-time tag comparison in `chacha20poly1305_ietf_decrypt`.
- No SIMD / no platform-specific code — pure portable C.

## Why not libsodium / monocypher / liboqs?

- `liboqs` does not ship ChaCha20 or Poly1305.
- `libsodium` is ~3 MB; we need exactly one AEAD instance at fixed nonce/key
  length and don't want to drag in the rest of NaCl.
- `monocypher` exposes IETF ChaCha20 raw and Poly1305 raw, but its packaged
  AEAD (`crypto_aead_lock`) uses **XChaCha20** (24-byte nonce), not RFC 8439
  IETF (12-byte nonce). The Karbo PQ spec is IETF, so we'd glue the parts
  ourselves anyway.
