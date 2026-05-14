/* chacha20poly1305.h — ChaCha20-Poly1305 IETF (RFC 8439).
 *
 * Public domain. See UPSTREAM.md for sources.
 *
 * Key      : 32 bytes
 * Nonce    : 12 bytes (IETF variant — NOT XChaCha20's 24-byte nonce)
 * Tag      : 16 bytes, appended to ciphertext on encrypt, checked on decrypt
 * Max data : 2^32 - 1 ChaCha20 blocks = ~256 GB. Far above any realistic
 *            single-message size; the implementation does not enforce this
 *            limit because all callers in Karbo encrypt 32-byte payloads.
 */

#ifndef CHACHA20POLY1305_H
#define CHACHA20POLY1305_H

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

/* AEAD encrypt.
 *
 * `out` must point to msg_len + 16 writable bytes; receives ciphertext
 *   followed by the 16-byte Poly1305 tag.
 * `aad` may be NULL when aad_len == 0.
 * `msg` may be NULL when msg_len == 0.
 *
 * Returns 0 on success. Currently never fails for well-formed inputs.
 */
int chacha20poly1305_ietf_encrypt(
    uint8_t* out,
    const uint8_t* aad, size_t aad_len,
    const uint8_t* msg, size_t msg_len,
    const uint8_t key[32],
    const uint8_t nonce[12]);

/* AEAD decrypt.
 *
 * `in` is ciphertext || tag; `in_len` includes the 16-byte tag.
 * `out` must point to in_len - 16 writable bytes.
 *
 * Returns 0 on success, -1 on tag mismatch (constant-time comparison).
 * On failure, the contents of `out` are unspecified — callers must not
 * use them.
 */
int chacha20poly1305_ietf_decrypt(
    uint8_t* out,
    const uint8_t* aad, size_t aad_len,
    const uint8_t* in, size_t in_len,
    const uint8_t key[32],
    const uint8_t nonce[12]);

#ifdef __cplusplus
}
#endif

#endif /* CHACHA20POLY1305_H */
