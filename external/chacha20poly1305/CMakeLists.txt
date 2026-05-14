# chacha20poly1305 — vendored ChaCha20-Poly1305 IETF (RFC 8439).
# See UPSTREAM.md for sources and licensing (public domain).

add_library(chacha20poly1305 STATIC
  chacha20poly1305.c
  chacha20poly1305.h)

target_include_directories(chacha20poly1305 PUBLIC
  $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>)

set_target_properties(chacha20poly1305 PROPERTIES C_STANDARD 99)

if(MSVC)
  # Suppress some legacy-CRT warnings that arrive only on consumer compile of
  # the C standard library headers; nothing here uses banned APIs.
  target_compile_definitions(chacha20poly1305 PRIVATE _CRT_SECURE_NO_WARNINGS)
endif()
