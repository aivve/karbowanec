set(UPNPC_BUILD_STATIC ON CACHE BOOL "Build static library")
set(UPNPC_BUILD_SHARED OFF CACHE BOOL "Build shared library")
set(UPNPC_BUILD_TESTS OFF CACHE BOOL "Build test executables")

if (STATIC AND MSVC)
  foreach(VAR CMAKE_C_FLAGS_RELEASE)
    string(REPLACE "/MD" "" ${VAR} "${${VAR}}")
  endforeach()
endif()

add_subdirectory(miniupnpc)
add_subdirectory(gtest)

message(STATUS "Using ${ARCH_WIDTH}-bit LMDB from source tree")
add_subdirectory(liblmdb)
set(LMDB_INCLUDE "liblmdb" CACHE STRING "LMDB Include path")
set(LMDB_LIBRARY "lmdb" CACHE STRING "LMDB Library name")

include_directories(${LMDB_INCLUDE})

# liboqs (Open Quantum Safe) — vendored snapshot of upstream 0.13.0,
# pruned to only ML-KEM-768 and ML-DSA-65. See external/liboqs/UPSTREAM.md.
# We disable every algorithm family except ML-KEM and ML-DSA, build static only,
# and skip OpenSSL / runtime CPU dispatch so the library stays self-contained.
set(OQS_BUILD_ONLY_LIB        ON  CACHE BOOL "" FORCE)
set(BUILD_SHARED_LIBS         OFF CACHE BOOL "" FORCE)
set(OQS_DIST_BUILD            OFF CACHE BOOL "" FORCE)
set(OQS_USE_OPENSSL           OFF CACHE BOOL "" FORCE)
set(OQS_USE_SHA2_OPENSSL      OFF CACHE BOOL "" FORCE)
set(OQS_USE_SHA3_OPENSSL      OFF CACHE BOOL "" FORCE)
set(OQS_USE_AES_OPENSSL       OFF CACHE BOOL "" FORCE)
# Disable all KEM families except ML-KEM
set(OQS_ENABLE_KEM_BIKE             OFF CACHE BOOL "" FORCE)
set(OQS_ENABLE_KEM_FRODOKEM         OFF CACHE BOOL "" FORCE)
set(OQS_ENABLE_KEM_NTRUPRIME        OFF CACHE BOOL "" FORCE)
set(OQS_ENABLE_KEM_CLASSIC_MCELIECE OFF CACHE BOOL "" FORCE)
set(OQS_ENABLE_KEM_HQC              OFF CACHE BOOL "" FORCE)
set(OQS_ENABLE_KEM_KYBER            OFF CACHE BOOL "" FORCE)
set(OQS_ENABLE_KEM_ML_KEM           ON  CACHE BOOL "" FORCE)
# Disable ML-KEM variants we don't use
set(OQS_ENABLE_KEM_ml_kem_512       OFF CACHE BOOL "" FORCE)
set(OQS_ENABLE_KEM_ml_kem_768       ON  CACHE BOOL "" FORCE)
set(OQS_ENABLE_KEM_ml_kem_1024      OFF CACHE BOOL "" FORCE)
# Disable all SIG families except ML-DSA
set(OQS_ENABLE_SIG_DILITHIUM        OFF CACHE BOOL "" FORCE)
set(OQS_ENABLE_SIG_FALCON           OFF CACHE BOOL "" FORCE)
set(OQS_ENABLE_SIG_SPHINCS          OFF CACHE BOOL "" FORCE)
set(OQS_ENABLE_SIG_MAYO             OFF CACHE BOOL "" FORCE)
set(OQS_ENABLE_SIG_CROSS            OFF CACHE BOOL "" FORCE)
set(OQS_ENABLE_SIG_UOV              OFF CACHE BOOL "" FORCE)
set(OQS_ENABLE_SIG_ML_DSA           ON  CACHE BOOL "" FORCE)
# Disable ML-DSA variants we don't use
set(OQS_ENABLE_SIG_ml_dsa_44        OFF CACHE BOOL "" FORCE)
set(OQS_ENABLE_SIG_ml_dsa_65        ON  CACHE BOOL "" FORCE)
set(OQS_ENABLE_SIG_ml_dsa_87        OFF CACHE BOOL "" FORCE)
# Stateful signatures — both off (we keep sig_stfl/sig_stfl.c as a no-op shim)
set(OQS_ENABLE_SIG_STFL_XMSS        OFF CACHE BOOL "" FORCE)
set(OQS_ENABLE_SIG_STFL_LMS         OFF CACHE BOOL "" FORCE)
add_subdirectory(liboqs)

# ChaCha20-Poly1305 IETF (RFC 8439) — vendored single-file implementation,
# public domain. See external/chacha20poly1305/UPSTREAM.md.
add_subdirectory(chacha20poly1305)

set_property(TARGET lmdb upnpc-static gtest gtest_main oqs chacha20poly1305 PROPERTY FOLDER "external")

if(MSVC)
  if (STATIC)
    set (UPNPC_FLAGS "/MT")
  endif()
  set_property(TARGET upnpc-static APPEND_STRING PROPERTY COMPILE_FLAGS " -wd4244 -wd4267 -wd4996 ${UPNPC_FLAGS}")
elseif(FREEBSD)
  set_property(TARGET upnpc-static APPEND_STRING PROPERTY COMPILE_FLAGS " -Wno-undef -Wno-unused-result -Wno-unused-value")
else()
  set_property(TARGET upnpc-static APPEND_STRING PROPERTY COMPILE_FLAGS " -Wno-undef -Wno-unused-result -Wno-unused-value")
endif()
