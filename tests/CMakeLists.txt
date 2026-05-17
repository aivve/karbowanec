add_definitions(-DSTATICLIB)

include_directories(${gtest_SOURCE_DIR}/include ${gtest_SOURCE_DIR} ../version)

file(GLOB_RECURSE CoreTests CoreTests/*)
file(GLOB_RECURSE CryptoTests crypto/*)
file(GLOB_RECURSE FunctionalTests FunctionalTests/*)
file(GLOB_RECURSE IntegrationTestLibrary IntegrationTestLib/*)
file(GLOB_RECURSE IntegrationTests IntegrationTests/*)
file(GLOB_RECURSE NodeRpcProxyTests NodeRpcProxyTests/*)
file(GLOB_RECURSE PerformanceTests PerformanceTests/*)
file(GLOB_RECURSE SystemTests System/*)
file(GLOB_RECURSE TestGenerator TestGenerator/*)
file(GLOB_RECURSE TransfersTests TransfersTests/*)
file(GLOB_RECURSE UnitTests UnitTests/*)

file(GLOB_RECURSE CryptoNoteProtocol ../src/CryptoNoteProtocol/*)
file(GLOB_RECURSE P2p ../src/P2p/*)

source_group("" FILES ${CoreTests} ${CryptoTests} ${FunctionalTests} ${IntegrationTestLibrary} ${IntegrationTests} ${NodeRpcProxyTests} ${PerformanceTests} ${SystemTests} ${TestGenerator} ${TransfersTests} ${UnitTests})
source_group("" FILES ${CryptoNoteProtocol} ${P2p})

add_library(IntegrationTestLibrary ${IntegrationTestLibrary})
add_library(TestGenerator ${TestGenerator})

add_executable(CoreTests ${CoreTests})
add_executable(CryptoTests ${CryptoTests})
add_executable(IntegrationTests ${IntegrationTests})
add_executable(NodeRpcProxyTests ${NodeRpcProxyTests})
add_executable(PerformanceTests ${PerformanceTests})
add_executable(SystemTests ${SystemTests})
add_executable(TransfersTests ${TransfersTests})
add_executable(UnitTests ${UnitTests})
add_executable(LMDBReorgTests test_lmdb_reorg.cpp)
add_executable(DifficultyTests Difficulty/Difficulty.cpp)
add_executable(HashTargetTests HashTarget.cpp)
add_executable(HashTests Hash/main.cpp)

add_executable(GKProofTests test_gk_proof.cpp)
add_executable(TriptychTests test_triptych.cpp)
add_executable(TxBalanceTests test_transaction_balance.cpp)
add_executable(CTIntegrationTests test_ct_integration.cpp)
add_executable(CTBench bench_ct_validation.cpp)
# CT serializer fuzz harness, standalone PRNG-driven mode.
# For coverage-guided fuzzing rebuild with clang -fsanitize=fuzzer,address,undefined
# and drop the CTFUZZER_STANDALONE define so LLVMFuzzerTestOneInput becomes the entrypoint.
add_executable(CTFuzz fuzz_ct_serializer.cpp)
target_compile_definitions(CTFuzz PRIVATE CTFUZZER_STANDALONE=1)
add_executable(MixedV2Roundtrip test_mixed_v2_roundtrip.cpp)
target_link_libraries(MixedV2Roundtrip CryptoNoteCore Serialization Crypto Common Logging ${Boost_LIBRARIES})
if(MSVC)
  target_link_libraries(MixedV2Roundtrip legacy_stdio_definitions.lib)
endif()
add_executable(KVMixedV2Roundtrip test_kv_v2_roundtrip.cpp)
target_link_libraries(KVMixedV2Roundtrip CryptoNoteCore Serialization Crypto Common Logging ${Boost_LIBRARIES})
if(MSVC)
  target_link_libraries(KVMixedV2Roundtrip legacy_stdio_definitions.lib)
endif()

target_link_libraries(CoreTests TestGenerator Wallet CryptoNoteCore Serialization System Logging Common Crypto BlockchainExplorer ${Boost_LIBRARIES})
target_link_libraries(IntegrationTests IntegrationTestLibrary Wallet NodeRpcProxy InProcessNode P2P Rpc Http Transfers Serialization System CryptoNoteCore Logging Common Crypto BlockchainExplorer gtest Mnemonics upnpc-static ${Boost_LIBRARIES})
target_link_libraries(NodeRpcProxyTests NodeRpcProxy CryptoNoteCore Rpc Http Serialization System Logging Common Crypto ${Boost_LIBRARIES})
target_link_libraries(PerformanceTests Wallet CryptoNoteCore Serialization Logging Common Crypto ${Boost_LIBRARIES})
target_link_libraries(SystemTests System gtest_main ${Boost_LIBRARIES})
target_link_libraries(TransfersTests IntegrationTestLibrary Wallet gtest_main InProcessNode NodeRpcProxy P2P Rpc Http BlockchainExplorer CryptoNoteCore Serialization System Logging Transfers Common Crypto Mnemonics upnpc-static ${Boost_LIBRARIES})
target_link_libraries(UnitTests gtest_main PaymentGate Wallet TestGenerator InProcessNode NodeRpcProxy Rpc Http Transfers Serialization System Logging BlockchainExplorer CryptoNoteCore Checkpoints Common Crypto Mnemonics ${Boost_LIBRARIES})
target_link_libraries(LMDBReorgTests TestGenerator Wallet CryptoNoteCore Serialization System Logging Common Crypto BlockchainExplorer Checkpoints ${Boost_LIBRARIES})
target_link_libraries(DifficultyTests CryptoNoteCore Serialization Crypto Logging Common ${Boost_LIBRARIES})
target_link_libraries(HashTargetTests CryptoNoteCore Crypto)
target_link_libraries(HashTests Crypto)

target_link_libraries(GKProofTests Crypto Common ${Boost_LIBRARIES})
target_link_libraries(TriptychTests Crypto Common ${Boost_LIBRARIES})
target_link_libraries(TxBalanceTests Crypto Common ${Boost_LIBRARIES})
target_link_libraries(CTIntegrationTests Crypto Common ${Boost_LIBRARIES})
target_link_libraries(CTBench Crypto Common ${Boost_LIBRARIES})
target_link_libraries(CTFuzz CryptoNoteCore Serialization Crypto Common Logging ${Boost_LIBRARIES})
if(MSVC)
  # CryptoNoteCore.lib pulls in StringTools which references the inline sscanf;
  # link against the MSVC compatibility shim so it resolves under VS2022.
  target_link_libraries(CTFuzz legacy_stdio_definitions.lib)
endif()

if(MSVC)
  target_link_libraries(SystemTests ws2_32)
  target_link_libraries(NodeRpcProxyTests ws2_32)
  target_link_libraries(CoreTests ws2_32)
  target_link_libraries(DifficultyTests legacy_stdio_definitions.lib)
  target_link_libraries(HashTests legacy_stdio_definitions.lib)
  target_link_libraries(UnitTests legacy_stdio_definitions.lib)
  target_link_libraries(LMDBReorgTests ws2_32)
  target_link_libraries(LMDBReorgTests legacy_stdio_definitions.lib)
endif()

if(${CMAKE_SYSTEM_NAME} STREQUAL "Linux" OR APPLE AND NOT ANDROID)
  target_link_libraries(CoreTests -lresolv)
  target_link_libraries(IntegrationTests -lresolv)
  target_link_libraries(NodeRpcProxyTests -lresolv)
  target_link_libraries(PerformanceTests -lresolv)
  target_link_libraries(TransfersTests -lresolv)
  target_link_libraries(UnitTests -lresolv)
  target_link_libraries(DifficultyTests -lresolv)
endif()

if(NOT MSVC)
  set_property(TARGET gtest gtest_main IntegrationTestLibrary IntegrationTests TestGenerator UnitTests SystemTests HashTargetTests TransfersTests APPEND_STRING PROPERTY COMPILE_FLAGS " -Wno-undef -Wno-sign-compare")
endif()

add_custom_target(tests DEPENDS CoreTests IntegrationTests NodeRpcProxyTests PerformanceTests SystemTests TransfersTests UnitTests LMDBReorgTests DifficultyTests HashTargetTests HashTests)

set_property(TARGET
  tests
  IntegrationTestLibrary
  TestGenerator
  LMDBReorgTests
  CoreTests
  CryptoTests
  IntegrationTests
  NodeRpcProxyTests
  PerformanceTests
  SystemTests
  TransfersTests
  UnitTests
  DifficultyTests
  HashTargetTests
  HashTests
  GKProofTests
  TriptychTests
  TxBalanceTests
  CTIntegrationTests
PROPERTY FOLDER "tests")

add_dependencies(IntegrationTestLibrary version)

set_property(TARGET CoreTests PROPERTY OUTPUT_NAME "core_tests")
set_property(TARGET CryptoTests PROPERTY OUTPUT_NAME "crypto_tests")
set_property(TARGET IntegrationTests PROPERTY OUTPUT_NAME "integration_tests")
set_property(TARGET NodeRpcProxyTests PROPERTY OUTPUT_NAME "node_rpc_proxy_tests")
set_property(TARGET PerformanceTests PROPERTY OUTPUT_NAME "performance_tests")
set_property(TARGET SystemTests PROPERTY OUTPUT_NAME "system_tests")
set_property(TARGET TransfersTests PROPERTY OUTPUT_NAME "transfers_tests")
set_property(TARGET UnitTests PROPERTY OUTPUT_NAME "unit_tests")
set_property(TARGET LMDBReorgTests PROPERTY OUTPUT_NAME "lmdb_reorg_tests")
set_property(TARGET DifficultyTests PROPERTY OUTPUT_NAME "difficulty_tests")
set_property(TARGET HashTargetTests PROPERTY OUTPUT_NAME "hash_target_tests")
set_property(TARGET HashTests PROPERTY OUTPUT_NAME "hash_tests")
set_property(TARGET GKProofTests PROPERTY OUTPUT_NAME "gk_proof_tests")
set_property(TARGET TriptychTests PROPERTY OUTPUT_NAME "triptych_tests")
set_property(TARGET TxBalanceTests PROPERTY OUTPUT_NAME "tx_balance_tests")
set_property(TARGET CTIntegrationTests PROPERTY OUTPUT_NAME "ct_integration_tests")

add_test(CoreTests core_tests --generate_and_play_test_data)
add_test(CryptoTests crypto_tests ${CMAKE_CURRENT_SOURCE_DIR}/crypto/tests.txt)
add_test(DifficultyTests difficulty_tests ${CMAKE_CURRENT_SOURCE_DIR}/Difficulty/data.txt)
foreach(hash IN ITEMS fast slow tree extra-blake extra-groestl extra-jh extra-skein)
  add_test(hash-${hash} hash_tests ${hash} ${CMAKE_CURRENT_SOURCE_DIR}/Hash/tests-${hash}.txt)
endforeach()
add_test(HashTargetTests hash_target_tests)
add_test(SystemTests system_tests)
add_test(UnitTests unit_tests)
add_test(GKProofTests gk_proof_tests)
add_test(TriptychTests triptych_tests)
add_test(TxBalanceTests tx_balance_tests)
add_test(CTIntegrationTests ct_integration_tests)
add_test(LMDBReorgTests lmdb_reorg_tests)

