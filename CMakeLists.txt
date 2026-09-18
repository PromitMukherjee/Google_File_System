cmake_minimum_required(VERSION 3.20)

project(Google_File_System
    VERSION 1.0
    LANGUAGES CXX
)

# ============================================================
# C++ CONFIGURATION
# ============================================================

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

if(NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE Debug)
endif()

# ============================================================
# DEPENDENCIES
# ============================================================

include(FetchContent)
include(GoogleTest)

find_package(Protobuf REQUIRED)

find_package(gRPC CONFIG QUIET)

if(NOT TARGET gRPC::grpc++)
    find_library(
        GRPC_LIBRARY
        NAMES grpc++
        REQUIRED
    )

    find_library(
        GRPC_GRPC_LIBRARY
        NAMES grpc
        REQUIRED
    )

    find_library(
        GRPC_REFLECTION_LIBRARY
        NAMES grpc++_reflection
    )

    find_library(
        GRPC_PROTOBUF_LIBRARY
        NAMES grpc++_unsecure
    )

    find_program(
        GRPC_CPP_PLUGIN
        NAMES grpc_cpp_plugin
        REQUIRED
    )

    add_library(gfs_grpc INTERFACE)

    target_link_libraries(
        gfs_grpc
        INTERFACE
            ${GRPC_LIBRARY}
            ${GRPC_GRPC_LIBRARY}
    )

    if(GRPC_REFLECTION_LIBRARY)
        target_link_libraries(
            gfs_grpc
            INTERFACE
                ${GRPC_REFLECTION_LIBRARY}
        )
    endif()
else()
    set(
        GRPC_CPP_PLUGIN_TARGET
        gRPC::grpc_cpp_plugin
    )
endif()

# ============================================================
# GOOGLE TEST
# ============================================================

FetchContent_Declare(
    googletest
    URL https://github.com/google/googletest/archive/refs/tags/v1.17.0.zip
)

set(
    gtest_force_shared_crt
    ON
    CACHE BOOL
    ""
    FORCE
)

FetchContent_MakeAvailable(googletest)

# ============================================================
# PROTOBUF / gRPC GENERATION
# ============================================================

set(
    PROTO_FILES

    ${CMAKE_CURRENT_SOURCE_DIR}/proto/master.proto
    ${CMAKE_CURRENT_SOURCE_DIR}/proto/chunkserver.proto
    ${CMAKE_CURRENT_SOURCE_DIR}/proto/client.proto
)

set(
    GENERATED_DIR
    ${CMAKE_CURRENT_BINARY_DIR}/generated
)

file(
    MAKE_DIRECTORY
    ${GENERATED_DIR}
)

set(PROTO_GENERATED_SOURCES)
set(PROTO_GENERATED_HEADERS)

foreach(PROTO_FILE ${PROTO_FILES})

    get_filename_component(
        PROTO_NAME
        ${PROTO_FILE}
        NAME_WE
    )

    set(
        PB_CC
        ${GENERATED_DIR}/${PROTO_NAME}.pb.cc
    )

    set(
        PB_H
        ${GENERATED_DIR}/${PROTO_NAME}.pb.h
    )

    set(
        GRPC_CC
        ${GENERATED_DIR}/${PROTO_NAME}.grpc.pb.cc
    )

    set(
        GRPC_H
        ${GENERATED_DIR}/${PROTO_NAME}.grpc.pb.h
    )

    if(TARGET protobuf::protoc)

        set(
            PROTOC_EXECUTABLE
            $<TARGET_FILE:protobuf::protoc>
        )

    else()

        find_program(
            PROTOC_EXECUTABLE
            NAMES protoc
            REQUIRED
        )

    endif()

    if(TARGET gRPC::grpc_cpp_plugin)

        set(
            GRPC_PLUGIN_EXECUTABLE
            $<TARGET_FILE:gRPC::grpc_cpp_plugin>
        )

    else()

        set(
            GRPC_PLUGIN_EXECUTABLE
            ${GRPC_CPP_PLUGIN}
        )

    endif()

    add_custom_command(
        OUTPUT
            ${PB_CC}
            ${PB_H}
            ${GRPC_CC}
            ${GRPC_H}

        COMMAND
            ${PROTOC_EXECUTABLE}

            --proto_path=${CMAKE_CURRENT_SOURCE_DIR}/proto

            --cpp_out=${GENERATED_DIR}

            --grpc_out=${GENERATED_DIR}

            --plugin=protoc-gen-grpc=${GRPC_PLUGIN_EXECUTABLE}

            ${PROTO_FILE}

        DEPENDS
            ${PROTO_FILE}

        VERBATIM
    )

    list(
        APPEND
        PROTO_GENERATED_SOURCES

        ${PB_CC}
        ${GRPC_CC}
    )

    list(
        APPEND
        PROTO_GENERATED_HEADERS

        ${PB_H}
        ${GRPC_H}
    )

endforeach()

# ============================================================
# GFS PROTOCOL LIBRARY
# ============================================================

add_library(
    gfs_protocol

    ${PROTO_GENERATED_SOURCES}
)

target_include_directories(
    gfs_protocol
    PUBLIC

        ${GENERATED_DIR}
)

target_link_libraries(
    gfs_protocol
    PUBLIC

        protobuf::libprotobuf
)

if(TARGET gRPC::grpc++)

    target_link_libraries(
        gfs_protocol
        PUBLIC

            gRPC::grpc++

    )

else()

    target_link_libraries(
        gfs_protocol
        PUBLIC

            gfs_grpc

    )

endif()

target_compile_features(
    gfs_protocol
    PUBLIC

        cxx_std_20
)

# ============================================================
# GFS CORE LIBRARY
# ============================================================

add_library(
    gfs_core

    # --------------------------------------------------------
    # COMMON
    # --------------------------------------------------------

    src/common/types.cpp
    src/common/status.cpp
    src/common/utils.cpp

    # --------------------------------------------------------
    # PROTOCOL
    # --------------------------------------------------------

    src/protocol/master_service.cpp
    src/protocol/chunkserver_service.cpp
    src/protocol/client_service.cpp

    # --------------------------------------------------------
    # MASTER — METADATA
    # --------------------------------------------------------

    src/master/metadata/metadata.cpp
    src/master/metadata/file_metadata.cpp
    src/master/metadata/chunk_metadata.cpp

    # --------------------------------------------------------
    # MASTER — NAMESPACE
    # --------------------------------------------------------

    src/master/namespace/namespace_manager.cpp
    src/master/namespace/namespace_lock.cpp

    # --------------------------------------------------------
    # MASTER — LEASE
    # --------------------------------------------------------

    src/master/lease/lease.cpp
    src/master/lease/lease_manager.cpp

    # --------------------------------------------------------
    # MASTER — HEARTBEAT
    # --------------------------------------------------------

    src/master/heartbeat/chunkserver_state.cpp
    src/master/heartbeat/heartbeat_manager.cpp

    # --------------------------------------------------------
    # MASTER
    # --------------------------------------------------------

    src/master/master.cpp

    # --------------------------------------------------------
    # MASTER — REPLICATION
    # --------------------------------------------------------

    src/master/replication/replica_manager.cpp
    src/master/replication/placement_policy.cpp
    src/master/replication/re_replication.cpp

    # --------------------------------------------------------
    # MASTER — RECOVERY
    # Phase 10
    # --------------------------------------------------------

    src/master/recovery/recovery_manager.cpp

    # --------------------------------------------------------
    # MASTER — PERSISTENCE / RECOVERY
    # Phase 11
    # --------------------------------------------------------

    src/master/recovery/operation_log.cpp
    src/master/recovery/checkpoint.cpp

    # --------------------------------------------------------
    # MASTER — GARBAGE COLLECTION
    # Phase 14
    # --------------------------------------------------------

    src/master/garbage_collection/orphan_chunk_manager.cpp
    src/master/garbage_collection/garbage_collector.cpp

    # --------------------------------------------------------
    # MASTER — REPLICATION REBALANCING
    # Phase 14
    # --------------------------------------------------------

    src/master/replication/rebalancer.cpp

    # --------------------------------------------------------
    # PHASE 15 — TESTING INFRASTRUCTURE
    # --------------------------------------------------------

    src/testing/failure_injector.cpp
    src/testing/distributed_test_harness.cpp

    # --------------------------------------------------------
    # CHUNKSERVER — CHECKSUM
    # --------------------------------------------------------

    src/chunkserver/checksum/checksum_block.cpp
    src/chunkserver/checksum/checksum_manager.cpp

    # --------------------------------------------------------
    # CHUNKSERVER — STORAGE
    # --------------------------------------------------------

    src/chunkserver/storage/chunk_storage.cpp
    src/chunkserver/storage/chunk_file.cpp
    src/chunkserver/storage/storage_manager.cpp

    # --------------------------------------------------------
    # CHUNKSERVER — MUTATION
    # --------------------------------------------------------

    src/chunkserver/mutation/mutation.cpp
    src/chunkserver/mutation/mutation_manager.cpp

    # --------------------------------------------------------
    # CHUNKSERVER — HEARTBEAT
    # --------------------------------------------------------

    src/chunkserver/heartbeat/heartbeat_client.cpp

    # --------------------------------------------------------
    # CHUNKSERVER — REPLICATION
    # --------------------------------------------------------

    src/chunkserver/replication/replica_receiver.cpp
    src/chunkserver/replication/replica_sender.cpp
    src/chunkserver/replication/clone_manager.cpp

    # --------------------------------------------------------
    # CHUNKSERVER
    # --------------------------------------------------------

    src/chunkserver/chunkserver.cpp

    # --------------------------------------------------------
    # STORAGE
    # --------------------------------------------------------

    src/storage/local_filesystem.cpp
    src/storage/file_handle.cpp

    # --------------------------------------------------------
    # CLIENT — METADATA
    # --------------------------------------------------------

    src/client/metadata/master_client.cpp
    src/client/metadata/chunk_location_cache.cpp
    src/client/retry/retry_policy.cpp

    # --------------------------------------------------------
    # CLIENT — IO
    # --------------------------------------------------------

    src/client/io/reader.cpp
    src/client/io/writer.cpp
    src/client/io/record_appender.cpp

    # --------------------------------------------------------
    # CLIENT
    # --------------------------------------------------------

    src/client/gfs_client.cpp
)

# ============================================================
# GFS CORE INCLUDE DIRECTORIES
# ============================================================

target_include_directories(
    gfs_core
    PUBLIC

        ${CMAKE_CURRENT_SOURCE_DIR}/include
        ${GENERATED_DIR}
)

# ============================================================
# GFS CORE LINKING
# ============================================================

target_link_libraries(
    gfs_core
    PUBLIC

        gfs_protocol
)

target_compile_features(
    gfs_core
    PUBLIC

        cxx_std_20
)

# ============================================================
# COMPILER WARNINGS — CORE
# ============================================================

if(CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang")

    target_compile_options(
        gfs_core
        PRIVATE

            -Wall
            -Wextra
            -Wpedantic
            -Wconversion
            -Wshadow
    )

endif()

# ============================================================
# MAIN EXECUTABLE
# ============================================================

add_executable(
    gfs

    src/main.cpp
)

target_link_libraries(
    gfs
    PRIVATE

        gfs_core
)

target_include_directories(
    gfs
    PRIVATE

        ${CMAKE_CURRENT_SOURCE_DIR}/include
        ${GENERATED_DIR}
)

# ============================================================
# TESTING
# ============================================================

enable_testing()

# ============================================================
# COMMON TEST
# ============================================================

add_executable(
    gfs_common_test

    tests/common_test.cpp
)

target_link_libraries(
    gfs_common_test
    PRIVATE

        gfs_core
        GTest::gtest_main
)

# ============================================================
# MASTER TEST
# ============================================================

add_executable(
    gfs_master_test

    tests/master_test.cpp
)

target_link_libraries(
    gfs_master_test
    PRIVATE

        gfs_core
        GTest::gtest_main
)

# ============================================================
# CHUNKSERVER TEST
# ============================================================

add_executable(
    gfs_chunkserver_test

    tests/chunkserver_test.cpp
)

target_link_libraries(
    gfs_chunkserver_test
    PRIVATE

        gfs_core
        GTest::gtest_main
)

# ============================================================
# CLIENT TEST
# ============================================================

add_executable(
    gfs_client_test

    tests/client_test.cpp
)

target_link_libraries(
    gfs_client_test
    PRIVATE

        gfs_core
        GTest::gtest_main
)

# ============================================================
# REPLICATION TEST
# ============================================================

add_executable(
    gfs_replication_test

    tests/replication_test.cpp
)

target_link_libraries(
    gfs_replication_test
    PRIVATE

        gfs_core
        GTest::gtest_main
)

# ============================================================
# PHASE 7 TEST
# ============================================================

add_executable(
    gfs_phase7_test

    tests/phase7_test.cpp
)

target_link_libraries(
    gfs_phase7_test
    PRIVATE

        gfs_core
        GTest::gtest_main
)

# ============================================================
# PHASE 8 TEST
# ============================================================

add_executable(
    gfs_phase8_test

    tests/phase8_test.cpp
)

target_link_libraries(
    gfs_phase8_test
    PRIVATE

        gfs_core
        GTest::gtest_main
)

# ============================================================
# PHASE 9 TEST
# ============================================================

add_executable(
    gfs_phase9_test

    tests/phase9_test.cpp
)

target_link_libraries(
    gfs_phase9_test
    PRIVATE

        gfs_core
        GTest::gtest_main
)

# ============================================================
# PHASE 10 TEST
# ============================================================

add_executable(
    gfs_phase10_test

    tests/phase10_test.cpp
)

target_link_libraries(
    gfs_phase10_test
    PRIVATE

        gfs_core
        GTest::gtest_main
)

# ============================================================
# PHASE 11 TEST
# Operation Log + Checkpoint + Master Recovery
# ============================================================

add_executable(
    gfs_phase11_test

    tests/phase11_test.cpp
)

target_link_libraries(
    gfs_phase11_test
    PRIVATE

        gfs_core
        GTest::gtest_main
)

# ============================================================
# PHASE 12 TEST
# Record Append
# ============================================================

add_executable(
    gfs_phase12_test

    tests/phase12_test.cpp
)

target_link_libraries(
    gfs_phase12_test
    PRIVATE

        gfs_core
        GTest::gtest_main
)

# ============================================================
# PHASE 13 TEST
# Snapshot / Copy-On-Write
# ============================================================

add_executable(
    gfs_phase13_test

    tests/phase13_test.cpp
)

target_link_libraries(
    gfs_phase13_test
    PRIVATE

        gfs_core
        GTest::gtest_main
)

# ============================================================
# PHASE 14 TEST
# Garbage Collection + Rebalancing
# ============================================================

add_executable(
    gfs_phase14_test

    tests/phase14_test.cpp
)

target_link_libraries(
    gfs_phase14_test
    PRIVATE

        gfs_core
        GTest::gtest_main
)

# ============================================================
# PHASE 15 TEST
# Failure Injection + Distributed Testing
# ============================================================

add_executable(
    gfs_phase15_test

    tests/phase15_test.cpp
)

target_link_libraries(
    gfs_phase15_test
    PRIVATE

        gfs_core
        GTest::gtest_main
)

# ============================================================
# TEST CONFIGURATION
# ============================================================

foreach(
    TEST_TARGET

        gfs_common_test
        gfs_master_test
        gfs_chunkserver_test
        gfs_client_test
        gfs_replication_test
        gfs_phase7_test
        gfs_phase8_test
        gfs_phase9_test
        gfs_phase10_test
        gfs_phase11_test
        gfs_phase12_test
        gfs_phase13_test
        gfs_phase14_test
        gfs_phase15_test
)

    target_include_directories(
        ${TEST_TARGET}
        PRIVATE

            ${CMAKE_CURRENT_SOURCE_DIR}/include
            ${GENERATED_DIR}
    )

    if(CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang")

        target_compile_options(
            ${TEST_TARGET}
            PRIVATE

                -Wall
                -Wextra
                -Wpedantic
                -Wconversion
                -Wshadow
        )

    endif()

    gtest_discover_tests(
        ${TEST_TARGET}
    )

endforeach()