cmake_minimum_required(VERSION 3.20)

project(GFS_CPP
    VERSION 0.1.0
    DESCRIPTION "Paper-Faithful Reproduction of the Google File System in C++"
    LANGUAGES CXX
)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

include(GNUInstallDirs)
include(FetchContent)

# ============================================================
# Protobuf
# ============================================================

find_package(Protobuf REQUIRED)

if(TARGET protobuf::protoc)
    set(GFS_PROTOC_TARGET protobuf::protoc)
    set(GFS_PROTOC_EXECUTABLE "$<TARGET_FILE:protobuf::protoc>")
elseif(Protobuf_PROTOC_EXECUTABLE)
    set(GFS_PROTOC_TARGET "")
    set(GFS_PROTOC_EXECUTABLE "${Protobuf_PROTOC_EXECUTABLE}")
else()
    find_program(GFS_PROTOC_EXECUTABLE protoc REQUIRED)
    set(GFS_PROTOC_TARGET "")
endif()

if(TARGET protobuf::libprotobuf)
    set(GFS_PROTOBUF_LIBRARY protobuf::libprotobuf)
else()
    set(GFS_PROTOBUF_LIBRARY "${Protobuf_LIBRARIES}")
endif()

# ============================================================
# gRPC
# ============================================================

find_package(gRPC CONFIG QUIET)

if(TARGET gRPC::grpc++)
    set(GFS_GRPC_LIBRARY gRPC::grpc++)
else()
    find_library(GFS_GRPC_LIBRARY
        NAMES grpc++
        REQUIRED
    )

    find_library(GFS_GRPC_CORE_LIBRARY
        NAMES grpc
        REQUIRED
    )

    find_library(GFS_GPR_LIBRARY
        NAMES gpr
        REQUIRED
    )

    find_library(GFS_ADDRESS_SORTING_LIBRARY
        NAMES address_sorting
        REQUIRED
    )

    find_library(GFS_UPB_LIBRARY
        NAMES upb
        REQUIRED
    )

    find_library(GFS_RE2_LIBRARY
        NAMES re2
        REQUIRED
    )

    find_library(GFS_CARES_LIBRARY
        NAMES cares
        REQUIRED
    )

    find_library(GFS_ABSL_STRINGS_LIBRARY
        NAMES absl_strings
        REQUIRED
    )

    find_library(GFS_ABSL_BASE_LIBRARY
        NAMES absl_base
        REQUIRED
    )

    find_library(GFS_ABSL_SYNCHRONIZATION_LIBRARY
        NAMES absl_synchronization
        REQUIRED
    )

    find_library(GFS_ABSL_TIME_LIBRARY
        NAMES absl_time
        REQUIRED
    )

    find_library(GFS_ABSL_STATUS_LIBRARY
        NAMES absl_status
        REQUIRED
    )

    find_library(GFS_ABSL_STATUSOR_LIBRARY
        NAMES absl_statusor
        REQUIRED
    )

    add_library(gfs_grpc_ubuntu INTERFACE)

    target_link_libraries(gfs_grpc_ubuntu
        INTERFACE
            ${GFS_GRPC_LIBRARY}
            ${GFS_GRPC_CORE_LIBRARY}
            ${GFS_GPR_LIBRARY}
            ${GFS_ADDRESS_SORTING_LIBRARY}
            ${GFS_UPB_LIBRARY}
            ${GFS_RE2_LIBRARY}
            ${GFS_CARES_LIBRARY}
            ${GFS_ABSL_STRINGS_LIBRARY}
            ${GFS_ABSL_BASE_LIBRARY}
            ${GFS_ABSL_SYNCHRONIZATION_LIBRARY}
            ${GFS_ABSL_TIME_LIBRARY}
            ${GFS_ABSL_STATUS_LIBRARY}
            ${GFS_ABSL_STATUSOR_LIBRARY}
    )

    set(GFS_GRPC_LIBRARY gfs_grpc_ubuntu)
endif()

find_program(
    GFS_GRPC_CPP_PLUGIN
    grpc_cpp_plugin
    REQUIRED
)

# ============================================================
# Protobuf / gRPC sources
# ============================================================

set(PROTO_FILES
    ${PROJECT_SOURCE_DIR}/proto/master.proto
    ${PROJECT_SOURCE_DIR}/proto/chunkserver.proto
    ${PROJECT_SOURCE_DIR}/proto/client.proto
)

set(PROTO_GEN_DIR
    ${CMAKE_CURRENT_BINARY_DIR}/generated
)

file(MAKE_DIRECTORY ${PROTO_GEN_DIR})

set(GENERATED_PROTO_SRCS)
set(GENERATED_PROTO_HDRS)

foreach(PROTO_FILE ${PROTO_FILES})

    get_filename_component(PROTO_NAME ${PROTO_FILE} NAME_WE)

    set(PROTO_SRC
        ${PROTO_GEN_DIR}/${PROTO_NAME}.pb.cc
    )

    set(PROTO_HDR
        ${PROTO_GEN_DIR}/${PROTO_NAME}.pb.h
    )

    set(GRPC_SRC
        ${PROTO_GEN_DIR}/${PROTO_NAME}.grpc.pb.cc
    )

    set(GRPC_HDR
        ${PROTO_GEN_DIR}/${PROTO_NAME}.grpc.pb.h
    )

    if(GFS_PROTOC_TARGET)

        add_custom_command(
            OUTPUT
                ${PROTO_SRC}
                ${PROTO_HDR}
                ${GRPC_SRC}
                ${GRPC_HDR}

            COMMAND
                ${GFS_PROTOC_TARGET}

            ARGS
                --proto_path=${PROJECT_SOURCE_DIR}/proto
                --cpp_out=${PROTO_GEN_DIR}
                --grpc_out=${PROTO_GEN_DIR}
                --plugin=protoc-gen-grpc=${GFS_GRPC_CPP_PLUGIN}
                ${PROTO_FILE}

            DEPENDS
                ${PROTO_FILE}
                ${GFS_PROTOC_TARGET}

            COMMENT
                "Generating C++ protobuf/gRPC sources for ${PROTO_NAME}.proto"

            VERBATIM
        )

    else()

        add_custom_command(
            OUTPUT
                ${PROTO_SRC}
                ${PROTO_HDR}
                ${GRPC_SRC}
                ${GRPC_HDR}

            COMMAND
                ${GFS_PROTOC_EXECUTABLE}

            ARGS
                --proto_path=${PROJECT_SOURCE_DIR}/proto
                --cpp_out=${PROTO_GEN_DIR}
                --grpc_out=${PROTO_GEN_DIR}
                --plugin=protoc-gen-grpc=${GFS_GRPC_CPP_PLUGIN}
                ${PROTO_FILE}

            DEPENDS
                ${PROTO_FILE}

            COMMENT
                "Generating C++ protobuf/gRPC sources for ${PROTO_NAME}.proto"

            VERBATIM
        )

    endif()

    list(APPEND GENERATED_PROTO_SRCS
        ${PROTO_SRC}
        ${GRPC_SRC}
    )

    list(APPEND GENERATED_PROTO_HDRS
        ${PROTO_HDR}
        ${GRPC_HDR}
    )

endforeach()

set_source_files_properties(
    ${GENERATED_PROTO_SRCS}
    ${GENERATED_PROTO_HDRS}
    PROPERTIES
        GENERATED TRUE
)

# ============================================================
# Protocol library
# ============================================================

add_library(gfs_protocol
    ${GENERATED_PROTO_SRCS}
    ${GENERATED_PROTO_HDRS}
)

target_include_directories(gfs_protocol
    PUBLIC
        ${PROTO_GEN_DIR}
        ${Protobuf_INCLUDE_DIRS}
)

target_link_libraries(gfs_protocol
    PUBLIC
        ${GFS_PROTOBUF_LIBRARY}
        ${GFS_GRPC_LIBRARY}
)

target_compile_features(gfs_protocol
    PUBLIC
        cxx_std_20
)

# ============================================================
# Core library
# ============================================================

add_library(gfs_core
    src/common/types.cpp
    src/common/status.cpp
    src/common/utils.cpp
    src/protocol/master_service.cpp
    src/protocol/chunkserver_service.cpp
    src/protocol/client_service.cpp
)

target_include_directories(gfs_core
    PUBLIC
        ${PROJECT_SOURCE_DIR}/include
        ${PROTO_GEN_DIR}
        ${Protobuf_INCLUDE_DIRS}
)

target_link_libraries(gfs_core
    PUBLIC
        gfs_protocol
)

target_compile_features(gfs_core
    PUBLIC
        cxx_std_20
)

if(CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang")
    target_compile_options(gfs_core
        PRIVATE
            -Wall
            -Wextra
            -Wpedantic
    )
endif()

# ============================================================
# Main executable
# ============================================================

add_executable(gfs
    src/main.cpp
)

target_link_libraries(gfs
    PRIVATE
        gfs_core
)

target_compile_features(gfs
    PRIVATE
        cxx_std_20
)

# ============================================================
# Tests
# ============================================================

enable_testing()

FetchContent_Declare(
    googletest
    URL https://github.com/google/googletest/archive/refs/tags/v1.17.0.zip
)

set(INSTALL_GTEST OFF CACHE BOOL "" FORCE)
set(BUILD_GMOCK OFF CACHE BOOL "" FORCE)

FetchContent_MakeAvailable(googletest)

add_executable(gfs_common_test
    tests/common_test.cpp
)

target_link_libraries(gfs_common_test
    PRIVATE
        gfs_core
        GTest::gtest_main
)

target_compile_features(gfs_common_test
    PRIVATE
        cxx_std_20
)

include(GoogleTest)

gtest_discover_tests(gfs_common_test)