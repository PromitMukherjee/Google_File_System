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

add_library(gfs_core
    src/common/types.cpp
    src/common/status.cpp
    src/common/utils.cpp
)

target_include_directories(gfs_core
    PUBLIC
        ${PROJECT_SOURCE_DIR}/include
)

target_compile_features(gfs_core PUBLIC cxx_std_20)

if(CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang")
    target_compile_options(gfs_core PRIVATE
        -Wall
        -Wextra
        -Wpedantic
    )
endif()

add_executable(gfs
    src/main.cpp
)

target_link_libraries(gfs
    PRIVATE
        gfs_core
)

target_compile_features(gfs PRIVATE cxx_std_20)

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

target_compile_features(gfs_common_test PRIVATE cxx_std_20)

include(GoogleTest)

gtest_discover_tests(gfs_common_test)