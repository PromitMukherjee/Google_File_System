#include "gfs/benchmark/benchmark.hpp"

#include <gtest/gtest.h>

#include <filesystem>
#include <fstream>
#include <string>

namespace {

TEST(
    Phase16BenchmarkTest,
    BenchmarkResultCalculatesThroughput) {
    EXPECT_DOUBLE_EQ(
        gfs::benchmark::BenchmarkResult::
            ThroughputMbPerSec(
                10ULL * 1024ULL * 1024ULL,
                2.0),
        5.0);
}

TEST(
    Phase16BenchmarkTest,
    BenchmarkResultCalculatesOperationsPerSecond) {
    EXPECT_DOUBLE_EQ(
        gfs::benchmark::BenchmarkResult::
            OperationsPerSec(
                100,
                4.0),
        25.0);
}

TEST(
    Phase16BenchmarkTest,
    BenchmarkRunnerExecutesBenchmark) {
    gfs::benchmark::BenchmarkRunner
        runner;

    bool executed = false;

    const auto result =
        runner.Run(
            "smoke",
            "clients=1",
            1,
            1,
            1,
            1024,
            2,
            [&] {
                executed = true;
                return true;
            });

    EXPECT_TRUE(executed);
    EXPECT_EQ(
        result.successes,
        1U);
    EXPECT_EQ(
        result.failures,
        0U);
    EXPECT_EQ(
        result.operations,
        2U);
    EXPECT_GT(
        result.elapsed_seconds,
        0.0);
    EXPECT_GT(
        result.throughput_mb_per_sec,
        0.0);
    EXPECT_GT(
        result.operations_per_sec,
        0.0);
}

TEST(
    Phase16BenchmarkTest,
    CsvExportProducesExpectedColumns) {
    const auto path =
        std::filesystem::temp_directory_path() /
        "gfs_phase16_test.csv";

    gfs::benchmark::BenchmarkResult
        result;

    result.benchmark =
        "csv_smoke";

    result.configuration =
        "clients=1";

    result.clients = 1;
    result.chunkservers = 3;
    result.replication_factor = 2;
    result.bytes = 1024;
    result.operations = 1;
    result.elapsed_seconds = 1.0;
    result.successes = 1;

    ASSERT_TRUE(
        gfs::benchmark::CsvWriter::Write(
            path.string(),
            {result}));

    std::ifstream input(path);

    ASSERT_TRUE(input.is_open());

    std::string header;

    ASSERT_TRUE(
        static_cast<bool>(
            std::getline(
                input,
                header)));

    EXPECT_NE(
        header.find("benchmark"),
        std::string::npos);

    EXPECT_NE(
        header.find(
            "throughput_mb_per_sec"),
        std::string::npos);

    EXPECT_NE(
        header.find(
            "paper_reference"),
        std::string::npos);

    std::error_code error;

    std::filesystem::remove(
        path,
        error);
}

TEST(
    Phase16BenchmarkTest,
    EnvironmentSummaryIsNotEmpty) {
    EXPECT_FALSE(
        gfs::benchmark::
            EnvironmentSummary()
            .empty());
}

TEST(
    Phase16BenchmarkTest,
    ReportContainsPaperLabels) {
    gfs::benchmark::BenchmarkConfig
        config;

    const auto report =
        gfs::benchmark::BuildPhase16Report(
            {},
            config);

    EXPECT_NE(
        report.find(
            "PAPER_REPORTED"),
        std::string::npos);

    EXPECT_NE(
        report.find(
            "REPRODUCTION_MEASURED"),
        std::string::npos);

    EXPECT_NE(
        report.find(
            "not hardware-equivalent"),
        std::string::npos);
}

TEST(
    Phase16BenchmarkTest,
    ReadBenchmarkProducesResult) {
    gfs::benchmark::BenchmarkConfig
        config;

    config.client_counts = {1};
    config.max_clients = 1;
    config.max_chunkservers = 3;
    config.read_file_size_mb = 8;
    config.read_region_mb = 1;
    config.read_iterations = 1;

    const auto results =
        gfs::benchmark::BenchmarkSuite::
            RunRead(config);

    ASSERT_EQ(
        results.size(),
        1U);

    EXPECT_EQ(
        results.front().benchmark,
        "sequential_read");

    EXPECT_EQ(
        results.front().successes,
        1U);
}

TEST(
    Phase16BenchmarkTest,
    WriteBenchmarkProducesResult) {
    gfs::benchmark::BenchmarkConfig
        config;

    config.client_counts = {1};
    config.max_clients = 1;
    config.max_chunkservers = 3;
    config.write_file_size_mb = 2;
    config.write_operation_mb = 1;

    const auto results =
        gfs::benchmark::BenchmarkSuite::
            RunWrite(config);

    ASSERT_EQ(
        results.size(),
        1U);

    EXPECT_EQ(
        results.front().benchmark,
        "sequential_write");

    EXPECT_EQ(
        results.front().successes,
        1U);
}

TEST(
    Phase16BenchmarkTest,
    RecordAppendBenchmarkProducesResult) {
    gfs::benchmark::BenchmarkConfig
        config;

    config.client_counts = {1};
    config.max_clients = 1;
    config.max_chunkservers = 3;
    config.append_records_per_client = 1;
    config.append_record_size_bytes = 1024;

    const auto results =
        gfs::benchmark::BenchmarkSuite::
            RunRecordAppend(config);

    ASSERT_EQ(
        results.size(),
        1U);

    EXPECT_EQ(
        results.front().benchmark,
        "record_append");

    EXPECT_EQ(
        results.front().successes,
        1U);
}

TEST(
    Phase16BenchmarkTest,
    MasterBenchmarkProducesResult) {
    gfs::benchmark::BenchmarkConfig
        config;

    config.master_operations = 10;

    const auto results =
        gfs::benchmark::BenchmarkSuite::
            RunMaster(config);

    ASSERT_EQ(
        results.size(),
        1U);

    EXPECT_EQ(
        results.front().benchmark,
        "master_metadata_lookup");

    EXPECT_EQ(
        results.front().successes,
        1U);
}

TEST(
    Phase16BenchmarkTest,
    RecoveryBenchmarkProducesResult) {
    gfs::benchmark::BenchmarkConfig
        config;

    config.recovery_chunk_count = 1;
    config.recovery_chunk_size_mb = 1;

    const auto results =
        gfs::benchmark::BenchmarkSuite::
            RunRecovery(config);

    ASSERT_EQ(
        results.size(),
        1U);

    EXPECT_EQ(
        results.front().benchmark,
        "recovery_rereplication");

    EXPECT_EQ(
        results.front().successes,
        1U);
}

}  // namespace