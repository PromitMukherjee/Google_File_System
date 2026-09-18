#pragma once

#include "gfs/common/types.hpp"

#include <cstddef>
#include <cstdint>
#include <functional>
#include <string>
#include <vector>

namespace gfs::benchmark {

struct BenchmarkResult {
    std::string benchmark;
    std::string configuration;

    std::size_t clients = 0;
    std::size_t chunkservers = 0;
    std::uint32_t replication_factor = 0;

    std::uint64_t bytes = 0;
    std::uint64_t operations = 0;

    double elapsed_seconds = 0.0;
    double throughput_mb_per_sec = 0.0;
    double operations_per_sec = 0.0;

    std::size_t successes = 0;
    std::size_t failures = 0;

    std::string paper_reference;
    std::string paper_value;
    std::string notes;

    [[nodiscard]] static double ThroughputMbPerSec(
        std::uint64_t bytes,
        double elapsed_seconds) noexcept;

    [[nodiscard]] static double OperationsPerSec(
        std::uint64_t operations,
        double elapsed_seconds) noexcept;
};

class BenchmarkRunner {
public:
    using Operation = std::function<bool()>;

    [[nodiscard]] BenchmarkResult Run(
        std::string name,
        std::string configuration,
        std::size_t clients,
        std::size_t chunkservers,
        std::uint32_t replication_factor,
        std::uint64_t bytes,
        std::uint64_t operations,
        const Operation& operation,
        std::string paper_reference = {},
        std::string paper_value = {},
        std::string notes = {}) const;
};

class CsvWriter {
public:
    [[nodiscard]] static bool Write(
        const std::string& path,
        const std::vector<BenchmarkResult>& results);
};

struct BenchmarkConfig {
    std::vector<std::size_t> client_counts{
        1, 2, 4, 8, 16};

    std::vector<std::size_t> chunkserver_counts{
        3, 4, 8, 16};

    std::vector<std::uint32_t> replication_factors{
        1, 2, 3};

    std::size_t read_file_size_mb = 128;
    std::size_t read_region_mb = 4;
    std::size_t read_iterations = 8;

    std::size_t write_file_size_mb = 32;
    std::size_t write_operation_mb = 1;

    std::size_t append_record_size_bytes = 4096;
    std::size_t append_records_per_client = 8;

    std::size_t master_operations = 1000;

    std::size_t recovery_chunk_count = 4;
    std::size_t recovery_chunk_size_mb = 1;

    std::size_t max_chunkservers = 16;
    std::size_t max_clients = 16;
};

class BenchmarkSuite {
public:
    [[nodiscard]] static std::vector<BenchmarkResult>
    RunAll(const BenchmarkConfig& config);

    [[nodiscard]] static std::vector<BenchmarkResult>
    RunRead(const BenchmarkConfig& config);

    [[nodiscard]] static std::vector<BenchmarkResult>
    RunWrite(const BenchmarkConfig& config);

    [[nodiscard]] static std::vector<BenchmarkResult>
    RunRecordAppend(const BenchmarkConfig& config);

    [[nodiscard]] static std::vector<BenchmarkResult>
    RunChunkserverScaling(
        const BenchmarkConfig& config);

    [[nodiscard]] static std::vector<BenchmarkResult>
    RunMaster(const BenchmarkConfig& config);

    [[nodiscard]] static std::vector<BenchmarkResult>
    RunRecovery(const BenchmarkConfig& config);

    [[nodiscard]] static std::vector<BenchmarkResult>
    RunReplication(const BenchmarkConfig& config);
};

[[nodiscard]] std::string EnvironmentSummary();

[[nodiscard]] std::string BuildPhase16Report(
    const std::vector<BenchmarkResult>& results,
    const BenchmarkConfig& config);

}  // namespace gfs::benchmark