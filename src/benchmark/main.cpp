#include "gfs/benchmark/benchmark.hpp"

#include <cstdint>
#include <fstream>
#include <iostream>
#include <string>
#include <vector>

namespace {

bool ParseSize(
    const std::string& value,
    std::size_t& result) {
    try {
        std::size_t position = 0;

        const auto parsed =
            std::stoull(
                value,
                &position);

        if (position != value.size()) {
            return false;
        }

        result =
            static_cast<std::size_t>(
                parsed);

        return true;
    } catch (...) {
        return false;
    }
}

bool ParseSizeList(
    const std::string& value,
    std::vector<std::size_t>& result) {
    result.clear();

    std::size_t start = 0;

    while (start < value.size()) {
        const std::size_t comma =
            value.find(',', start);

        const std::string token =
            value.substr(
                start,
                comma == std::string::npos
                    ? std::string::npos
                    : comma - start);

        std::size_t parsed = 0;

        if (!ParseSize(token, parsed) ||
            parsed == 0) {
            return false;
        }

        result.push_back(parsed);

        if (comma ==
            std::string::npos) {
            break;
        }

        start = comma + 1;
    }

    return !result.empty();
}

void PrintUsage(
    const char* program) {
    std::cout
        << "Usage: "
        << program
        << " [options]\n"
        << "  --clients N[,N...]\n"
        << "  --chunkservers N[,N...]\n"
        << "  --replication N[,N...]\n"
        << "  --output PATH\n"
        << "  --report PATH\n"
        << "  --quick\n"
        << "  --help\n";
}

}  // namespace

int main(
    int argc,
    char** argv) {
    gfs::benchmark::BenchmarkConfig
        config;

    std::string output =
        "phase16_benchmark_results.csv";

    std::string report =
        "docs/phase16_benchmark_report.md";

    for (int index = 1;
         index < argc;
         ++index) {
        const std::string argument =
            argv[index];

        auto next_value =
            [&]() -> const char* {
                if (index + 1 >= argc) {
                    return nullptr;
                }

                return argv[++index];
            };

        if (argument == "--help") {
            PrintUsage(argv[0]);
            return 0;
        }

        if (argument == "--quick") {
            config.client_counts =
                {1, 2, 4};

            config.chunkserver_counts =
                {3};

            config.replication_factors =
                {1, 2, 3};

            config.read_file_size_mb = 32;
            config.read_iterations = 2;

            config.write_file_size_mb = 8;

            config.append_records_per_client =
                2;

            config.master_operations = 100;

            config.recovery_chunk_count = 2;

            continue;
        }

        if (argument == "--clients") {
            const char* value =
                next_value();

            if (value == nullptr ||
                !ParseSizeList(
                    value,
                    config.client_counts)) {
                std::cerr
                    << "Invalid --clients value\n";
                return 2;
            }

            continue;
        }

        if (argument == "--chunkservers") {
            const char* value =
                next_value();

            if (value == nullptr ||
                !ParseSizeList(
                    value,
                    config.chunkserver_counts)) {
                std::cerr
                    << "Invalid --chunkservers value\n";
                return 2;
            }

            continue;
        }

        if (argument == "--replication") {
            const char* value =
                next_value();

            std::vector<std::size_t>
                parsed;

            if (value == nullptr ||
                !ParseSizeList(
                    value,
                    parsed)) {
                std::cerr
                    << "Invalid --replication value\n";
                return 2;
            }

            config.replication_factors.clear();

            for (const std::size_t factor :
                 parsed) {
                if (factor >
                    static_cast<std::size_t>(
                        UINT32_MAX)) {
                    std::cerr
                        << "Replication factor out of range\n";
                    return 2;
                }

                config.replication_factors.push_back(
                    static_cast<std::uint32_t>(
                        factor));
            }

            continue;
        }

        if (argument == "--output") {
            const char* value =
                next_value();

            if (value == nullptr) {
                std::cerr
                    << "Missing --output value\n";
                return 2;
            }

            output = value;
            continue;
        }

        if (argument == "--report") {
            const char* value =
                next_value();

            if (value == nullptr) {
                std::cerr
                    << "Missing --report value\n";
                return 2;
            }

            report = value;
            continue;
        }

        std::cerr
            << "Unknown option: "
            << argument
            << '\n';

        return 2;
    }

    config.max_clients = 16;
    config.max_chunkservers = 16;

    const auto results =
        gfs::benchmark::BenchmarkSuite::
            RunAll(config);

    if (!gfs::benchmark::CsvWriter::Write(
            output,
            results)) {
        std::cerr
            << "Failed to write CSV: "
            << output
            << '\n';

        return 1;
    }

    const std::string report_text =
        gfs::benchmark::BuildPhase16Report(
            results,
            config);

    std::ofstream report_file(report);

    if (!report_file.is_open()) {
        std::cerr
            << "Failed to write report: "
            << report
            << '\n';

        return 1;
    }

    report_file << report_text;

    std::cout
        << "Phase 16 benchmarks completed: "
        << results.size()
        << " results\n"
        << "CSV: "
        << output
        << '\n'
        << "Report: "
        << report
        << '\n';

    return 0;
}