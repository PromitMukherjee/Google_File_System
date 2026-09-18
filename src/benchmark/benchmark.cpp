// PATH: src/benchmark/benchmark.cpp

#include "gfs/benchmark/benchmark.hpp"

#include "gfs/chunkserver/mutation/mutation_manager.hpp"
#include "gfs/common/constants.hpp"
#include "gfs/testing/distributed_test_harness.hpp"

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <iterator>
#include <mutex>
#include <sstream>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#if defined(__linux__)
#include <sys/sysinfo.h>
#endif

namespace gfs::benchmark {
namespace {

using Clock = std::chrono::steady_clock;

constexpr double kMiB =
    1024.0 * 1024.0;

std::string CsvEscape(
    const std::string& value) {
    if (value.find_first_of(",\"\n") ==
        std::string::npos) {
        return value;
    }

    std::string result = "\"";

    for (const char character : value) {
        if (character == '"') {
            result += "\"\"";
        } else {
            result += character;
        }
    }

    result += '"';
    return result;
}

std::string Configuration(
    std::size_t clients,
    std::size_t chunkservers,
    std::uint32_t replication) {
    std::ostringstream stream;

    stream << "clients=" << clients
           << ";chunkservers=" << chunkservers
           << ";replication=" << replication;

    return stream.str();
}

std::string GenerateData(
    std::size_t size,
    std::uint8_t seed) {
    std::string data(size, '\0');

    for (std::size_t index = 0;
         index < size;
         ++index) {
        data[index] = static_cast<char>(
            static_cast<unsigned int>(seed) +
            static_cast<unsigned int>(
                index % 251U));
    }

    return data;
}

bool ConfigureHarness(
    testing::DistributedTestHarness& harness,
    std::size_t chunkservers) {
    if (!harness.Initialize()) {
        return false;
    }

    for (std::size_t index = 1;
         index <= chunkservers;
         ++index) {
        const ServerId server_id =
            static_cast<ServerId>(index);

        if (!harness.AddChunkserver(server_id)) {
            return false;
        }

        if (!harness.SendHeartbeat(
                server_id,
                1000)) {
            return false;
        }
    }

    return true;
}

bool PopulateFile(
    client::GFSClient& client,
    const std::string& path,
    std::size_t size,
    std::size_t operation_size) {
    if (!client.CreateFile(path)) {
        return false;
    }

    const std::string data =
        GenerateData(operation_size, 17);

    std::size_t written = 0;

    while (written < size) {
        const std::size_t amount =
            std::min(
                operation_size,
                size - written);

        if (!client.Write(
                path,
                static_cast<std::uint64_t>(
                    written),
                data.substr(0, amount))) {
            return false;
        }

        written += amount;
    }

    return true;
}

void ConfigureAppend(
    testing::DistributedTestHarness& harness,
    client::GFSClient& client) {
    client.SetRecordAppendLeaseValidator(
        [&harness](
            ChunkHandle handle,
            ServerId server_id) {
            if (harness.GetMaster()
                    .IsLeaseValid(
                        handle,
                        server_id)) {
                return true;
            }

            return harness.GetMaster()
                .AcquireLease(
                    handle,
                    server_id)
                .has_value();
        });

    client.SetRecordAppendPrimarySelector(
        [&harness](
            ChunkHandle handle,
            const std::vector<
                client::metadata::ChunkLocation>&
                locations) {
            const auto primary =
                harness.GetMaster()
                    .GetPrimary(handle);

            if (primary.has_value()) {
                return primary;
            }

            if (!locations.empty()) {
                return std::optional<ServerId>(
                    locations.front().server_id);
            }

            return std::optional<ServerId>{};
        });

    client.SetRecordAppendFunction(
        [&harness](
            const client::metadata::ChunkLocation&
                primary_location,
            ChunkHandle handle,
            ChunkVersion version,
            const std::vector<
                client::metadata::ChunkLocation>&
                replicas,
            const std::string& data) {
            client::io::RecordAppender::AppendResult
                converted;

            auto* primary =
                harness.GetChunkserver(
                    primary_location.server_id);

            if (primary == nullptr) {
                return converted;
            }

            const auto result =
                primary->GetMutationManager()
                    .ExecutePrimaryRecordAppend(
                        handle,
                        version,
                        data,
                        [&harness,
                         &replicas,
                         &primary_location](
                            const chunkserver::mutation::
                                Mutation& mutation) {
                            for (const auto& replica :
                                 replicas) {
                                if (replica.server_id ==
                                    primary_location.server_id) {
                                    continue;
                                }

                                auto* server =
                                    harness.GetChunkserver(
                                        replica.server_id);

                                if (server == nullptr ||
                                    !server->GetMutationManager()
                                         .ApplyMutation(
                                             mutation)) {
                                    return false;
                                }
                            }

                            return true;
                        });

            converted.offset =
                result.offset;

            converted.chunk_size =
                result.chunk_size;

            converted.bytes_appended =
                result.bytes_appended;

            using Status =
                chunkserver::mutation::
                    MutationManager::
                        RecordAppendStatus;

            if (result.status ==
                Status::Success) {
                converted.status =
                    client::io::RecordAppender::
                        AppendStatus::Success;
            } else if (result.status ==
                       Status::RetryNextChunk) {
                converted.status =
                    client::io::RecordAppender::
                        AppendStatus::
                            RetryNextChunk;
            } else {
                converted.status =
                    client::io::RecordAppender::
                        AppendStatus::Failed;
            }

            return converted;
        });
}

BenchmarkResult RunConcurrent(
    const std::string& name,
    std::size_t clients,
    std::size_t chunkservers,
    std::uint32_t replication,
    std::uint64_t bytes,
    std::uint64_t operations,
    const std::function<bool(std::size_t)>&
        worker,
    const std::string& paper_reference,
    const std::string& paper_value,
    const std::string& notes) {
    BenchmarkResult result;

    result.benchmark = name;

    result.configuration =
        Configuration(
            clients,
            chunkservers,
            replication);

    result.clients = clients;

    result.chunkservers =
        chunkservers;

    result.replication_factor =
        replication;

    result.bytes = bytes;

    result.operations =
        operations;

    result.paper_reference =
        paper_reference;

    result.paper_value =
        paper_value;

    result.notes =
        notes;

    std::size_t successes = 0;
    std::size_t failures = 0;

    std::mutex result_mutex;

    const auto start =
        Clock::now();

    std::vector<std::jthread> threads;

    threads.reserve(clients);

    for (std::size_t index = 0;
         index < clients;
         ++index) {
        threads.emplace_back(
            [&, index] {
                const bool success =
                    worker(index);

                std::lock_guard lock(
                    result_mutex);

                if (success) {
                    ++successes;
                } else {
                    ++failures;
                }
            });
    }

    threads.clear();

    const auto end =
        Clock::now();

    result.elapsed_seconds =
        std::chrono::duration<double>(
            end - start)
            .count();

    result.successes =
        successes;

    result.failures =
        failures;

    result.throughput_mb_per_sec =
        BenchmarkResult::
            ThroughputMbPerSec(
                bytes,
                result.elapsed_seconds);

    result.operations_per_sec =
        BenchmarkResult::
            OperationsPerSec(
                operations,
                result.elapsed_seconds);

    return result;
}

bool ValidClients(
    std::size_t value,
    const BenchmarkConfig& config) {
    return value > 0 &&
           value <= config.max_clients;
}

bool ValidServers(
    std::size_t value,
    const BenchmarkConfig& config) {
    return value > 0 &&
           value <= config.max_chunkservers;
}

}  // namespace

double BenchmarkResult::ThroughputMbPerSec(
    std::uint64_t bytes,
    double elapsed_seconds) noexcept {
    if (elapsed_seconds <= 0.0) {
        return 0.0;
    }

    return static_cast<double>(bytes) /
           kMiB /
           elapsed_seconds;
}

double BenchmarkResult::OperationsPerSec(
    std::uint64_t operations,
    double elapsed_seconds) noexcept {
    if (elapsed_seconds <= 0.0) {
        return 0.0;
    }

    return static_cast<double>(operations) /
           elapsed_seconds;
}

BenchmarkResult BenchmarkRunner::Run(
    std::string name,
    std::string configuration,
    std::size_t clients,
    std::size_t chunkservers,
    std::uint32_t replication_factor,
    std::uint64_t bytes,
    std::uint64_t operations,
    const Operation& operation,
    std::string paper_reference,
    std::string paper_value,
    std::string notes) const {
    BenchmarkResult result;

    result.benchmark =
        std::move(name);

    result.configuration =
        std::move(configuration);

    result.clients =
        clients;

    result.chunkservers =
        chunkservers;

    result.replication_factor =
        replication_factor;

    result.bytes =
        bytes;

    result.operations =
        operations;

    result.paper_reference =
        std::move(paper_reference);

    result.paper_value =
        std::move(paper_value);

    result.notes =
        std::move(notes);

    const auto start =
        Clock::now();

    const bool success =
        operation &&
        operation();

    const auto end =
        Clock::now();

    result.elapsed_seconds =
        std::chrono::duration<double>(
            end - start)
            .count();

    result.successes =
        success ? 1U : 0U;

    result.failures =
        success ? 0U : 1U;

    result.throughput_mb_per_sec =
        BenchmarkResult::
            ThroughputMbPerSec(
                bytes,
                result.elapsed_seconds);

    result.operations_per_sec =
        BenchmarkResult::
            OperationsPerSec(
                operations,
                result.elapsed_seconds);

    return result;
}

bool CsvWriter::Write(
    const std::string& path,
    const std::vector<BenchmarkResult>&
        results) {
    std::ofstream output(path);

    if (!output.is_open()) {
        return false;
    }

    output
        << "benchmark,"
           "configuration,"
           "clients,"
           "chunkservers,"
           "replication_factor,"
           "bytes,"
           "operations,"
           "elapsed_seconds,"
           "throughput_mb_per_sec,"
           "operations_per_sec,"
           "successes,"
           "failures,"
           "paper_reference,"
           "paper_value,"
           "notes\n";

    output << std::setprecision(12);

    for (const auto& result : results) {
        output
            << CsvEscape(
                result.benchmark)
            << ','
            << CsvEscape(
                result.configuration)
            << ','
            << result.clients
            << ','
            << result.chunkservers
            << ','
            << result.replication_factor
            << ','
            << result.bytes
            << ','
            << result.operations
            << ','
            << result.elapsed_seconds
            << ','
            << result.throughput_mb_per_sec
            << ','
            << result.operations_per_sec
            << ','
            << result.successes
            << ','
            << result.failures
            << ','
            << CsvEscape(
                result.paper_reference)
            << ','
            << CsvEscape(
                result.paper_value)
            << ','
            << CsvEscape(
                result.notes)
            << '\n';
    }

    return output.good();
}

std::vector<BenchmarkResult>
BenchmarkSuite::RunRead(
    const BenchmarkConfig& config) {
    std::vector<BenchmarkResult> results;

    const std::size_t file_size =
        config.read_file_size_mb *
        1024U *
        1024U;

    const std::size_t region_size =
        config.read_region_mb *
        1024U *
        1024U;

    for (const std::size_t clients :
         config.client_counts) {
        if (!ValidClients(
                clients,
                config)) {
            continue;
        }

        const std::size_t servers =
            std::max<std::size_t>(
                3,
                clients);

        if (!ValidServers(
                servers,
                config)) {
            continue;
        }

        testing::DistributedTestHarness
            harness(3);

        if (!ConfigureHarness(
                harness,
                servers)) {
            continue;
        }

        auto setup =
            harness.CreateClient();

        if (!setup ||
            !PopulateFile(
                *setup,
                "/phase16-read",
                file_size,
                region_size)) {
            continue;
        }

        std::vector<
            std::unique_ptr<
                client::GFSClient>>
            clients_vector;

        for (std::size_t index = 0;
             index < clients;
             ++index) {
            clients_vector.push_back(
                harness.CreateClient());
        }

        const std::uint64_t operations =
            static_cast<std::uint64_t>(
                clients) *
            static_cast<std::uint64_t>(
                config.read_iterations);

        const std::uint64_t bytes =
            operations *
            static_cast<std::uint64_t>(
                region_size);

        std::mutex io_mutex;

        results.push_back(
            RunConcurrent(
                "sequential_read",
                clients,
                servers,
                3,
                bytes,
                operations,
                [&](std::size_t client_index) {
                    bool success = true;

                    for (std::size_t iteration = 0;
                         iteration <
                         config.read_iterations;
                         ++iteration) {
                        const std::size_t
                            max_offset =
                                file_size -
                                region_size;

                        const std::size_t
                            offset =
                                max_offset == 0
                                    ? 0
                                    : (client_index *
                                           1315423911ULL +
                                       iteration *
                                           2654435761ULL) %
                                          max_offset;

                        std::string data;

                        std::lock_guard lock(
                            io_mutex);

                        if (!clients_vector[
                                client_index]
                                ->Read(
                                    "/phase16-read",
                                    static_cast<
                                        std::uint64_t>(
                                        offset),
                                    region_size,
                                    data)) {
                            success = false;
                            break;
                        }
                    }

                    return success;
                },
                "Section 6.1.1",
                "PAPER_REPORTED: 10 MB/s at N=1; 94 MB/s at N=16; 125 MB/s theoretical limit",
                "Smaller deterministic Codespaces dataset; not hardware-equivalent."));
    }

    return results;
}

std::vector<BenchmarkResult>
BenchmarkSuite::RunWrite(
    const BenchmarkConfig& config) {
    std::vector<BenchmarkResult> results;

    const std::size_t operation_size =
        config.write_operation_mb *
        1024U *
        1024U;

    const std::size_t file_size =
        config.write_file_size_mb *
        1024U *
        1024U;

    for (const std::size_t clients :
         config.client_counts) {
        if (!ValidClients(
                clients,
                config)) {
            continue;
        }

        const std::size_t servers =
            std::max<std::size_t>(
                3,
                clients);

        if (!ValidServers(
                servers,
                config)) {
            continue;
        }

        testing::DistributedTestHarness
            harness(3);

        if (!ConfigureHarness(
                harness,
                servers)) {
            continue;
        }

        std::vector<
            std::unique_ptr<
                client::GFSClient>>
            clients_vector;

        bool setup_ok = true;

        for (std::size_t index = 0;
             index < clients;
             ++index) {
            auto client =
                harness.CreateClient();

            const std::string path =
                "/phase16-write-" +
                std::to_string(index);

            if (!client ||
                !client->CreateFile(path)) {
                setup_ok = false;
                break;
            }

            clients_vector.push_back(
                std::move(client));
        }

        if (!setup_ok) {
            continue;
        }

        const std::uint64_t
            operations_per_client =
                static_cast<std::uint64_t>(
                    (file_size +
                     operation_size -
                     1U) /
                    operation_size);

        const std::uint64_t operations =
            operations_per_client *
            static_cast<std::uint64_t>(
                clients);

        const std::uint64_t bytes =
            static_cast<std::uint64_t>(
                file_size) *
            static_cast<std::uint64_t>(
                clients);

        const std::string data =
            GenerateData(
                operation_size,
                31);

        std::mutex io_mutex;

        results.push_back(
            RunConcurrent(
                "sequential_write",
                clients,
                servers,
                3,
                bytes,
                operations,
                [&](std::size_t client_index) {
                    const std::string path =
                        "/phase16-write-" +
                        std::to_string(
                            client_index);

                    std::size_t written = 0;

                    while (written < file_size) {
                        const std::size_t amount =
                            std::min(
                                operation_size,
                                file_size -
                                    written);

                        std::lock_guard lock(
                            io_mutex);

                        if (!clients_vector[
                                client_index]
                                ->Write(
                                    path,
                                    static_cast<
                                        std::uint64_t>(
                                        written),
                                    data.substr(
                                        0,
                                        amount))) {
                            return false;
                        }

                        written += amount;
                    }

                    return true;
                },
                "Section 6.1.2",
                "PAPER_REPORTED: 6.3 MB/s at N=1; 35 MB/s at N=16; 67 MB/s theoretical limit",
                "Distinct files per client; replication factor 3."));
    }

    return results;
}

std::vector<BenchmarkResult>
BenchmarkSuite::RunRecordAppend(
    const BenchmarkConfig& config) {
    std::vector<BenchmarkResult> results;

    for (const std::size_t clients :
         config.client_counts) {
        const std::size_t servers =
            std::max<std::size_t>(
                3,
                clients);

        if (!ValidClients(
                clients,
                config) ||
            !ValidServers(
                servers,
                config)) {
            continue;
        }

        testing::DistributedTestHarness
            harness(3);

        if (!ConfigureHarness(
                harness,
                servers)) {
            continue;
        }

        auto setup =
            harness.CreateClient();

        if (!setup ||
            !setup->CreateFile(
                "/phase16-append")) {
            continue;
        }

        std::vector<
            std::unique_ptr<
                client::GFSClient>>
            clients_vector;

        for (std::size_t index = 0;
             index < clients;
             ++index) {
            auto client =
                harness.CreateClient();

            if (!client) {
                clients_vector.clear();
                break;
            }

            ConfigureAppend(
                harness,
                *client);

            clients_vector.push_back(
                std::move(client));
        }

        if (clients_vector.size() !=
            clients) {
            continue;
        }

        const std::uint64_t operations =
            static_cast<std::uint64_t>(
                clients) *
            static_cast<std::uint64_t>(
                config.append_records_per_client);

        const std::uint64_t bytes =
            operations *
            static_cast<std::uint64_t>(
                config.append_record_size_bytes);

        const std::string record =
            GenerateData(
                config.append_record_size_bytes,
                47);

        std::mutex io_mutex;

        results.push_back(
            RunConcurrent(
                "record_append",
                clients,
                servers,
                3,
                bytes,
                operations,
                [&](std::size_t client_index) {
                    for (std::size_t index = 0;
                         index <
                         config.append_records_per_client;
                         ++index) {
                        std::uint64_t offset = 0;

                        std::lock_guard lock(
                            io_mutex);

                        if (!clients_vector[
                                client_index]
                                ->Append(
                                    "/phase16-append",
                                    record,
                                    offset)) {
                            return false;
                        }
                    }

                    return true;
                },
                "Section 6.1.3",
                "PAPER_REPORTED: 6.0 MB/s at N=1; 4.8 MB/s at N=16",
                "All clients append to one file."));
    }

    return results;
}

std::vector<BenchmarkResult>
BenchmarkSuite::RunChunkserverScaling(
    const BenchmarkConfig& config) {
    std::vector<BenchmarkResult> results;

    const std::size_t file_size =
        config.read_file_size_mb *
        1024U *
        1024U;

    const std::size_t region_size =
        config.read_region_mb *
        1024U *
        1024U;

    for (const std::size_t servers :
         config.chunkserver_counts) {
        if (servers < 3 ||
            !ValidServers(
                servers,
                config)) {
            continue;
        }

        testing::DistributedTestHarness
            harness(3);

        if (!ConfigureHarness(
                harness,
                servers)) {
            continue;
        }

        auto client =
            harness.CreateClient();

        if (!client ||
            !PopulateFile(
                *client,
                "/phase16-server-scaling",
                file_size,
                region_size)) {
            continue;
        }

        const std::uint64_t operations =
            static_cast<std::uint64_t>(
                config.read_iterations);

        const std::uint64_t bytes =
            operations *
            static_cast<std::uint64_t>(
                region_size);

        BenchmarkRunner runner;

        results.push_back(
            runner.Run(
                "chunkserver_scaling_read",
                Configuration(
                    1,
                    servers,
                    3),
                1,
                servers,
                3,
                bytes,
                operations,
                [&] {
                    for (std::size_t index = 0;
                         index <
                         config.read_iterations;
                         ++index) {
                        std::string data;

                        const std::size_t
                            max_offset =
                                file_size -
                                region_size;

                        const std::size_t
                            offset =
                                max_offset == 0
                                    ? 0
                                    : (index *
                                           2654435761ULL) %
                                          max_offset;

                        if (!client->Read(
                                "/phase16-server-scaling",
                                static_cast<
                                    std::uint64_t>(
                                    offset),
                                region_size,
                                data)) {
                            return false;
                        }
                    }

                    return true;
                },
                "Section 6.1 and 6.2",
                "PAPER_REPORTED: microbenchmark used 16 chunkservers; real clusters had hundreds",
                "Three replicas are used; additional servers expose cluster-size effects."));
    }

    return results;
}

std::vector<BenchmarkResult>
BenchmarkSuite::RunMaster(
    const BenchmarkConfig& config) {
    std::vector<BenchmarkResult> results;

    testing::DistributedTestHarness
        harness(1);

    if (!ConfigureHarness(
            harness,
            1)) {
        return results;
    }

    auto client =
        harness.CreateClient();

    if (!client ||
        !client->CreateFile(
            "/phase16-master")) {
        return results;
    }

    BenchmarkRunner runner;

    results.push_back(
        runner.Run(
            "master_metadata_lookup",
            "clients=1;chunkservers=1;replication=1",
            1,
            1,
            1,
            0,
            config.master_operations,
            [&] {
                for (std::size_t index = 0;
                     index <
                     config.master_operations;
                     ++index) {
                    if (!client->FileExists(
                            "/phase16-master")) {
                        return false;
                    }

                    if (!client->LookupFile(
                            "/phase16-master")
                            .has_value()) {
                        return false;
                    }
                }

                return true;
            },
            "Section 6.2.4",
            "PAPER_REPORTED: approximately 200-500 master operations/s in real clusters",
            "In-process metadata path."));

    return results;
}

/*
 * PHASE 16 RECOVERY BENCHMARK
 *
 * Recovery sequence:
 *
 *   1. Start three chunkservers.
 *   2. Use replication factor 2.
 *   3. Allocate and populate chunks.
 *   4. Refresh healthy-server heartbeats.
 *   5. Stop chunkserver 1.
 *   6. Detect chunkserver 1 as failed.
 *   7. Restart chunkserver 1.
 *   8. Send heartbeat for the restarted server.
 *   9. Recover each chunk using RecoveryManager.
 *
 * The fresh heartbeats for servers 2 and 3 are
 * important because failure detection uses
 * heartbeat timestamps. ConfigureHarness() initially
 * sends timestamp 1000. If detection occurs at
 * 31001 without refreshing servers 2 and 3, they
 * can also be classified as stale.
 */
std::vector<BenchmarkResult>
BenchmarkSuite::RunRecovery(
    const BenchmarkConfig& config) {
    std::vector<BenchmarkResult> results;

    testing::DistributedTestHarness
        harness(2);

    if (!ConfigureHarness(
            harness,
            3)) {
        return results;
    }

    if (!harness.CreateFile(
            "/phase16-recovery",
            2)) {
        return results;
    }

    const std::size_t chunk_size =
        config.recovery_chunk_size_mb *
        1024U *
        1024U;

    const std::string data =
        GenerateData(
            chunk_size,
            61);

    std::vector<ChunkHandle>
        handles;

    for (std::size_t index = 0;
         index <
         config.recovery_chunk_count;
         ++index) {
        const auto handle =
            harness.AllocateChunk(
                "/phase16-recovery",
                2);

        if (!handle.has_value()) {
            return results;
        }

        if (!harness.WriteChunk(
                *handle,
                0,
                data)) {
            return results;
        }

        handles.push_back(
            *handle);
    }

    /*
     * Refresh healthy chunkservers.
     *
     * ConfigureHarness() initially sends:
     *
     *     heartbeat(2, 1000)
     *     heartbeat(3, 1000)
     *
     * Failure detection happens at 31001.
     * Therefore servers 2 and 3 would otherwise
     * be considered stale as well.
     *
     * This matches the sequence used by the
     * passing Phase 15 recovery test.
     */
    if (!harness.SendHeartbeat(
            2,
            30000)) {
        return results;
    }

    if (!harness.SendHeartbeat(
            3,
            30000)) {
        return results;
    }

    /*
     * Fail chunkserver 1.
     */
    if (!harness.StopChunkserver(1)) {
        return results;
    }

    /*
     * Detect the failed chunkserver.
     *
     * The timestamp 31001 is after the refreshed
     * healthy-server heartbeat timestamp 30000.
     */
    const auto failed_servers =
        harness.GetMaster()
            .DetectFailedChunkservers(
                31001);

    if (failed_servers.empty()) {
        return results;
    }

    /*
     * Restart chunkserver 1.
     */
    if (!harness.RestartChunkserver(1)) {
        return results;
    }

    /*
     * Re-register the restarted chunkserver
     * with a fresh heartbeat.
     */
    if (!harness.SendHeartbeat(
            1,
            31002)) {
        return results;
    }

    /*
     * Start timing only after the recovery
     * destination has been prepared.
     */
    const auto start =
        Clock::now();

    std::size_t recovered_chunks = 0;

    for (const ChunkHandle handle :
         handles) {
        if (harness.GetMaster()
                .GetRecoveryManager()
                .RecoverChunk(
                    handle,
                    31002)) {
            ++recovered_chunks;
        }
    }

    const auto end =
        Clock::now();

    const double elapsed_seconds =
        std::chrono::duration<double>(
            end - start)
            .count();

    BenchmarkResult result;

    result.benchmark =
        "recovery_rereplication";

    result.configuration =
        Configuration(
            1,
            3,
            2);

    result.clients = 1;

    result.chunkservers = 3;

    result.replication_factor = 2;

    result.bytes =
        static_cast<std::uint64_t>(
            recovered_chunks) *
        static_cast<std::uint64_t>(
            chunk_size);

    result.operations =
        static_cast<std::uint64_t>(
            handles.size());

    result.elapsed_seconds =
        elapsed_seconds;

    result.throughput_mb_per_sec =
        BenchmarkResult::
            ThroughputMbPerSec(
                result.bytes,
                result.elapsed_seconds);

    result.operations_per_sec =
        BenchmarkResult::
            OperationsPerSec(
                result.operations,
                result.elapsed_seconds);

    result.successes =
        recovered_chunks;

    result.failures =
        handles.size() -
        recovered_chunks;

    result.paper_reference =
        "Section 6.2.5";

    result.paper_value =
        "PAPER_REPORTED: 15,000 chunks / "
        "600 GB; 23.2 min; 440 MB/s";

    result.notes =
        "Small deterministic recovery workload. "
        "Healthy replicas receive fresh heartbeats "
        "before failure detection. Recovery uses "
        "the Phase 15 validated sequence. "
        "Not hardware-equivalent.";

    results.push_back(
        std::move(result));

    return results;
}

std::vector<BenchmarkResult>
BenchmarkSuite::RunReplication(
    const BenchmarkConfig& config) {
    std::vector<BenchmarkResult> results;

    constexpr std::size_t
        kServers = 3;

    constexpr std::size_t
        kFileSize = 8U * 1024U * 1024U;

    constexpr std::size_t
        kOperationSize = 1024U * 1024U;

    for (const std::uint32_t replication :
         config.replication_factors) {
        if (replication == 0 ||
            replication >
                kServers) {
            continue;
        }

        testing::DistributedTestHarness
            harness(replication);

        if (!ConfigureHarness(
                harness,
                kServers)) {
            continue;
        }

        auto client =
            harness.CreateClient();

        const std::string path =
            "/phase16-replication-" +
            std::to_string(replication);

        if (!client ||
            !client->CreateFile(path)) {
            continue;
        }

        const std::string data =
            GenerateData(
                kOperationSize,
                static_cast<
                    std::uint8_t>(
                    71U + replication));

        const std::size_t operations =
            kFileSize /
            kOperationSize;

        BenchmarkRunner runner;

        results.push_back(
            runner.Run(
                "replication_overhead",
                Configuration(
                    1,
                    kServers,
                    replication),
                1,
                kServers,
                replication,
                kFileSize,
                operations,
                [&] {
                    for (std::size_t index = 0;
                         index < operations;
                         ++index) {
                        if (!client->Write(
                                path,
                                static_cast<
                                    std::uint64_t>(
                                    index *
                                    kOperationSize),
                                data)) {
                            return false;
                        }
                    }

                    return true;
                },
                "Section 6.1.2",
                "PAPER_REPORTED: writes propagate each byte to 3 replicas; theoretical limit ~67 MB/s",
                "Replication factors 1, 2 and 3."));
    }

    return results;
}

std::vector<BenchmarkResult>
BenchmarkSuite::RunAll(
    const BenchmarkConfig& config) {
    std::vector<BenchmarkResult>
        results;

    auto append =
        [&results](
            std::vector<
                BenchmarkResult> values) {
            results.insert(
                results.end(),
                std::make_move_iterator(
                    values.begin()),
                std::make_move_iterator(
                    values.end()));
        };

    append(RunRead(config));
    append(RunWrite(config));
    append(RunRecordAppend(config));
    append(RunChunkserverScaling(config));
    append(RunMaster(config));
    append(RunRecovery(config));
    append(RunReplication(config));

    return results;
}

std::string EnvironmentSummary() {
    std::ostringstream stream;

    stream << "compiler=";

#if defined(__GNUC__)
    stream << "GCC "
           << __GNUC__
           << '.'
           << __GNUC_MINOR__
           << '.'
           << __GNUC_PATCHLEVEL__;
#elif defined(__clang__)
    stream << "Clang "
           << __clang_major__
           << '.'
           << __clang_minor__
           << '.'
           << __clang_patchlevel__;
#else
    stream << "unknown";
#endif

    stream << ";cxx_standard=20";

#if defined(__linux__)
    stream << ";os=Linux"
           << ";cpu_count="
           << get_nprocs();
#else
    stream << ";os=non-Linux";
#endif

    stream << ";chunk_size_bytes="
           << constants::kChunkSize;

    return stream.str();
}

std::string BuildPhase16Report(
    const std::vector<BenchmarkResult>& results,
    const BenchmarkConfig& config) {
    std::ostringstream output;

    output
        << "# Phase 16 — Benchmarks + Paper Comparison\n\n";

    output
        << "## 1. Benchmark environment\n\n";

    output
        << EnvironmentSummary()
        << "\n\n";

    output
        << "The reproduction uses GitHub "
           "Codespaces/Linux with in-process "
           "Master and Chunkserver components. "
           "It is not hardware-equivalent to "
           "the 2003 GFS evaluation.\n\n";

    output
        << "## 2. Methodology\n\n";

    output
        << "Phase 16 uses steady_clock, "
           "deterministic data, the existing "
           "DistributedTestHarness, GFSClient, "
           "Master metadata APIs, Chunkserver "
           "I/O, RecordAppender, and recovery.\n\n";

    output
        << "Default read dataset: "
        << config.read_file_size_mb
        << " MiB; read region: "
        << config.read_region_mb
        << " MiB; write file: "
        << config.write_file_size_mb
        << " MiB.\n\n";

    output
        << "## 3. Results\n\n"
           "| Benchmark | Clients | Chunkservers | "
           "Replication | Bytes | Operations | "
           "Seconds | MB/s | Ops/s | Success | "
           "Failure |\n"
           "|---|---:|---:|---:|---:|---:|"
           "---:|---:|---:|---:|---:|\n";

    output << std::fixed
           << std::setprecision(3);

    for (const auto& result : results) {
        output
            << "| "
            << result.benchmark
            << " | "
            << result.clients
            << " | "
            << result.chunkservers
            << " | "
            << result.replication_factor
            << " | "
            << result.bytes
            << " | "
            << result.operations
            << " | "
            << result.elapsed_seconds
            << " | "
            << result.throughput_mb_per_sec
            << " | "
            << result.operations_per_sec
            << " | "
            << result.successes
            << " | "
            << result.failures
            << " |\n";
    }

    output
        << "\n## 4. Paper comparison\n\n"
           "Paper values are labeled "
           "`PAPER_REPORTED`; benchmark measurements "
           "are `REPRODUCTION_MEASURED`. No accuracy "
           "score is calculated.\n\n";

    output
        << "- Reads: approximately 10 MB/s for one "
           "client, 94 MB/s aggregate for 16 clients, "
           "and 125 MB/s theoretical limit.\n"
        << "- Writes: approximately 6.3 MB/s for one "
           "client, 35 MB/s aggregate for 16 clients, "
           "and 67 MB/s theoretical limit.\n"
        << "- Record append: approximately 6.0 MB/s "
           "for one client and 4.8 MB/s for 16 clients.\n"
        << "- Master load: approximately 200–500 "
           "operations/s in the reported real clusters.\n"
        << "- Recovery example: approximately 15,000 "
           "chunks / 600 GB, 23.2 minutes and "
           "440 MB/s effective replication rate.\n\n";

    output
        << "## 5. Architectural interpretation\n\n"
           "The measurements expose relationships "
           "described in Section 6: master metadata "
           "operations are separated from bulk data "
           "I/O, replication increases write-side "
           "work, record append concentrates traffic "
           "on the shared file's last chunk, and "
           "recovery cost depends on data volume and "
           "available replicas.\n\n";

    output
        << "## 6. Limitations\n\n"
           "- The datasets are much smaller than the "
           "paper's 320 GB read set and 1 GB/client "
           "microbenchmarks.\n"
           "- There is no physical network simulator.\n"
           "- Master and Chunkservers run in-process.\n"
           "- Timing depends on the Codespaces host.\n"
           "- No throughput threshold causes a test "
           "failure.\n\n";

    output
        << "## 7. Conclusion\n\n"
           "Phase 16 provides repeatable measurements "
           "and a labeled comparison with the GFS "
           "Section 6 methodology and reported values. "
           "The results study the reproduction's "
           "architectural behavior rather than claiming "
           "hardware-equivalent performance.\n";

    return output.str();
}

}  // namespace gfs::benchmark