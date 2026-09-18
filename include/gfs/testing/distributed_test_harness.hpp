#pragma once

#include "gfs/chunkserver/chunkserver.hpp"
#include "gfs/client/gfs_client.hpp"
#include "gfs/common/types.hpp"
#include "gfs/master/master.hpp"
#include "gfs/testing/failure_injector.hpp"

#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <memory>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

namespace gfs::testing {

class DistributedTestHarness {
public:
    explicit DistributedTestHarness(
        std::uint32_t replication_factor = 2);

    DistributedTestHarness(
        const DistributedTestHarness&) = delete;

    DistributedTestHarness& operator=(
        const DistributedTestHarness&) = delete;

    ~DistributedTestHarness();

    [[nodiscard]] bool Initialize();

    [[nodiscard]] bool AddChunkserver(
        ServerId server_id);

    [[nodiscard]] bool StopChunkserver(
        ServerId server_id);

    [[nodiscard]] bool RestartChunkserver(
        ServerId server_id);

    [[nodiscard]] bool SendHeartbeat(
        ServerId server_id,
        std::uint64_t timestamp_ms);

    [[nodiscard]] bool SendHeartbeat(
        ServerId server_id,
        std::uint64_t timestamp_ms,
        const std::vector<
            master::heartbeat::ReportedChunk>& chunks);

    [[nodiscard]] bool CreateFile(
        const std::string& path,
        std::uint32_t replication_factor = 0);

    [[nodiscard]] std::optional<ChunkHandle>
    AllocateChunk(
        const std::string& path,
        std::size_t replica_count = 0);

    [[nodiscard]] bool WriteChunk(
        ChunkHandle handle,
        std::uint64_t offset,
        const std::string& data);

    [[nodiscard]] bool ReadChunk(
        ChunkHandle handle,
        std::uint64_t offset,
        std::size_t length,
        std::string& data);

    [[nodiscard]] bool Recover(
        std::uint64_t now_ms);

    [[nodiscard]] bool RecoverChunk(
        ChunkHandle handle,
        std::uint64_t now_ms);

    [[nodiscard]] std::unique_ptr<
        client::GFSClient>
    CreateClient();

    [[nodiscard]] master::Master&
    GetMaster() noexcept;

    [[nodiscard]] const master::Master&
    GetMaster() const noexcept;

    [[nodiscard]] chunkserver::Chunkserver*
    GetChunkserver(
        ServerId server_id) noexcept;

    [[nodiscard]] const chunkserver::Chunkserver*
    GetChunkserver(
        ServerId server_id) const noexcept;

    [[nodiscard]] bool HasChunkserver(
        ServerId server_id) const noexcept;

    [[nodiscard]] std::vector<ServerId>
    GetRunningServerIds() const;

    [[nodiscard]] std::filesystem::path
    GetStorageDirectory(
        ServerId server_id) const;

    [[nodiscard]] FailureInjector&
    GetFailureInjector() noexcept;

    [[nodiscard]] const FailureInjector&
    GetFailureInjector() const noexcept;

private:
    [[nodiscard]] std::vector<ServerId>
    GetLiveServers(
        std::uint64_t now_ms) const;

    [[nodiscard]] std::optional<
        client::metadata::ChunkMetadata>
    LookupClientChunk(
        const FilePath& path,
        ChunkIndex index) const;

    [[nodiscard]] std::unique_ptr<
        client::metadata::MasterClient>
    CreateMasterClient();

    [[nodiscard]] bool
    ShouldFailRead(
        ServerId server_id);

    [[nodiscard]] bool
    ShouldFailWrite(
        ServerId server_id);

    std::filesystem::path root_directory_;

    master::Master master_;

    std::unordered_map<
        ServerId,
        std::unique_ptr<
            chunkserver::Chunkserver>>
        chunkservers_;

    std::unordered_map<
        ServerId,
        std::filesystem::path>
        storage_directories_;

    std::uint32_t replication_factor_;
    bool initialized_ = false;

    FailureInjector failure_injector_;
};

}  // namespace gfs::testing