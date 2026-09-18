#include "gfs/testing/distributed_test_harness.hpp"

#include "gfs/client/metadata/chunk_location_cache.hpp"
#include "gfs/client/metadata/master_client.hpp"

#include <algorithm>
#include <cstdint>
#include <filesystem>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

namespace gfs::testing {

DistributedTestHarness::DistributedTestHarness(
    std::uint32_t replication_factor)
    : root_directory_(
          std::filesystem::temp_directory_path() /
          ("gfs_phase15_" +
           std::to_string(
               reinterpret_cast<
                   std::uintptr_t>(this)))),
      master_(
          replication_factor == 0
              ? 1
              : replication_factor),
      replication_factor_(
          replication_factor == 0
              ? 1
              : replication_factor) {
    std::filesystem::create_directories(
        root_directory_);
}

DistributedTestHarness::~DistributedTestHarness() {
    for (const auto& [server_id, server] :
         chunkservers_) {
        static_cast<void>(server_id);

        if (server != nullptr) {
            static_cast<void>(
                master_.GetRecoveryManager()
                    .UnregisterChunkserver(
                        server->GetServerId()));
        }
    }

    chunkservers_.clear();

    std::error_code error;
    std::filesystem::remove_all(
        root_directory_,
        error);
}

bool DistributedTestHarness::Initialize() {
    if (initialized_) {
        return true;
    }

    if (!master_.Initialize()) {
        return false;
    }

    initialized_ = true;
    return true;
}

bool DistributedTestHarness::AddChunkserver(
    ServerId server_id) {
    if (!initialized_ ||
        server_id == 0 ||
        chunkservers_.contains(server_id)) {
        return false;
    }

    const auto directory =
        root_directory_ /
        ("chunkserver_" +
         std::to_string(server_id));

    std::filesystem::create_directories(
        directory);

    auto server =
        std::make_unique<
            chunkserver::Chunkserver>(
                server_id,
                directory.string());

    if (!server->Initialize()) {
        return false;
    }

    if (!master_.GetRecoveryManager()
            .RegisterChunkserver(*server)) {
        return false;
    }

    storage_directories_[server_id] =
        directory;

    chunkservers_.emplace(
        server_id,
        std::move(server));

    return true;
}

bool DistributedTestHarness::StopChunkserver(
    ServerId server_id) {
    const auto it =
        chunkservers_.find(server_id);

    if (it == chunkservers_.end() ||
        it->second == nullptr) {
        return false;
    }

    if (!master_.GetRecoveryManager()
            .UnregisterChunkserver(
                server_id)) {
        return false;
    }

    it->second.reset();
    return true;
}

bool DistributedTestHarness::RestartChunkserver(
    ServerId server_id) {
    if (server_id == 0 ||
        chunkservers_.contains(server_id)) {
        return false;
    }

    const auto path_it =
        storage_directories_.find(server_id);

    if (path_it == storage_directories_.end()) {
        return false;
    }

    auto server =
        std::make_unique<
            chunkserver::Chunkserver>(
                server_id,
                path_it->second.string());

    if (!server->Initialize()) {
        return false;
    }

    if (!master_.GetRecoveryManager()
            .RegisterChunkserver(*server)) {
        return false;
    }

    chunkservers_[server_id] =
        std::move(server);

    return true;
}

bool DistributedTestHarness::SendHeartbeat(
    ServerId server_id,
    std::uint64_t timestamp_ms) {
    return SendHeartbeat(
        server_id,
        timestamp_ms,
        {});
}

bool DistributedTestHarness::SendHeartbeat(
    ServerId server_id,
    std::uint64_t timestamp_ms,
    const std::vector<
        master::heartbeat::ReportedChunk>& chunks) {
    if (!HasChunkserver(server_id)) {
        return false;
    }

    if (failure_injector_.ShouldFail(
            FailureOperation::Heartbeat,
            server_id) ||
        failure_injector_.ShouldFail(
            FailureOperation::DelayedHeartbeat,
            server_id)) {
        return false;
    }

    return master_.ProcessHeartbeat(
        server_id,
        timestamp_ms,
        chunks);
}

bool DistributedTestHarness::CreateFile(
    const std::string& path,
    std::uint32_t replication_factor) {
    return master_.CreateFile(
        path,
        replication_factor);
}

std::optional<ChunkHandle>
DistributedTestHarness::AllocateChunk(
    const std::string& path,
    std::size_t replica_count) {
    const auto handle =
        master_.AllocateChunk(path);

    if (!handle.has_value()) {
        return std::nullopt;
    }

    const std::uint32_t desired =
        replica_count == 0U
            ? replication_factor_
            : static_cast<std::uint32_t>(
                  replica_count);

    std::vector<ServerId> servers;

    const auto live =
        GetLiveServers(1000);

    for (const ServerId server_id :
         live) {
        if (servers.size() >=
            static_cast<std::size_t>(
                desired)) {
            break;
        }

        if (!master_.RegisterReplica(
                *handle,
                server_id,
                servers.empty())) {
            return std::nullopt;
        }

        auto* server =
            GetChunkserver(server_id);

        if (server == nullptr ||
            !server->CreateChunk(*handle)) {
            static_cast<void>(
                master_.GetReplicaManager()
                    .RemoveChunk(*handle));
            return std::nullopt;
        }

        servers.push_back(server_id);
    }

    if (servers.size() <
        static_cast<std::size_t>(
            desired)) {
        static_cast<void>(
            master_.GetReplicaManager()
                .RemoveChunk(*handle));
        static_cast<void>(
            master_.GetMetadata()
                .DeleteChunk(*handle));
        return std::nullopt;
    }

    return handle;
}

bool DistributedTestHarness::WriteChunk(
    ChunkHandle handle,
    std::uint64_t offset,
    const std::string& data) {
    const auto replicas =
        master_.GetReplicaServers(handle);

    if (replicas.empty()) {
        return false;
    }

    for (const ServerId server_id :
         replicas) {
        if (failure_injector_.ShouldFail(
                FailureOperation::ReplicaWrite,
                server_id) ||
            failure_injector_.ShouldFail(
                FailureOperation::NetworkRpc,
                server_id)) {
            continue;
        }

        auto* server =
            GetChunkserver(server_id);

        if (server == nullptr) {
            continue;
        }

        if (server->WriteChunk(
                handle,
                offset,
                data)) {
            const auto end =
                offset +
                static_cast<std::uint64_t>(
                    data.size());

            static_cast<void>(
                master_.SetChunkSize(
                    handle,
                    std::max(
                        master_.GetChunkInfo(handle)
                            ->GetSize(),
                        end)));

            return true;
        }
    }

    return false;
}

bool DistributedTestHarness::ReadChunk(
    ChunkHandle handle,
    std::uint64_t offset,
    std::size_t length,
    std::string& data) {
    data.clear();

    const auto replicas =
        master_.GetReplicaServers(handle);

    for (const ServerId server_id :
         replicas) {
        if (failure_injector_.ShouldFail(
                FailureOperation::ReplicaRead,
                server_id) ||
            failure_injector_.ShouldFail(
                FailureOperation::NetworkRpc,
                server_id)) {
            continue;
        }

        auto* server =
            GetChunkserver(server_id);

        if (server == nullptr) {
            continue;
        }

        if (server->ReadChunk(
                handle,
                offset,
                length,
                data)) {
            return true;
        }
    }

    return false;
}

bool DistributedTestHarness::Recover(
    std::uint64_t now_ms) {
    if (failure_injector_.ShouldFail(
            FailureOperation::ReReplication)) {
        return false;
    }

    const auto recovered =
        master_.GetRecoveryManager()
            .RecoverFailedChunkservers(
                now_ms);

    return !recovered.empty() ||
           master_.GetAllChunkHandles().empty();
}

bool DistributedTestHarness::RecoverChunk(
    ChunkHandle handle,
    std::uint64_t now_ms) {
    if (failure_injector_.ShouldFail(
            FailureOperation::ReReplication)) {
        return false;
    }

    return master_.GetRecoveryManager()
        .RecoverChunk(
            handle,
            now_ms);
}

std::unique_ptr<client::GFSClient>
DistributedTestHarness::CreateClient() {
    auto master_client =
        CreateMasterClient();

    auto location_cache =
        std::make_unique<
            client::metadata::ChunkLocationCache>();

    auto read_function =
        [this](
            const client::metadata::ChunkLocation& location,
            ChunkHandle handle,
            std::uint64_t offset,
            std::size_t length,
            std::string& data) {
            if (failure_injector_.ShouldFail(
                    FailureOperation::ReplicaRead,
                    location.server_id) ||
                failure_injector_.ShouldFail(
                    FailureOperation::NetworkRpc,
                    location.server_id)) {
                return false;
            }

            const auto* server =
                GetChunkserver(
                    location.server_id);

            if (server == nullptr) {
                return false;
            }

            return server->ReadChunk(
                handle,
                offset,
                length,
                data);
        };

    auto write_function =
        [this](
            const client::metadata::ChunkLocation& location,
            ChunkHandle handle,
            std::uint64_t offset,
            const std::string& data) {
            if (failure_injector_.ShouldFail(
                    FailureOperation::ReplicaWrite,
                    location.server_id) ||
                failure_injector_.ShouldFail(
                    FailureOperation::NetworkRpc,
                    location.server_id)) {
                return false;
            }

            auto* server =
                GetChunkserver(
                    location.server_id);

            if (server == nullptr) {
                return false;
            }

            return server->WriteChunk(
                handle,
                offset,
                data);
        };

    auto client =
        std::make_unique<
            client::GFSClient>(
            std::move(master_client),
            std::move(location_cache),
            std::move(read_function),
            std::move(write_function),
            [this](
                const FilePath& path,
                std::uint64_t size) {
                return master_.UpdateFileSize(
                    path,
                    size);
            });

    client->SetFileCreateFunction(
        [this](const FilePath& path) {
            return master_.CreateFile(
                path,
                replication_factor_);
        });

    client->SetSnapshotCreateFunction(
        [this](
            const FilePath& source,
            const FilePath& snapshot) {
            return master_.CreateSnapshot(
                source,
                snapshot);
        });

    client->SetChunkSizeUpdater(
        [this](
            ChunkHandle handle,
            std::uint64_t size) {
            return master_.SetChunkSize(
                handle,
                size);
        });

    return client;
}

master::Master&
DistributedTestHarness::GetMaster() noexcept {
    return master_;
}

const master::Master&
DistributedTestHarness::GetMaster() const noexcept {
    return master_;
}

chunkserver::Chunkserver*
DistributedTestHarness::GetChunkserver(
    ServerId server_id) noexcept {
    const auto it =
        chunkservers_.find(server_id);

    if (it == chunkservers_.end()) {
        return nullptr;
    }

    return it->second.get();
}

const chunkserver::Chunkserver*
DistributedTestHarness::GetChunkserver(
    ServerId server_id) const noexcept {
    const auto it =
        chunkservers_.find(server_id);

    if (it == chunkservers_.end()) {
        return nullptr;
    }

    return it->second.get();
}

bool DistributedTestHarness::HasChunkserver(
    ServerId server_id) const noexcept {
    return GetChunkserver(server_id) != nullptr;
}

std::vector<ServerId>
DistributedTestHarness::GetRunningServerIds() const {
    std::vector<ServerId> ids;

    ids.reserve(chunkservers_.size());

    for (const auto& [server_id, server] :
         chunkservers_) {
        if (server != nullptr) {
            ids.push_back(server_id);
        }
    }

    std::sort(ids.begin(), ids.end());
    return ids;
}

std::filesystem::path
DistributedTestHarness::GetStorageDirectory(
    ServerId server_id) const {
    const auto it =
        storage_directories_.find(server_id);

    if (it == storage_directories_.end()) {
        return {};
    }

    return it->second;
}

FailureInjector&
DistributedTestHarness::GetFailureInjector() noexcept {
    return failure_injector_;
}

const FailureInjector&
DistributedTestHarness::GetFailureInjector() const noexcept {
    return failure_injector_;
}

std::vector<ServerId>
DistributedTestHarness::GetLiveServers(
    std::uint64_t now_ms) const {
    std::vector<ServerId> live;

    for (const ServerId server_id :
         GetRunningServerIds()) {
        if (master_.IsChunkserverAlive(
                server_id,
                now_ms) &&
            master_.GetRecoveryManager()
                .HasChunkserver(server_id)) {
            live.push_back(server_id);
        }
    }

    return live;
}

std::optional<
    client::metadata::ChunkMetadata>
DistributedTestHarness::LookupClientChunk(
    const FilePath& path,
    ChunkIndex index) const {
    const auto chunks =
        master_.GetFileChunks(path);

    if (index >= chunks.size()) {
        return std::nullopt;
    }

    const ChunkHandle handle =
        chunks[
            static_cast<std::size_t>(
                index)];

    const auto chunk =
        master_.GetChunkInfo(handle);

    if (!chunk.has_value()) {
        return std::nullopt;
    }

    client::metadata::ChunkMetadata result;

    result.handle = handle;
    result.index = index;
    result.version =
        chunk->GetVersion();

    for (const ServerId server_id :
         master_.GetReplicaServers(handle)) {
        result.locations.push_back(
            client::metadata::ChunkLocation{
                server_id,
                "chunkserver-" +
                    std::to_string(server_id)});
    }

    return result;
}

std::unique_ptr<
    client::metadata::MasterClient>
DistributedTestHarness::CreateMasterClient() {
    auto client =
        std::make_unique<
            client::metadata::MasterClient>(
            "in-process-master");

    client->SetFileLookup(
        [this](const FilePath& path) {
            const auto file =
                master_.GetFile(path);

            if (!file.has_value()) {
                return std::optional<
                    client::metadata::FileMetadata>{};
            }

            return std::optional<
                client::metadata::FileMetadata>(
                client::metadata::FileMetadata{
                    file->GetPath(),
                    file->GetSize(),
                    file->ChunkCount()});
        });

    client->SetChunkLookup(
        [this](
            const FilePath& path,
            ChunkIndex index) {
            return LookupClientChunk(
                path,
                index);
        });

    client->SetChunkAllocator(
        [this](
            const FilePath& path,
            ChunkIndex index) {
            const auto count =
                master_.GetChunkCount(path);

            if (!count.has_value() ||
                index != *count) {
                return std::optional<
                    client::metadata::ChunkMetadata>{};
            }

            const auto handle =
                AllocateChunk(path);

            if (!handle.has_value()) {
                return std::optional<
                    client::metadata::ChunkMetadata>{};
            }

            return LookupClientChunk(
                path,
                index);
        });

    return client;
}

bool DistributedTestHarness::ShouldFailRead(
    ServerId server_id) {
    return failure_injector_.ShouldFail(
        FailureOperation::ReplicaRead,
        server_id);
}

bool DistributedTestHarness::ShouldFailWrite(
    ServerId server_id) {
    return failure_injector_.ShouldFail(
        FailureOperation::ReplicaWrite,
        server_id);
}

}  // namespace gfs::testing