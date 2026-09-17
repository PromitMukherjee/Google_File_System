
#pragma once

#include "gfs/common/types.hpp"
#include "gfs/master/heartbeat/heartbeat_manager.hpp"
#include "gfs/master/lease/lease_manager.hpp"
#include "gfs/master/metadata/metadata.hpp"
#include "gfs/master/namespace/namespace_manager.hpp"
#include "gfs/master/replication/placement_policy.hpp"
#include "gfs/master/replication/replica_manager.hpp"

#include <cstddef>
#include <cstdint>
#include <optional>
#include <string>
#include <vector>

namespace gfs::master {

class Master {
public:
    explicit Master(
        std::uint32_t default_replication_factor = 3);

    Master(const Master&) = delete;
    Master& operator=(const Master&) = delete;

    [[nodiscard]] bool CreateFile(
        const std::string& path,
        std::uint32_t replication_factor = 0);

    [[nodiscard]] bool DeleteFile(
        const std::string& path);

    [[nodiscard]] bool RenameFile(
        const std::string& source_path,
        const std::string& destination_path);

    [[nodiscard]] bool FileExists(
        const std::string& path) const;

    [[nodiscard]] bool CreateDirectory(
        const std::string& path);

    [[nodiscard]] bool DirectoryExists(
        const std::string& path) const;

    [[nodiscard]] std::optional<metadata::FileMetadata> GetFile(
        const std::string& path) const;

    [[nodiscard]] std::optional<metadata::FileMetadata> GetFileInfo(
        const std::string& path) const;

    [[nodiscard]] std::optional<metadata::ChunkMetadata> GetChunkInfo(
        ChunkHandle handle) const;

    [[nodiscard]] std::optional<ChunkHandle> AllocateChunk(
        const std::string& path);

    [[nodiscard]] bool AddChunkToFile(
        const std::string& path,
        ChunkHandle handle);

    [[nodiscard]] bool RemoveChunkFromFile(
        const std::string& path,
        ChunkHandle handle);

    [[nodiscard]] std::vector<ChunkHandle> GetFileChunks(
        const std::string& path) const;

    [[nodiscard]] std::optional<std::size_t> GetChunkCount(
        const std::string& path) const;

    [[nodiscard]] std::size_t ChunkCount() const;

    [[nodiscard]] std::size_t NamespaceNodeCount() const;

    [[nodiscard]] std::size_t FileCount() const;

    [[nodiscard]] namespace_management::NamespaceManager&
    GetNamespaceManager() noexcept;

    [[nodiscard]] const namespace_management::NamespaceManager&
    GetNamespaceManager() const noexcept;

    [[nodiscard]] replication::ReplicaManager&
    GetReplicaManager() noexcept;

    [[nodiscard]] const replication::ReplicaManager&
    GetReplicaManager() const noexcept;

    [[nodiscard]] replication::PlacementPolicy&
    GetPlacementPolicy() noexcept;

    [[nodiscard]] const replication::PlacementPolicy&
    GetPlacementPolicy() const noexcept;

    [[nodiscard]] lease::LeaseManager&
    GetLeaseManager() noexcept;

    [[nodiscard]] const lease::LeaseManager&
    GetLeaseManager() const noexcept;

    [[nodiscard]] heartbeat::HeartbeatManager&
    GetHeartbeatManager() noexcept;

    [[nodiscard]] const heartbeat::HeartbeatManager&
    GetHeartbeatManager() const noexcept;

    [[nodiscard]] bool Initialize();

    [[nodiscard]] bool RegisterChunkReplica(
        ChunkHandle handle,
        ServerId server_id,
        bool is_primary = false);

    [[nodiscard]] bool HasChunkReplica(
        ChunkHandle handle,
        ServerId server_id) const;

    [[nodiscard]] std::optional<ServerId>
    GetChunkPrimary(ChunkHandle handle) const;

    [[nodiscard]] std::vector<ServerId> PlaceChunkReplicas(
        ChunkHandle handle,
        const std::vector<replication::PlacementCandidate>& candidates);

    [[nodiscard]] bool SetChunkPrimary(
        ChunkHandle handle,
        ServerId server_id);

    [[nodiscard]] bool AddReplica(
        ChunkHandle handle,
        ServerId server_id);

    [[nodiscard]] std::vector<ServerId> GetChunkReplicas(
        ChunkHandle handle) const;

    [[nodiscard]] bool RegisterReplica(
        ChunkHandle handle,
        ServerId server_id,
        bool is_primary = false);

    [[nodiscard]] bool RemoveReplica(
        ChunkHandle handle,
        ServerId server_id);

    [[nodiscard]] bool HasReplica(
        ChunkHandle handle,
        ServerId server_id) const;

    [[nodiscard]] std::vector<ServerId> GetReplicaServers(
        ChunkHandle handle) const;

    [[nodiscard]] std::optional<ServerId> GetPrimary(
        ChunkHandle handle) const;

    [[nodiscard]] bool SetPrimary(
        ChunkHandle handle,
        ServerId server_id);

    [[nodiscard]] std::vector<ServerId> SelectReplicaServers(
        ChunkHandle handle,
        const std::vector<replication::PlacementCandidate>&
            candidates) const;

    [[nodiscard]] std::optional<lease::Lease>
    AcquireLease(
        ChunkHandle handle,
        ServerId primary_server_id);

    [[nodiscard]] std::optional<lease::Lease>
    GetLease(
        ChunkHandle handle) const;

    [[nodiscard]] bool IsLeaseValid(
        ChunkHandle handle) const;

    [[nodiscard]] bool IsLeaseValid(
        ChunkHandle handle,
        ServerId primary_server_id) const;

    [[nodiscard]] bool ExtendLease(
        ChunkHandle handle,
        ServerId primary_server_id);

    [[nodiscard]] bool ExtendLease(
        ChunkHandle handle,
        ServerId primary_server_id,
        std::uint64_t extension_ms);

    [[nodiscard]] bool ReleaseLease(
        ChunkHandle handle);

    [[nodiscard]] bool ProcessHeartbeat(
        ServerId server_id,
        std::uint64_t timestamp_ms,
        const std::vector<heartbeat::ReportedChunk>&
            chunks);

    [[nodiscard]] bool ProcessHeartbeat(
        ServerId server_id,
        std::uint64_t timestamp_ms);

    [[nodiscard]] bool IsChunkserverAlive(
        ServerId server_id) const;

    [[nodiscard]] bool IsChunkserverAlive(
        ServerId server_id,
        std::uint64_t now_ms) const;

    [[nodiscard]] std::vector<ServerId>
    DetectFailedChunkservers(
        std::uint64_t now_ms);

    [[nodiscard]] bool IsStaleReplica(
        ChunkHandle handle,
        ServerId server_id) const;

    [[nodiscard]] std::vector<ServerId>
    GetStaleReplicas(
        ChunkHandle handle) const;

    [[nodiscard]] std::vector<ChunkHandle>
    GetStaleChunks(
        ServerId server_id) const;

private:
    metadata::Metadata metadata_;
    namespace_management::NamespaceManager namespace_manager_;
    replication::ReplicaManager replica_manager_;
    replication::PlacementPolicy placement_policy_;
    lease::LeaseManager lease_manager_;
    heartbeat::HeartbeatManager heartbeat_manager_;
};

}  // namespace gfs::master