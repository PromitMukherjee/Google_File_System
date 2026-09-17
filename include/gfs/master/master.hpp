#pragma once

#include "gfs/common/types.hpp"
#include "gfs/master/heartbeat/heartbeat_manager.hpp"
#include "gfs/master/lease/lease_manager.hpp"
#include "gfs/master/metadata/metadata.hpp"
#include "gfs/master/namespace/namespace_manager.hpp"
#include "gfs/master/replication/placement_policy.hpp"
#include "gfs/master/replication/replica_manager.hpp"
#include "gfs/master/replication/re_replication.hpp"
#include "gfs/master/recovery/recovery_manager.hpp"
#include "gfs/master/recovery/checkpoint.hpp"
#include "gfs/master/recovery/operation_log.hpp"

#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <optional>
#include <string>
#include <vector>

namespace gfs::master {

class Master {
public:
    explicit Master(
        std::uint32_t default_replication_factor = 3,
        std::filesystem::path persistence_directory = {});

    Master(const Master&) = delete;
    Master& operator=(const Master&) = delete;

    // ============================================================
    // FILE OPERATIONS
    // ============================================================

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

    [[nodiscard]] std::optional<metadata::FileMetadata>
    GetFile(
        const std::string& path) const;

    [[nodiscard]] std::optional<metadata::FileMetadata>
    GetFileInfo(
        const std::string& path) const;

    [[nodiscard]] bool UpdateFileSize(
        const std::string& path,
        std::uint64_t size);

    // ============================================================
    // CHUNK METADATA OPERATIONS
    // ============================================================

    [[nodiscard]] std::optional<metadata::ChunkMetadata>
    GetChunkInfo(
        ChunkHandle handle) const;

    [[nodiscard]] std::optional<std::uint32_t>
    GetChunkReplicationFactor(
        ChunkHandle handle) const;

    [[nodiscard]] bool SetChunkVersion(
        ChunkHandle handle,
        ChunkVersion version);

    [[nodiscard]] bool SetChunkSize(
        ChunkHandle handle,
        std::uint64_t size);

    [[nodiscard]] std::optional<ChunkHandle>
    AllocateChunk(
        const std::string& path);

    [[nodiscard]] bool AddChunkToFile(
        const std::string& path,
        ChunkHandle handle);

    [[nodiscard]] bool RemoveChunkFromFile(
        const std::string& path,
        ChunkHandle handle);

    [[nodiscard]] std::vector<ChunkHandle>
    GetFileChunks(
        const std::string& path) const;

    [[nodiscard]] std::optional<std::size_t>
    GetChunkCount(
        const std::string& path) const;

    [[nodiscard]] std::size_t
    ChunkCount() const;

    // ============================================================
    // MASTER / NAMESPACE INFORMATION
    // ============================================================

    [[nodiscard]] std::size_t
    NamespaceNodeCount() const;

    [[nodiscard]] std::size_t
    FileCount() const;

    [[nodiscard]] namespace_management::NamespaceManager&
    GetNamespaceManager() noexcept;

    [[nodiscard]] const namespace_management::NamespaceManager&
    GetNamespaceManager() const noexcept;

    // ============================================================
    // REPLICA MANAGER
    // ============================================================

    [[nodiscard]] replication::ReplicaManager&
    GetReplicaManager() noexcept;

    [[nodiscard]] const replication::ReplicaManager&
    GetReplicaManager() const noexcept;

    // ============================================================
    // PLACEMENT POLICY
    // ============================================================

    [[nodiscard]] replication::PlacementPolicy&
    GetPlacementPolicy() noexcept;

    [[nodiscard]] const replication::PlacementPolicy&
    GetPlacementPolicy() const noexcept;

    // ============================================================
    // LEASE MANAGER
    // ============================================================

    [[nodiscard]] lease::LeaseManager&
    GetLeaseManager() noexcept;

    [[nodiscard]] const lease::LeaseManager&
    GetLeaseManager() const noexcept;

    // ============================================================
    // HEARTBEAT MANAGER
    // ============================================================

    [[nodiscard]] heartbeat::HeartbeatManager&
    GetHeartbeatManager() noexcept;

    [[nodiscard]] const heartbeat::HeartbeatManager&
    GetHeartbeatManager() const noexcept;

    // ============================================================
    // RE-REPLICATION MANAGER
    // ============================================================

    [[nodiscard]] replication::ReReplicationManager&
    GetReReplicationManager() noexcept;

    [[nodiscard]] const replication::ReReplicationManager&
    GetReReplicationManager() const noexcept;

    // ============================================================
    // RECOVERY MANAGER
    // ============================================================

    [[nodiscard]] recovery::RecoveryManager&
    GetRecoveryManager() noexcept;

    [[nodiscard]] const recovery::RecoveryManager&
    GetRecoveryManager() const noexcept;

    // ============================================================
    // INITIALIZATION / RECOVERY
    // ============================================================

    [[nodiscard]] bool
    Initialize();

    // ============================================================
    // PHASE 11 — CHECKPOINT / OPERATION LOG
    // ============================================================

    [[nodiscard]] bool
    CreateCheckpoint();

    [[nodiscard]] std::uint64_t
    GetLastOperationSequence() const noexcept;

    [[nodiscard]] const std::filesystem::path&
    GetPersistenceDirectory() const noexcept;

    [[nodiscard]] const std::filesystem::path&
    GetOperationLogPath() const noexcept;

    // ============================================================
    // REPLICA OPERATIONS
    // ============================================================

    [[nodiscard]] bool RegisterChunkReplica(
        ChunkHandle handle,
        ServerId server_id,
        bool is_primary = false);

    [[nodiscard]] bool HasChunkReplica(
        ChunkHandle handle,
        ServerId server_id) const;

    [[nodiscard]] std::optional<ServerId>
    GetChunkPrimary(
        ChunkHandle handle) const;

    [[nodiscard]] std::vector<ServerId>
    PlaceChunkReplicas(
        ChunkHandle handle,
        const std::vector<
            replication::PlacementCandidate>& candidates);

    [[nodiscard]] bool SetChunkPrimary(
        ChunkHandle handle,
        ServerId server_id);

    [[nodiscard]] bool AddReplica(
        ChunkHandle handle,
        ServerId server_id);

    [[nodiscard]] std::vector<ServerId>
    GetChunkReplicas(
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

    [[nodiscard]] std::vector<ServerId>
    GetReplicaServers(
        ChunkHandle handle) const;

    [[nodiscard]] std::optional<ServerId>
    GetPrimary(
        ChunkHandle handle) const;

    [[nodiscard]] bool SetPrimary(
        ChunkHandle handle,
        ServerId server_id);

    [[nodiscard]] std::vector<ServerId>
    SelectReplicaServers(
        ChunkHandle handle,
        const std::vector<
            replication::PlacementCandidate>& candidates) const;

    // ============================================================
    // LEASE OPERATIONS
    // ============================================================

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

    // ============================================================
    // HEARTBEAT OPERATIONS
    // ============================================================

    [[nodiscard]] bool ProcessHeartbeat(
        ServerId server_id,
        std::uint64_t timestamp_ms,
        const std::vector<
            heartbeat::ReportedChunk>& chunks);

    [[nodiscard]] bool ProcessHeartbeat(
        ServerId server_id,
        std::uint64_t timestamp_ms);

    [[nodiscard]] bool IsChunkserverAlive(
        ServerId server_id) const;

    [[nodiscard]] bool IsChunkserverAlive(
        ServerId server_id,
        std::uint64_t now_ms) const;

    // ============================================================
    // FAILURE / STALE REPLICA DETECTION
    // ============================================================

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
    // ============================================================
    // PHASE 11 — RECOVERY
    // ============================================================

    [[nodiscard]] bool ReplayOperation(
        const recovery::OperationRecord& record);

    [[nodiscard]] bool AppendOperation(
        recovery::OperationType type,
        const std::vector<std::string>& fields);

    // ============================================================
    // CORE MASTER STATE
    // ============================================================

    metadata::Metadata metadata_;

    namespace_management::NamespaceManager
        namespace_manager_;

    replication::ReplicaManager
        replica_manager_;

    replication::PlacementPolicy
        placement_policy_;

    lease::LeaseManager
        lease_manager_;

    heartbeat::HeartbeatManager
        heartbeat_manager_;

    replication::ReReplicationManager
        re_replication_manager_;

    recovery::RecoveryManager
        recovery_manager_;

    // ============================================================
    // PHASE 11 — PERSISTENCE STATE
    // ============================================================

    std::filesystem::path
        persistence_directory_;

    recovery::OperationLog
        operation_log_;

    recovery::Checkpoint
        checkpoint_;

    bool initialized_ = false;
};

}  // namespace gfs::master