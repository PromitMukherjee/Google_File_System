#include "gfs/master/master.hpp"

#include <algorithm>
#include <cstdint>
#include <filesystem>
#include <limits>
#include <optional>
#include <string>
#include <vector>

namespace gfs::master {

Master::Master(
    std::uint32_t default_replication_factor,
    std::filesystem::path persistence_directory)
    : metadata_(),
      namespace_manager_(),
      replica_manager_(),
      placement_policy_(default_replication_factor),
      lease_manager_(replica_manager_),
      heartbeat_manager_(),
      re_replication_manager_(*this),
      recovery_manager_(re_replication_manager_),
      persistence_directory_(
          std::move(persistence_directory)),
      operation_log_(),
      checkpoint_() {
    if (!persistence_directory_.empty()) {
        checkpoint_ =
            recovery::Checkpoint(
                persistence_directory_);

        operation_log_.Open(
            persistence_directory_ /
            "operation.log");
    }
}

bool Master::AppendOperation(
    recovery::OperationType type,
    const std::vector<std::string>& fields) {
    if (persistence_directory_.empty()) {
        return true;
    }

    if (!operation_log_.IsOpen()) {
        return false;
    }

    const std::string payload =
        recovery::OperationLog::EncodeFields(
            fields);

    if (!operation_log_.Append(
            type,
            payload)) {
        return false;
    }

    return operation_log_.Flush();
}

bool Master::CreateFile(
    const std::string& path,
    std::uint32_t replication_factor) {
    if (path.empty() ||
        namespace_manager_.Exists(path)) {
        return false;
    }

    const std::uint32_t factor =
        replication_factor == 0
            ? 3
            : replication_factor;

    if (!AppendOperation(
            recovery::OperationType::CreateFile,
            {path, std::to_string(factor)})) {
        return false;
    }

    if (!namespace_manager_.CreateFile(path)) {
        return false;
    }

    if (!metadata_.CreateFile(
            path,
            factor)) {
        static_cast<void>(
            namespace_manager_.DeleteFile(path));

        return false;
    }

    return true;
}

bool Master::DeleteFile(
    const std::string& path) {
    if (!namespace_manager_.IsFile(path)) {
        return false;
    }

    const auto chunks =
        metadata_.GetFileChunks(path);

    if (!AppendOperation(
            recovery::OperationType::DeleteFile,
            {path})) {
        return false;
    }

    if (!metadata_.DeleteFile(path)) {
        return false;
    }

    for (const ChunkHandle handle :
         chunks) {
        static_cast<void>(
            replica_manager_.RemoveChunk(handle));

        static_cast<void>(
            lease_manager_.ReleaseLease(handle));
    }

    return namespace_manager_.DeleteFile(path);
}

bool Master::RenameFile(
    const std::string& source_path,
    const std::string& destination_path) {
    if (!namespace_manager_.IsFile(source_path) ||
        namespace_manager_.Exists(destination_path)) {
        return false;
    }

    if (!AppendOperation(
            recovery::OperationType::RenameFile,
            {source_path, destination_path})) {
        return false;
    }

    if (!namespace_manager_.Rename(
            source_path,
            destination_path)) {
        return false;
    }

    if (!metadata_.RenameFile(
            source_path,
            destination_path)) {
        static_cast<void>(
            namespace_manager_.Rename(
                destination_path,
                source_path));

        return false;
    }

    return true;
}

bool Master::FileExists(
    const std::string& path) const {
    return namespace_manager_.Exists(path) &&
           namespace_manager_.IsFile(path);
}

bool Master::CreateDirectory(
    const std::string& path) {
    if (namespace_manager_.Exists(path)) {
        return false;
    }

    if (!AppendOperation(
            recovery::OperationType::CreateDirectory,
            {path})) {
        return false;
    }

    return namespace_manager_.CreateDirectory(
        path);
}

bool Master::DirectoryExists(
    const std::string& path) const {
    return namespace_manager_.Exists(path) &&
           namespace_manager_.IsDirectory(path);
}

std::optional<metadata::FileMetadata>
Master::GetFile(
    const std::string& path) const {
    return metadata_.GetFile(path);
}

std::optional<metadata::FileMetadata>
Master::GetFileInfo(
    const std::string& path) const {
    return metadata_.GetFile(path);
}

std::optional<metadata::ChunkMetadata>
Master::GetChunkInfo(
    ChunkHandle handle) const {
    return metadata_.GetChunk(handle);
}

std::optional<std::uint32_t>
Master::GetChunkReplicationFactor(
    ChunkHandle handle) const {
    return metadata_.GetChunkReplicationFactor(
        handle);
}

bool Master::SetChunkVersion(
    ChunkHandle handle,
    ChunkVersion version) {
    if (!metadata_.ChunkExists(handle) ||
        version == 0) {
        return false;
    }

    if (!AppendOperation(
            recovery::OperationType::SetChunkVersion,
            {std::to_string(handle),
             std::to_string(version)})) {
        return false;
    }

    return metadata_.SetChunkVersion(
        handle,
        version);
}

std::optional<ChunkHandle>
Master::AllocateChunk(
    const std::string& path) {
    if (!metadata_.FileExists(path)) {
        return std::nullopt;
    }

    const ChunkHandle handle =
        metadata_.GetNextChunkHandle();

    if (handle == 0) {
        return std::nullopt;
    }

    if (!AppendOperation(
            recovery::OperationType::AllocateChunk,
            {path,
             std::to_string(handle),
             "1",
             "0"})) {
        return std::nullopt;
    }

    if (!metadata_.AllocateChunk(
            path,
            handle,
            1,
            0)) {
        return std::nullopt;
    }

    return handle;
}

bool Master::AddChunkToFile(
    const std::string& path,
    ChunkHandle handle) {
    if (!metadata_.FileExists(path) ||
        !metadata_.ChunkExists(handle)) {
        return false;
    }

    if (metadata_.GetFile(path)
            .has_value()) {
        const auto chunks =
            metadata_.GetFileChunks(path);

        if (std::find(
                chunks.begin(),
                chunks.end(),
                handle) != chunks.end()) {
            return true;
        }
    }

    if (!AppendOperation(
            recovery::OperationType::AddChunkToFile,
            {path,
             std::to_string(handle)})) {
        return false;
    }

    return metadata_.AddChunkToFile(
        path,
        handle);
}

bool Master::RemoveChunkFromFile(
    const std::string& path,
    ChunkHandle handle) {
    if (!metadata_.FileExists(path)) {
        return false;
    }

    const auto chunks =
        metadata_.GetFileChunks(path);

    if (std::find(
            chunks.begin(),
            chunks.end(),
            handle) == chunks.end()) {
        return false;
    }

    if (!AppendOperation(
            recovery::OperationType::RemoveChunkFromFile,
            {path,
             std::to_string(handle)})) {
        return false;
    }

    return metadata_.RemoveChunkFromFile(
        path,
        handle);
}

bool Master::UpdateFileSize(
    const std::string& path,
    std::uint64_t size) {
    if (!metadata_.FileExists(path)) {
        return false;
    }

    if (!AppendOperation(
            recovery::OperationType::UpdateFileSize,
            {path,
             std::to_string(size)})) {
        return false;
    }

    return metadata_.UpdateFileSize(
        path,
        size);
}

bool Master::SetChunkSize(
    ChunkHandle handle,
    std::uint64_t size) {
    if (!metadata_.ChunkExists(handle)) {
        return false;
    }

    if (!AppendOperation(
            recovery::OperationType::SetChunkSize,
            {std::to_string(handle),
             std::to_string(size)})) {
        return false;
    }

    return metadata_.SetChunkSize(
        handle,
        size);
}

std::vector<ChunkHandle>
Master::GetFileChunks(
    const std::string& path) const {
    return metadata_.GetFileChunks(path);
}

std::optional<std::size_t>
Master::GetChunkCount(
    const std::string& path) const {
    return metadata_.GetChunkCount(path);
}

std::size_t Master::ChunkCount() const {
    return metadata_.ChunkCount();
}

std::size_t Master::NamespaceNodeCount() const {
    return namespace_manager_.NodeCount();
}

std::size_t Master::FileCount() const {
    return metadata_.FileCount();
}

namespace_management::NamespaceManager&
Master::GetNamespaceManager() noexcept {
    return namespace_manager_;
}

const namespace_management::NamespaceManager&
Master::GetNamespaceManager() const noexcept {
    return namespace_manager_;
}

replication::ReplicaManager&
Master::GetReplicaManager() noexcept {
    return replica_manager_;
}

const replication::ReplicaManager&
Master::GetReplicaManager() const noexcept {
    return replica_manager_;
}

replication::PlacementPolicy&
Master::GetPlacementPolicy() noexcept {
    return placement_policy_;
}

const replication::PlacementPolicy&
Master::GetPlacementPolicy() const noexcept {
    return placement_policy_;
}

lease::LeaseManager&
Master::GetLeaseManager() noexcept {
    return lease_manager_;
}

const lease::LeaseManager&
Master::GetLeaseManager() const noexcept {
    return lease_manager_;
}

heartbeat::HeartbeatManager&
Master::GetHeartbeatManager() noexcept {
    return heartbeat_manager_;
}

const heartbeat::HeartbeatManager&
Master::GetHeartbeatManager() const noexcept {
    return heartbeat_manager_;
}

replication::ReReplicationManager&
Master::GetReReplicationManager() noexcept {
    return re_replication_manager_;
}

const replication::ReReplicationManager&
Master::GetReReplicationManager() const noexcept {
    return re_replication_manager_;
}

recovery::RecoveryManager&
Master::GetRecoveryManager() noexcept {
    return recovery_manager_;
}

const recovery::RecoveryManager&
Master::GetRecoveryManager() const noexcept {
    return recovery_manager_;
}

bool Master::Initialize() {
    if (persistence_directory_.empty()) {
        initialized_ = true;
        return true;
    }

    if (!operation_log_.IsOpen()) {
        if (!operation_log_.Open(
                persistence_directory_ /
                "operation.log")) {
            return false;
        }
    }

    if (!operation_log_.Validate()) {
        return false;
    }

    recovery::Checkpoint::State state;

    std::uint64_t checkpoint_sequence = 0;

    if (checkpoint_.LoadLatest(state)) {
        checkpoint_sequence =
            state.sequence;

        if (checkpoint_sequence >
            operation_log_.LastSequence()) {
            return false;
        }

        if (!checkpoint_.Restore(
                state,
                metadata_,
                namespace_manager_)) {
            return false;
        }
    } else {
        metadata_.Clear();
        namespace_manager_.Clear();
    }

    const auto records =
        operation_log_.Replay(
            checkpoint_sequence);

    for (const auto& record :
         records) {
        if (!ReplayOperation(record)) {
            return false;
        }
    }

    /*
     * Replica locations are runtime state.
     * They are deliberately not reconstructed
     * from the checkpoint or operation log.
     */
    replica_manager_.Clear();

    initialized_ = true;
    return true;
}

bool Master::CreateCheckpoint() {
    if (persistence_directory_.empty() ||
        !operation_log_.IsOpen()) {
        return false;
    }

    return checkpoint_.Create(
        metadata_,
        namespace_manager_,
        operation_log_.LastSequence());
}

std::uint64_t
Master::GetLastOperationSequence()
    const noexcept {
    return operation_log_.LastSequence();
}

const std::filesystem::path&
Master::GetPersistenceDirectory()
    const noexcept {
    return persistence_directory_;
}

const std::filesystem::path&
Master::GetOperationLogPath()
    const noexcept {
    return operation_log_.GetPath();
}

bool Master::ReplayOperation(
    const recovery::OperationRecord& record) {
    std::vector<std::string> fields;

    if (!recovery::OperationLog::DecodeFields(
            record.payload,
            fields)) {
        return false;
    }

    auto parse_u64 =
        [](const std::string& value,
           std::uint64_t& result) {
            try {
                std::size_t position = 0;

                result =
                    std::stoull(
                        value,
                        &position);

                return position ==
                       value.size();
            } catch (...) {
                return false;
            }
        };

    switch (record.type) {
    case recovery::OperationType::CreateDirectory: {
        if (fields.size() != 1) {
            return false;
        }

        if (namespace_manager_.Exists(fields[0])) {
            return namespace_manager_.IsDirectory(
                fields[0]);
        }

        return namespace_manager_.CreateDirectory(
            fields[0]);
    }

    case recovery::OperationType::CreateFile: {
        if (fields.size() != 2) {
            return false;
        }

        std::uint64_t factor = 0;

        if (!parse_u64(fields[1], factor) ||
            factor == 0 ||
            factor >
                std::numeric_limits<
                    std::uint32_t>::max()) {
            return false;
        }

        if (namespace_manager_.Exists(fields[0])) {
            return namespace_manager_.IsFile(
                       fields[0]) &&
                   metadata_.FileExists(fields[0]);
        }

        if (!namespace_manager_.CreateFile(
                fields[0])) {
            return false;
        }

        return metadata_.CreateFile(
            fields[0],
            static_cast<std::uint32_t>(
                factor));
    }

    case recovery::OperationType::DeleteFile: {
        if (fields.size() != 1) {
            return false;
        }

        const bool metadata_exists =
            metadata_.FileExists(fields[0]);

        const bool namespace_exists =
            namespace_manager_.IsFile(fields[0]);

        if (!metadata_exists &&
            !namespace_exists) {
            return true;
        }

        if (metadata_exists &&
            !metadata_.DeleteFile(fields[0])) {
            return false;
        }

        if (namespace_exists &&
            !namespace_manager_.DeleteFile(
                fields[0])) {
            return false;
        }

        return true;
    }

    case recovery::OperationType::RenameFile: {
        if (fields.size() != 2) {
            return false;
        }

        const std::string& source =
            fields[0];

        const std::string& destination =
            fields[1];

        if (!namespace_manager_.Exists(source) &&
            namespace_manager_.IsFile(destination) &&
            metadata_.FileExists(destination)) {
            return true;
        }

        if (!namespace_manager_.IsFile(source) ||
            namespace_manager_.Exists(destination) ||
            !metadata_.FileExists(source)) {
            return false;
        }

        if (!namespace_manager_.Rename(
                source,
                destination)) {
            return false;
        }

        if (!metadata_.RenameFile(
                source,
                destination)) {
            return false;
        }

        return true;
    }

    case recovery::OperationType::AllocateChunk: {
        if (fields.size() != 4) {
            return false;
        }

        std::uint64_t handle = 0;
        std::uint64_t version = 0;
        std::uint64_t size = 0;

        if (!parse_u64(fields[1], handle) ||
            !parse_u64(fields[2], version) ||
            !parse_u64(fields[3], size) ||
            handle == 0 ||
            version == 0) {
            return false;
        }

        if (metadata_.ChunkExists(
                static_cast<ChunkHandle>(handle))) {
            return true;
        }

        return metadata_.AllocateChunk(
            fields[0],
            static_cast<ChunkHandle>(handle),
            static_cast<ChunkVersion>(version),
            size);
    }

    case recovery::OperationType::UpdateFileSize: {
        if (fields.size() != 2) {
            return false;
        }

        std::uint64_t size = 0;

        if (!parse_u64(fields[1], size)) {
            return false;
        }

        return metadata_.UpdateFileSize(
            fields[0],
            size);
    }

    case recovery::OperationType::SetChunkVersion: {
        if (fields.size() != 2) {
            return false;
        }

        std::uint64_t handle = 0;
        std::uint64_t version = 0;

        if (!parse_u64(fields[0], handle) ||
            !parse_u64(fields[1], version) ||
            handle == 0 ||
            version == 0) {
            return false;
        }

        return metadata_.SetChunkVersion(
            static_cast<ChunkHandle>(handle),
            static_cast<ChunkVersion>(version));
    }

    case recovery::OperationType::AddChunkToFile: {
        if (fields.size() != 2) {
            return false;
        }

        std::uint64_t handle = 0;

        if (!parse_u64(fields[1], handle) ||
            handle == 0) {
            return false;
        }

        const auto chunks =
            metadata_.GetFileChunks(fields[0]);

        if (std::find(
                chunks.begin(),
                chunks.end(),
                static_cast<ChunkHandle>(handle)) !=
            chunks.end()) {
            return true;
        }

        return metadata_.AddChunkToFile(
            fields[0],
            static_cast<ChunkHandle>(handle));
    }

    case recovery::OperationType::RemoveChunkFromFile: {
        if (fields.size() != 2) {
            return false;
        }

        std::uint64_t handle = 0;

        if (!parse_u64(fields[1], handle) ||
            handle == 0) {
            return false;
        }

        const auto chunks =
            metadata_.GetFileChunks(fields[0]);

        if (std::find(
                chunks.begin(),
                chunks.end(),
                static_cast<ChunkHandle>(handle)) ==
            chunks.end()) {
            return true;
        }

        return metadata_.RemoveChunkFromFile(
            fields[0],
            static_cast<ChunkHandle>(handle));
    }

    case recovery::OperationType::DeleteChunk: {
        if (fields.size() != 1) {
            return false;
        }

        std::uint64_t handle = 0;

        if (!parse_u64(fields[0], handle) ||
            handle == 0) {
            return false;
        }

        if (!metadata_.ChunkExists(
                static_cast<ChunkHandle>(handle))) {
            return true;
        }

        return metadata_.DeleteChunk(
            static_cast<ChunkHandle>(handle));
    }

    case recovery::OperationType::SetChunkSize: {
        if (fields.size() != 2) {
            return false;
        }

        std::uint64_t handle = 0;
        std::uint64_t size = 0;

        if (!parse_u64(fields[0], handle) ||
            !parse_u64(fields[1], size) ||
            handle == 0) {
            return false;
        }

        return metadata_.SetChunkSize(
            static_cast<ChunkHandle>(handle),
            size);
    }
    }

    return false;
}

/*
 * ------------------------------------------------------------
 * Existing Phase 1-10 functionality
 * ------------------------------------------------------------
 */

bool Master::RegisterChunkReplica(
    ChunkHandle handle,
    ServerId server_id,
    bool is_primary) {
    return RegisterReplica(
        handle,
        server_id,
        is_primary);
}

bool Master::HasChunkReplica(
    ChunkHandle handle,
    ServerId server_id) const {
    return HasReplica(
        handle,
        server_id);
}

std::optional<ServerId>
Master::GetChunkPrimary(
    ChunkHandle handle) const {
    return GetPrimary(handle);
}

std::vector<ServerId>
Master::PlaceChunkReplicas(
    ChunkHandle handle,
    const std::vector<
        replication::PlacementCandidate>& candidates) {
    const auto servers =
        SelectReplicaServers(
            handle,
            candidates);

    if (servers.empty()) {
        return {};
    }

    bool primary_registered = false;

    for (const ServerId server_id :
         servers) {
        const bool is_primary =
            !primary_registered;

        if (!RegisterReplica(
                handle,
                server_id,
                is_primary)) {
            return {};
        }

        if (is_primary) {
            primary_registered = true;
        }
    }

    if (!primary_registered) {
        return {};
    }

    return servers;
}

bool Master::SetChunkPrimary(
    ChunkHandle handle,
    ServerId server_id) {
    return SetPrimary(
        handle,
        server_id);
}

bool Master::AddReplica(
    ChunkHandle handle,
    ServerId server_id) {
    if (!metadata_.AddReplica(
            handle,
            server_id)) {
        return false;
    }

    if (!replica_manager_.RegisterReplica(
            handle,
            server_id,
            false)) {
        static_cast<void>(
            metadata_.RemoveReplica(
                handle,
                server_id));

        return false;
    }

    return true;
}

std::vector<ServerId>
Master::GetChunkReplicas(
    ChunkHandle handle) const {
    return replica_manager_
        .GetReplicaServers(handle);
}

bool Master::RegisterReplica(
    ChunkHandle handle,
    ServerId server_id,
    bool is_primary) {
    return replica_manager_.RegisterReplica(
        handle,
        server_id,
        is_primary);
}

bool Master::RemoveReplica(
    ChunkHandle handle,
    ServerId server_id) {
    const bool removed =
        replica_manager_.RemoveReplica(
            handle,
            server_id);

    if (removed) {
        static_cast<void>(
            metadata_.RemoveReplica(
                handle,
                server_id));

        const auto primary =
            replica_manager_.GetPrimary(handle);

        const auto lease =
            lease_manager_.GetLease(handle);

        if (lease.has_value() &&
            (!primary.has_value() ||
             *primary !=
                 lease->primary_server_id)) {
            static_cast<void>(
                lease_manager_.ReleaseLease(
                    handle));
        }
    }

    return removed;
}

bool Master::HasReplica(
    ChunkHandle handle,
    ServerId server_id) const {
    return replica_manager_.HasReplica(
        handle,
        server_id);
}

std::vector<ServerId>
Master::GetReplicaServers(
    ChunkHandle handle) const {
    return replica_manager_
        .GetReplicaServers(handle);
}

std::optional<ServerId>
Master::GetPrimary(
    ChunkHandle handle) const {
    return replica_manager_
        .GetPrimary(handle);
}

bool Master::SetPrimary(
    ChunkHandle handle,
    ServerId server_id) {
    if (!replica_manager_.SetPrimary(
            handle,
            server_id)) {
        return false;
    }

    const auto lease =
        lease_manager_.GetLease(handle);

    if (lease.has_value() &&
        lease->primary_server_id !=
            server_id) {
        static_cast<void>(
            lease_manager_.ReleaseLease(handle));
    }

    return true;
}

std::vector<ServerId>
Master::SelectReplicaServers(
    ChunkHandle handle,
    const std::vector<
        replication::PlacementCandidate>& candidates)
    const {
    return placement_policy_.SelectReplicas(
        handle,
        candidates);
}

std::optional<lease::Lease>
Master::AcquireLease(
    ChunkHandle handle,
    ServerId primary_server_id) {
    if (handle == 0 ||
        primary_server_id == 0) {
        return std::nullopt;
    }

    const auto primary =
        replica_manager_.GetPrimary(handle);

    if (!primary.has_value() ||
        *primary != primary_server_id) {
        return std::nullopt;
    }

    ChunkVersion version = 1;

    const auto chunk =
        metadata_.GetChunk(handle);

    if (chunk.has_value()) {
        version = chunk->version;

        if (version == 0) {
            version = 1;
        }
    }

    return lease_manager_.AcquireLease(
        handle,
        primary_server_id,
        version);
}

std::optional<lease::Lease>
Master::GetLease(
    ChunkHandle handle) const {
    return lease_manager_.GetLease(handle);
}

bool Master::IsLeaseValid(
    ChunkHandle handle) const {
    return lease_manager_.IsLeaseValid(handle);
}

bool Master::IsLeaseValid(
    ChunkHandle handle,
    ServerId primary_server_id) const {
    return lease_manager_.IsLeaseValid(
        handle,
        primary_server_id);
}

bool Master::ExtendLease(
    ChunkHandle handle,
    ServerId primary_server_id) {
    return lease_manager_.ExtendLease(
        handle,
        primary_server_id);
}

bool Master::ExtendLease(
    ChunkHandle handle,
    ServerId primary_server_id,
    std::uint64_t extension_ms) {
    return lease_manager_.ExtendLease(
        handle,
        primary_server_id,
        extension_ms);
}

bool Master::ReleaseLease(
    ChunkHandle handle) {
    return lease_manager_.ReleaseLease(handle);
}

bool Master::ProcessHeartbeat(
    ServerId server_id,
    std::uint64_t timestamp_ms,
    const std::vector<
        heartbeat::ReportedChunk>& chunks) {
    if (!heartbeat_manager_.ProcessHeartbeat(
            server_id,
            timestamp_ms,
            chunks)) {
        return false;
    }

    for (const auto& chunk : chunks) {
        if (chunk.handle == 0 ||
            chunk.version == 0) {
            continue;
        }

        if (!replica_manager_.HasReplica(
                chunk.handle,
                server_id)) {
            static_cast<void>(
                replica_manager_.RegisterReplica(
                    chunk.handle,
                    server_id,
                    false));
        }
    }

    return true;
}

bool Master::ProcessHeartbeat(
    ServerId server_id,
    std::uint64_t timestamp_ms) {
    return heartbeat_manager_.ProcessHeartbeat(
        server_id,
        timestamp_ms);
}

bool Master::IsChunkserverAlive(
    ServerId server_id) const {
    return heartbeat_manager_
        .IsServerAlive(server_id);
}

bool Master::IsChunkserverAlive(
    ServerId server_id,
    std::uint64_t now_ms) const {
    return heartbeat_manager_
        .IsServerAlive(
            server_id,
            now_ms);
}

std::vector<ServerId>
Master::DetectFailedChunkservers(
    std::uint64_t now_ms) {
    return heartbeat_manager_
        .DetectFailedServers(now_ms);
}

bool Master::IsStaleReplica(
    ChunkHandle handle,
    ServerId server_id) const {
    if (handle == 0 ||
        server_id == 0) {
        return false;
    }

    const auto master_version =
        metadata_.GetChunkVersion(handle);

    if (!master_version.has_value() ||
        *master_version == 0) {
        return false;
    }

    const auto state =
        heartbeat_manager_
            .GetServerState(server_id);

    if (!state.has_value()) {
        return false;
    }

    const auto reported_chunks =
        state->GetReportedChunks();

    const auto it =
        std::find_if(
            reported_chunks.begin(),
            reported_chunks.end(),
            [handle](
                const heartbeat::ReportedChunk& chunk) {
                return chunk.handle == handle;
            });

    if (it == reported_chunks.end()) {
        return false;
    }

    return it->version < *master_version;
}

std::vector<ServerId>
Master::GetStaleReplicas(
    ChunkHandle handle) const {
    std::vector<ServerId> stale;

    if (handle == 0) {
        return stale;
    }

    const auto master_version =
        metadata_.GetChunkVersion(handle);

    if (!master_version.has_value() ||
        *master_version == 0) {
        return stale;
    }

    const auto replicas =
        replica_manager_
            .GetReplicaServers(handle);

    for (const ServerId server_id :
         replicas) {
        if (IsStaleReplica(
                handle,
                server_id)) {
            stale.push_back(server_id);
        }
    }

    std::sort(
        stale.begin(),
        stale.end());

    return stale;
}

std::vector<ChunkHandle>
Master::GetStaleChunks(
    ServerId server_id) const {
    std::vector<ChunkHandle> stale;

    if (server_id == 0) {
        return stale;
    }

    const auto state =
        heartbeat_manager_
            .GetServerState(server_id);

    if (!state.has_value()) {
        return stale;
    }

    const auto reported_chunks =
        state->GetReportedChunks();

    for (const auto& reported :
         reported_chunks) {
        if (IsStaleReplica(
                reported.handle,
                server_id)) {
            stale.push_back(
                reported.handle);
        }
    }

    std::sort(
        stale.begin(),
        stale.end());

    return stale;
}

}  // namespace gfs::master