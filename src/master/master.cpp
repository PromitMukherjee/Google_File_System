#include "gfs/master/master.hpp"

#include <cstddef>
#include <cstdint>
#include <optional>
#include <vector>

namespace gfs::master {

Master::Master(
    std::uint32_t default_replication_factor)
    : metadata_(),
      namespace_manager_(),
      replica_manager_(),
      placement_policy_(default_replication_factor),
      lease_manager_(replica_manager_) {
}

bool Master::Initialize() {
    return true;
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

    if (!namespace_manager_.CreateFile(path)) {
        return false;
    }

    if (!metadata_.CreateFile(path, factor)) {
        namespace_manager_.DeleteFile(path);
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

    if (!metadata_.DeleteFile(path)) {
        return false;
    }

    for (const ChunkHandle handle : chunks) {
        replica_manager_.RemoveChunk(handle);
        lease_manager_.ReleaseLease(handle);
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

    if (!namespace_manager_.Rename(
            source_path,
            destination_path)) {
        return false;
    }

    if (!metadata_.RenameFile(
            source_path,
            destination_path)) {
        namespace_manager_.Rename(
            destination_path,
            source_path);
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
    return namespace_manager_.CreateDirectory(path);
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

std::optional<ChunkHandle>
Master::AllocateChunk(
    const std::string& path) {
    return metadata_.AllocateChunk(path);
}

bool Master::AddChunkToFile(
    const std::string& path,
    ChunkHandle handle) {
    return metadata_.AddChunkToFile(
        path,
        handle);
}

bool Master::RemoveChunkFromFile(
    const std::string& path,
    ChunkHandle handle) {
    return metadata_.RemoveChunkFromFile(
        path,
        handle);
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
        metadata_.RemoveReplica(
            handle,
            server_id);

        const auto primary =
            replica_manager_.GetPrimary(handle);

        const auto lease =
            lease_manager_.GetLease(handle);

        if (lease.has_value() &&
            (!primary.has_value() ||
             *primary !=
                 lease->primary_server_id)) {
            lease_manager_.ReleaseLease(handle);
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
    return replica_manager_.GetReplicaServers(handle);
}

std::optional<ServerId>
Master::GetPrimary(
    ChunkHandle handle) const {
    return replica_manager_.GetPrimary(handle);
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
        lease_manager_.ReleaseLease(handle);
    }

    return true;
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
        metadata_.RemoveReplica(
            handle,
            server_id);
        return false;
    }

    return true;
}

std::vector<ServerId>
Master::GetChunkReplicas(
    ChunkHandle handle) const {
    return replica_manager_.GetReplicaServers(handle);
}

std::vector<ServerId>
Master::SelectReplicaServers(
    ChunkHandle handle,
    const std::vector<replication::PlacementCandidate>&
        candidates) const {
    replication::PlacementRequest request;
    request.handle = handle;
    request.candidates = candidates;

    return placement_policy_.SelectReplicas(request);
}

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
    const std::vector<replication::PlacementCandidate>&
        candidates) {
    const auto servers =
        SelectReplicaServers(
            handle,
            candidates);

    if (servers.empty()) {
        return {};
    }

    bool primary_registered = false;

    for (const ServerId server_id : servers) {
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

    /*
     * Phase 7 lease acquisition depends on the master's
     * current replica/primary state. A chunk does not need
     * to be present in the Phase 3 metadata table for the
     * lease test or for the lease manager to establish
     * primary authority.
     *
     * If metadata exists, preserve its current chunk
     * version. Otherwise use the initial GFS chunk version.
     */
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

}  // namespace gfs::master