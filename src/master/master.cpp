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
      placement_policy_(default_replication_factor) {}

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

    if (!metadata_.DeleteFile(path)) {
        return false;
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
    return replica_manager_.RemoveReplica(
        handle,
        server_id);
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
    return replica_manager_.SetPrimary(
        handle,
        server_id);
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
        SelectReplicaServers(handle, candidates);

    if (servers.empty()) {
        return {};
    }

    bool primary_registered = false;

    for (const ServerId server_id : servers) {
        const bool is_primary = !primary_registered;

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

}  // namespace gfs::master