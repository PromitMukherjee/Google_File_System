#include "gfs/master/metadata/chunk_metadata.hpp"

#include <algorithm>

namespace gfs::master::metadata {

ChunkMetadata::ChunkMetadata(
    ChunkHandle handle,
    ChunkVersion version)
    : handle_(handle),
      version_(version) {
}

ChunkHandle ChunkMetadata::GetHandle() const noexcept {
    return handle_;
}

ChunkVersion ChunkMetadata::GetVersion() const noexcept {
    return version_;
}

void ChunkMetadata::SetVersion(ChunkVersion version) noexcept {
    if (version != 0) {
        version_ = version;
    }
}

std::uint64_t ChunkMetadata::GetSize() const noexcept {
    return size_;
}

void ChunkMetadata::SetSize(std::uint64_t size) noexcept {
    size_ = size;
}

bool ChunkMetadata::AddReplica(ServerId server_id) {
    return replica_server_ids_.insert(server_id).second;
}

bool ChunkMetadata::RemoveReplica(ServerId server_id) {
    return replica_server_ids_.erase(server_id) != 0;
}

bool ChunkMetadata::HasReplica(ServerId server_id) const {
    return replica_server_ids_.contains(server_id);
}

const std::unordered_set<ServerId>&
ChunkMetadata::GetReplicas() const noexcept {
    return replica_server_ids_;
}

std::vector<ServerId> ChunkMetadata::GetReplicaServerIds() const {
    std::vector<ServerId> replicas(
        replica_server_ids_.begin(),
        replica_server_ids_.end());

    std::sort(replicas.begin(), replicas.end());
    return replicas;
}

std::size_t ChunkMetadata::ReplicaCount() const noexcept {
    return replica_server_ids_.size();
}

void ChunkMetadata::ClearReplicas() noexcept {
    replica_server_ids_.clear();
}

}  // namespace gfs::master::metadata