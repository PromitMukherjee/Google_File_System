#include "gfs/master/metadata/chunk_metadata.hpp"

#include <algorithm>

namespace gfs::master::metadata {

ChunkMetadata::ChunkMetadata(
    ChunkHandle chunk_handle,
    ChunkVersion chunk_version)
    : handle(chunk_handle),
      version(chunk_version) {
}

ChunkHandle ChunkMetadata::GetHandle() const noexcept {
    return handle;
}

ChunkVersion ChunkMetadata::GetVersion() const noexcept {
    return version;
}

void ChunkMetadata::SetVersion(
    ChunkVersion chunk_version) noexcept {
    if (chunk_version != 0) {
        version = chunk_version;
    }
}

std::uint64_t ChunkMetadata::GetSize() const noexcept {
    return size;
}

void ChunkMetadata::SetSize(
    std::uint64_t chunk_size) noexcept {
    size = chunk_size;
}

bool ChunkMetadata::AddReplica(
    ServerId server_id) {
    return replicas.insert(server_id).second;
}

bool ChunkMetadata::RemoveReplica(
    ServerId server_id) {
    return replicas.erase(server_id) != 0;
}

bool ChunkMetadata::HasReplica(
    ServerId server_id) const {
    return replicas.contains(server_id);
}

const std::unordered_set<ServerId>&
ChunkMetadata::GetReplicas() const noexcept {
    return replicas;
}

std::vector<ServerId>
ChunkMetadata::GetReplicaServerIds() const {
    std::vector<ServerId> server_ids(
        replicas.begin(),
        replicas.end());

    std::sort(
        server_ids.begin(),
        server_ids.end());

    return server_ids;
}

std::size_t ChunkMetadata::ReplicaCount() const noexcept {
    return replicas.size();
}

void ChunkMetadata::ClearReplicas() noexcept {
    replicas.clear();
}

}  // namespace gfs::master::metadata