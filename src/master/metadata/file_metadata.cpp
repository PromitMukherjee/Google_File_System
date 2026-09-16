#include "gfs/master/metadata/file_metadata.hpp"

#include <algorithm>
#include <utility>

namespace gfs::master::metadata {

FileMetadata::FileMetadata(
    std::string path,
    std::uint32_t replication_factor)
    : path(std::move(path)),
      replication_factor(replication_factor) {}

const std::string& FileMetadata::GetPath() const noexcept {
    return path;
}

void FileMetadata::SetPath(std::string value) {
    path = std::move(value);
}

std::uint64_t FileMetadata::GetSize() const noexcept {
    return size;
}

void FileMetadata::SetSize(std::uint64_t value) noexcept {
    size = value;
}

std::uint32_t FileMetadata::GetReplicationFactor() const noexcept {
    return replication_factor;
}

void FileMetadata::SetReplicationFactor(
    std::uint32_t value) noexcept {
    replication_factor = value;
}

bool FileMetadata::AddChunk(ChunkHandle chunk_handle) {
    if (HasChunk(chunk_handle)) {
        return false;
    }

    chunk_handles.push_back(chunk_handle);
    return true;
}

bool FileMetadata::RemoveChunk(ChunkHandle chunk_handle) {
    const auto it = std::find(
        chunk_handles.begin(),
        chunk_handles.end(),
        chunk_handle);

    if (it == chunk_handles.end()) {
        return false;
    }

    chunk_handles.erase(it);
    return true;
}

bool FileMetadata::HasChunk(ChunkHandle chunk_handle) const {
    return std::find(
        chunk_handles.begin(),
        chunk_handles.end(),
        chunk_handle) != chunk_handles.end();
}

const std::vector<ChunkHandle>&
FileMetadata::GetChunkHandles() const noexcept {
    return chunk_handles;
}

std::size_t FileMetadata::ChunkCount() const noexcept {
    return chunk_handles.size();
}

void FileMetadata::ClearChunks() noexcept {
    chunk_handles.clear();
}

}  // namespace gfs::master::metadata