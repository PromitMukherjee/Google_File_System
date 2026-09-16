#include "gfs/master/metadata/file_metadata.hpp"

#include <algorithm>
#include <utility>

namespace gfs::master::metadata {

FileMetadata::FileMetadata(
    std::string path,
    std::uint32_t replication_factor)
    : path_(std::move(path)),
      replication_factor_(replication_factor) {
}

const std::string& FileMetadata::GetPath() const noexcept {
    return path_;
}

void FileMetadata::SetPath(std::string path) {
    path_ = std::move(path);
}

std::uint64_t FileMetadata::GetSize() const noexcept {
    return size_;
}

void FileMetadata::SetSize(std::uint64_t size) noexcept {
    size_ = size;
}

std::uint32_t FileMetadata::GetReplicationFactor() const noexcept {
    return replication_factor_;
}

void FileMetadata::SetReplicationFactor(
    std::uint32_t replication_factor) noexcept {

    replication_factor_ = replication_factor;
}

bool FileMetadata::AddChunk(ChunkHandle chunk_handle) {
    if (HasChunk(chunk_handle)) {
        return false;
    }

    chunk_handles_.push_back(chunk_handle);
    return true;
}

bool FileMetadata::RemoveChunk(ChunkHandle chunk_handle) {
    const auto it = std::find(
        chunk_handles_.begin(),
        chunk_handles_.end(),
        chunk_handle);

    if (it == chunk_handles_.end()) {
        return false;
    }

    chunk_handles_.erase(it);
    return true;
}

bool FileMetadata::HasChunk(ChunkHandle chunk_handle) const {
    return std::find(
        chunk_handles_.begin(),
        chunk_handles_.end(),
        chunk_handle) != chunk_handles_.end();
}

const std::vector<ChunkHandle>&
FileMetadata::GetChunkHandles() const noexcept {
    return chunk_handles_;
}

std::size_t FileMetadata::ChunkCount() const noexcept {
    return chunk_handles_.size();
}

void FileMetadata::ClearChunks() noexcept {
    chunk_handles_.clear();
}

}  // namespace gfs::master::metadata