#include "gfs/master/metadata/metadata.hpp"

#include <algorithm>

namespace gfs::master::metadata {

bool Metadata::CreateFile(
    const std::string& path,
    std::uint32_t replication_factor) {

    if (path.empty() || files_.contains(path)) {
        return false;
    }

    if (replication_factor == 0) {
        return false;
    }

    FileMetadata metadata(path, replication_factor);
    files_.emplace(path, std::move(metadata));
    return true;
}

bool Metadata::DeleteFile(const std::string& path) {
    const auto file_it = files_.find(path);
    if (file_it == files_.end()) {
        return false;
    }

    for (const ChunkHandle handle : file_it->second.GetChunkHandles()) {
        chunks_.erase(handle);
    }

    files_.erase(file_it);
    return true;
}

bool Metadata::RenameFile(
    const std::string& source_path,
    const std::string& destination_path) {

    if (source_path.empty() ||
        destination_path.empty() ||
        source_path == destination_path) {
        return false;
    }

    const auto source_it = files_.find(source_path);
    if (source_it == files_.end() ||
        files_.contains(destination_path)) {
        return false;
    }

    FileMetadata metadata = source_it->second;
    metadata.SetPath(destination_path);

    files_.erase(source_it);
    files_.emplace(destination_path, std::move(metadata));

    return true;
}

bool Metadata::FileExists(const std::string& path) const {
    return files_.contains(path);
}

std::optional<FileMetadata> Metadata::GetFile(
    const std::string& path) const {

    const auto it = files_.find(path);
    if (it == files_.end()) {
        return std::nullopt;
    }

    return it->second;
}

bool Metadata::UpdateFileSize(
    const std::string& path,
    std::uint64_t size) {

    const auto it = files_.find(path);
    if (it == files_.end()) {
        return false;
    }

    it->second.SetSize(size);
    return true;
}

std::optional<ChunkHandle> Metadata::AllocateChunk(
    const std::string& path) {

    const auto file_it = files_.find(path);
    if (file_it == files_.end()) {
        return std::nullopt;
    }

    const ChunkHandle handle = GenerateChunkHandle();

    ChunkMetadata chunk(handle);
    chunk.SetVersion(1);
    chunk.SetSize(0);

    chunks_.emplace(handle, std::move(chunk));
    file_it->second.AddChunk(handle);

    return handle;
}

bool Metadata::AddChunkToFile(
    const std::string& path,
    ChunkHandle chunk_handle) {

    const auto file_it = files_.find(path);
    const auto chunk_it = chunks_.find(chunk_handle);

    if (file_it == files_.end() ||
        chunk_it == chunks_.end()) {
        return false;
    }

    return file_it->second.AddChunk(chunk_handle);
}

bool Metadata::RemoveChunkFromFile(
    const std::string& path,
    ChunkHandle chunk_handle) {

    const auto file_it = files_.find(path);
    if (file_it == files_.end()) {
        return false;
    }

    return file_it->second.RemoveChunk(chunk_handle);
}

std::optional<ChunkMetadata> Metadata::GetChunk(
    ChunkHandle chunk_handle) const {

    const auto it = chunks_.find(chunk_handle);
    if (it == chunks_.end()) {
        return std::nullopt;
    }

    return it->second;
}

bool Metadata::ChunkExists(ChunkHandle chunk_handle) const {
    return chunks_.contains(chunk_handle);
}

bool Metadata::DeleteChunk(ChunkHandle chunk_handle) {
    for (auto& [path, file] : files_) {
        file.RemoveChunk(chunk_handle);
    }

    return chunks_.erase(chunk_handle) != 0;
}

bool Metadata::AddReplica(
    ChunkHandle chunk_handle,
    ServerId server_id) {

    const auto it = chunks_.find(chunk_handle);
    if (it == chunks_.end()) {
        return false;
    }

    return it->second.AddReplica(server_id);
}

bool Metadata::RemoveReplica(
    ChunkHandle chunk_handle,
    ServerId server_id) {

    const auto it = chunks_.find(chunk_handle);
    if (it == chunks_.end()) {
        return false;
    }

    return it->second.RemoveReplica(server_id);
}

std::vector<ServerId> Metadata::GetReplicas(
    ChunkHandle chunk_handle) const {

    const auto it = chunks_.find(chunk_handle);
    if (it == chunks_.end()) {
        return {};
    }

    return it->second.GetReplicaServerIds();
}

bool Metadata::SetChunkVersion(
    ChunkHandle chunk_handle,
    ChunkVersion version) {

    const auto it = chunks_.find(chunk_handle);
    if (it == chunks_.end() || version == 0) {
        return false;
    }

    it->second.SetVersion(version);
    return true;
}

std::optional<ChunkVersion> Metadata::GetChunkVersion(
    ChunkHandle chunk_handle) const {

    const auto it = chunks_.find(chunk_handle);
    if (it == chunks_.end()) {
        return std::nullopt;
    }

    return it->second.GetVersion();
}

bool Metadata::SetChunkSize(
    ChunkHandle chunk_handle,
    std::uint64_t size) {

    const auto it = chunks_.find(chunk_handle);
    if (it == chunks_.end()) {
        return false;
    }

    it->second.SetSize(size);
    return true;
}

std::optional<std::size_t> Metadata::GetChunkCount(
    const std::string& path) const {

    const auto it = files_.find(path);
    if (it == files_.end()) {
        return std::nullopt;
    }

    return it->second.ChunkCount();
}

std::vector<ChunkHandle> Metadata::GetFileChunks(
    const std::string& path) const {

    const auto it = files_.find(path);
    if (it == files_.end()) {
        return {};
    }

    return it->second.GetChunkHandles();
}

std::size_t Metadata::FileCount() const {
    return files_.size();
}

std::size_t Metadata::ChunkCount() const {
    return chunks_.size();
}

ChunkHandle Metadata::GenerateChunkHandle() {
    while (chunks_.contains(next_chunk_handle_)) {
        ++next_chunk_handle_;
    }

    return next_chunk_handle_++;
}

}  // namespace gfs::master::metadata