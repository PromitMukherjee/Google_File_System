#include "gfs/master/metadata/metadata.hpp"

#include <algorithm>
#include <cstdint>
#include <optional>
#include <utility>
#include <vector>

namespace gfs::master::metadata {

bool Metadata::CreateFile(
    const std::string& path,
    std::uint32_t replication_factor) {
    if (path.empty() ||
        files_.contains(path) ||
        replication_factor == 0) {
        return false;
    }

    FileMetadata file(
        path,
        replication_factor);

    files_.emplace(
        path,
        std::move(file));

    return true;
}

bool Metadata::DeleteFile(
    const std::string& path) {
    const auto file_it =
        files_.find(path);

    if (file_it == files_.end()) {
        return false;
    }

    const auto chunk_handles =
        file_it->second.GetChunkHandles();

    for (const ChunkHandle handle :
         chunk_handles) {
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

    const auto source_it =
        files_.find(source_path);

    if (source_it == files_.end() ||
        files_.contains(destination_path)) {
        return false;
    }

    FileMetadata file =
        source_it->second;

    file.SetPath(destination_path);

    files_.erase(source_it);

    files_.emplace(
        destination_path,
        std::move(file));

    return true;
}

bool Metadata::FileExists(
    const std::string& path) const {
    return files_.contains(path);
}

std::optional<FileMetadata>
Metadata::GetFile(
    const std::string& path) const {
    const auto it =
        files_.find(path);

    if (it == files_.end()) {
        return std::nullopt;
    }

    return it->second;
}

bool Metadata::UpdateFileSize(
    const std::string& path,
    std::uint64_t size) {
    const auto it =
        files_.find(path);

    if (it == files_.end()) {
        return false;
    }

    it->second.SetSize(size);

    return true;
}

std::optional<ChunkHandle>
Metadata::AllocateChunk(
    const std::string& path) {
    if (!files_.contains(path)) {
        return std::nullopt;
    }

    const ChunkHandle handle =
        GenerateChunkHandle();

    if (handle == 0) {
        return std::nullopt;
    }

    if (!AllocateChunk(
            path,
            handle,
            1,
            0)) {
        return std::nullopt;
    }

    return handle;
}

bool Metadata::AllocateChunk(
    const std::string& path,
    ChunkHandle handle,
    ChunkVersion version,
    std::uint64_t size) {
    if (path.empty() ||
        handle == 0 ||
        version == 0 ||
        files_.find(path) == files_.end() ||
        chunks_.contains(handle)) {
        return false;
    }

    ChunkMetadata chunk(
        handle,
        version);

    chunk.SetSize(size);

    const auto [chunk_it, inserted] =
        chunks_.emplace(
            handle,
            std::move(chunk));

    if (!inserted) {
        return false;
    }

    if (!files_.at(path).AddChunk(handle)) {
        chunks_.erase(chunk_it);
        return false;
    }

    if (handle >= next_chunk_handle_) {
        next_chunk_handle_ = handle + 1;

        if (next_chunk_handle_ == 0) {
            return false;
        }
    }

    return true;
}

bool Metadata::AddChunkToFile(
    const std::string& path,
    ChunkHandle chunk_handle) {
    const auto file_it =
        files_.find(path);

    const auto chunk_it =
        chunks_.find(chunk_handle);

    if (file_it == files_.end() ||
        chunk_it == chunks_.end()) {
        return false;
    }

    return file_it->second.AddChunk(
        chunk_handle);
}

bool Metadata::RemoveChunkFromFile(
    const std::string& path,
    ChunkHandle chunk_handle) {
    const auto file_it =
        files_.find(path);

    if (file_it == files_.end()) {
        return false;
    }

    return file_it->second.RemoveChunk(
        chunk_handle);
}

std::optional<ChunkMetadata>
Metadata::GetChunk(
    ChunkHandle chunk_handle) const {
    const auto it =
        chunks_.find(chunk_handle);

    if (it == chunks_.end()) {
        return std::nullopt;
    }

    return it->second;
}

bool Metadata::ChunkExists(
    ChunkHandle chunk_handle) const {
    return chunks_.contains(chunk_handle);
}

bool Metadata::DeleteChunk(
    ChunkHandle chunk_handle) {
    const auto chunk_it =
        chunks_.find(chunk_handle);

    if (chunk_it == chunks_.end()) {
        return false;
    }

    for (auto& [path, file] :
         files_) {
        file.RemoveChunk(
            chunk_handle);
    }

    chunks_.erase(chunk_it);

    return true;
}

bool Metadata::AddReplica(
    ChunkHandle chunk_handle,
    ServerId server_id) {
    const auto it =
        chunks_.find(chunk_handle);

    if (it == chunks_.end()) {
        return false;
    }

    return it->second.AddReplica(
        server_id);
}

bool Metadata::RemoveReplica(
    ChunkHandle chunk_handle,
    ServerId server_id) {
    const auto it =
        chunks_.find(chunk_handle);

    if (it == chunks_.end()) {
        return false;
    }

    return it->second.RemoveReplica(
        server_id);
}

std::vector<ServerId>
Metadata::GetReplicas(
    ChunkHandle chunk_handle) const {
    const auto it =
        chunks_.find(chunk_handle);

    if (it == chunks_.end()) {
        return {};
    }

    return it->second.GetReplicaServerIds();
}

bool Metadata::SetChunkVersion(
    ChunkHandle chunk_handle,
    ChunkVersion version) {
    const auto it =
        chunks_.find(chunk_handle);

    if (it == chunks_.end() ||
        version == 0) {
        return false;
    }

    it->second.SetVersion(version);

    return true;
}

std::optional<ChunkVersion>
Metadata::GetChunkVersion(
    ChunkHandle chunk_handle) const {
    const auto it =
        chunks_.find(chunk_handle);

    if (it == chunks_.end()) {
        return std::nullopt;
    }

    return it->second.GetVersion();
}

std::optional<std::uint32_t>
Metadata::GetChunkReplicationFactor(
    ChunkHandle chunk_handle) const {
    if (chunk_handle == 0 ||
        !chunks_.contains(chunk_handle)) {
        return std::nullopt;
    }

    for (const auto& [path, file] :
         files_) {
        if (file.HasChunk(chunk_handle)) {
            return file.GetReplicationFactor();
        }
    }

    return std::nullopt;
}

bool Metadata::SetChunkSize(
    ChunkHandle chunk_handle,
    std::uint64_t size) {
    const auto it =
        chunks_.find(chunk_handle);

    if (it == chunks_.end()) {
        return false;
    }

    it->second.SetSize(size);

    return true;
}

std::optional<std::size_t>
Metadata::GetChunkCount(
    const std::string& path) const {
    const auto it =
        files_.find(path);

    if (it == files_.end()) {
        return std::nullopt;
    }

    return it->second.ChunkCount();
}

std::vector<ChunkHandle>
Metadata::GetFileChunks(
    const std::string& path) const {
    const auto it =
        files_.find(path);

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

std::vector<Metadata::PersistentFile>
Metadata::ExportFiles() const {
    std::vector<PersistentFile> result;
    result.reserve(files_.size());

    for (const auto& [path, file] :
         files_) {
        result.push_back(
            PersistentFile{
                file.GetPath(),
                file.GetSize(),
                file.GetReplicationFactor(),
                file.GetChunkHandles()});
    }

    std::sort(
        result.begin(),
        result.end(),
        [](const PersistentFile& a,
           const PersistentFile& b) {
            return a.path < b.path;
        });

    return result;
}

std::vector<Metadata::PersistentChunk>
Metadata::ExportChunks() const {
    std::vector<PersistentChunk> result;
    result.reserve(chunks_.size());

    for (const auto& [handle, chunk] :
         chunks_) {
        result.push_back(
            PersistentChunk{
                chunk.GetHandle(),
                chunk.GetVersion(),
                chunk.GetSize()});
    }

    std::sort(
        result.begin(),
        result.end(),
        [](const PersistentChunk& a,
           const PersistentChunk& b) {
            return a.handle < b.handle;
        });

    return result;
}

void Metadata::Clear() {
    files_.clear();
    chunks_.clear();
    next_chunk_handle_ = 1;
}

bool Metadata::RestoreFile(
    const std::string& path,
    std::uint64_t size,
    std::uint32_t replication_factor,
    const std::vector<ChunkHandle>& chunk_handles) {
    if (path.empty() ||
        files_.contains(path) ||
        replication_factor == 0) {
        return false;
    }

    FileMetadata file(
        path,
        replication_factor);

    file.SetSize(size);

    for (const ChunkHandle handle :
         chunk_handles) {
        if (handle == 0 ||
            file.HasChunk(handle)) {
            return false;
        }

        if (!file.AddChunk(handle)) {
            return false;
        }
    }

    files_.emplace(
        path,
        std::move(file));

    return true;
}

bool Metadata::RestoreChunk(
    ChunkHandle handle,
    ChunkVersion version,
    std::uint64_t size) {
    if (handle == 0 ||
        version == 0 ||
        chunks_.contains(handle)) {
        return false;
    }

    ChunkMetadata chunk(
        handle,
        version);

    chunk.SetSize(size);

    const auto [it, inserted] =
        chunks_.emplace(
            handle,
            std::move(chunk));

    if (!inserted) {
        return false;
    }

    if (handle >= next_chunk_handle_) {
        next_chunk_handle_ = handle + 1;

        if (next_chunk_handle_ == 0) {
            return false;
        }
    }

    return true;
}

ChunkHandle Metadata::GetNextChunkHandle()
    const noexcept {
    return next_chunk_handle_;
}

ChunkHandle Metadata::GenerateChunkHandle() {
    while (next_chunk_handle_ != 0 &&
           chunks_.contains(
               next_chunk_handle_)) {
        ++next_chunk_handle_;
    }

    if (next_chunk_handle_ == 0) {
        return 0;
    }

    return next_chunk_handle_++;
}

}  // namespace gfs::master::metadata