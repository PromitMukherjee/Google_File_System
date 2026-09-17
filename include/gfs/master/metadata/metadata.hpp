#pragma once

#include "gfs/master/metadata/chunk_metadata.hpp"
#include "gfs/master/metadata/file_metadata.hpp"

#include <cstddef>
#include <cstdint>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

namespace gfs::master::metadata {

class Metadata {
public:
    struct PersistentFile {
        std::string path;
        std::uint64_t size = 0;
        std::uint32_t replication_factor = 3;
        std::vector<ChunkHandle> chunk_handles;
    };

    struct PersistentChunk {
        ChunkHandle handle = 0;
        ChunkVersion version = 1;
        std::uint64_t size = 0;
    };

    Metadata() = default;
    ~Metadata() = default;

    Metadata(const Metadata&) = delete;
    Metadata& operator=(const Metadata&) = delete;
    Metadata(Metadata&&) = delete;
    Metadata& operator=(Metadata&&) = delete;

    bool CreateFile(
        const std::string& path,
        std::uint32_t replication_factor = 3);

    bool DeleteFile(
        const std::string& path);

    bool RenameFile(
        const std::string& source_path,
        const std::string& destination_path);

    bool FileExists(
        const std::string& path) const;

    std::optional<FileMetadata> GetFile(
        const std::string& path) const;

    bool UpdateFileSize(
        const std::string& path,
        std::uint64_t size);

    std::optional<ChunkHandle> AllocateChunk(
        const std::string& path);

    bool AllocateChunk(
        const std::string& path,
        ChunkHandle handle,
        ChunkVersion version = 1,
        std::uint64_t size = 0);

    bool AddChunkToFile(
        const std::string& path,
        ChunkHandle chunk_handle);

    bool RemoveChunkFromFile(
        const std::string& path,
        ChunkHandle chunk_handle);

    std::optional<ChunkMetadata> GetChunk(
        ChunkHandle chunk_handle) const;

    bool ChunkExists(
        ChunkHandle chunk_handle) const;

    bool DeleteChunk(
        ChunkHandle chunk_handle);

    bool AddReplica(
        ChunkHandle chunk_handle,
        ServerId server_id);

    bool RemoveReplica(
        ChunkHandle chunk_handle,
        ServerId server_id);

    std::vector<ServerId> GetReplicas(
        ChunkHandle chunk_handle) const;

    bool SetChunkVersion(
        ChunkHandle chunk_handle,
        ChunkVersion version);

    std::optional<ChunkVersion> GetChunkVersion(
        ChunkHandle chunk_handle) const;

    std::optional<std::uint32_t>
    GetChunkReplicationFactor(
        ChunkHandle chunk_handle) const;

    bool SetChunkSize(
        ChunkHandle chunk_handle,
        std::uint64_t size);

    std::optional<std::size_t> GetChunkCount(
        const std::string& path) const;

    std::vector<ChunkHandle> GetFileChunks(
        const std::string& path) const;

    std::size_t FileCount() const;
    std::size_t ChunkCount() const;

    std::vector<PersistentFile>
    ExportFiles() const;

    std::vector<PersistentChunk>
    ExportChunks() const;

    void Clear();

    bool RestoreFile(
        const std::string& path,
        std::uint64_t size,
        std::uint32_t replication_factor,
        const std::vector<ChunkHandle>& chunk_handles);

    bool RestoreChunk(
        ChunkHandle handle,
        ChunkVersion version,
        std::uint64_t size);

    [[nodiscard]] ChunkHandle
    GetNextChunkHandle() const noexcept;

private:
    ChunkHandle GenerateChunkHandle();

    std::unordered_map<std::string, FileMetadata> files_;
    std::unordered_map<ChunkHandle, ChunkMetadata> chunks_;

    ChunkHandle next_chunk_handle_ = 1;
};

}  // namespace gfs::master::metadata