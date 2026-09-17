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
    Metadata() = default;
    ~Metadata() = default;

    Metadata(const Metadata&) = delete;
    Metadata& operator=(const Metadata&) = delete;
    Metadata(Metadata&&) = delete;
    Metadata& operator=(Metadata&&) = delete;

    bool CreateFile(
        const std::string& path,
        std::uint32_t replication_factor = 3);

    bool DeleteFile(const std::string& path);

    bool RenameFile(
        const std::string& source_path,
        const std::string& destination_path);

    bool FileExists(const std::string& path) const;

    std::optional<FileMetadata> GetFile(
        const std::string& path) const;

    bool UpdateFileSize(
        const std::string& path,
        std::uint64_t size);

    std::optional<ChunkHandle> AllocateChunk(
        const std::string& path);

    bool AddChunkToFile(
        const std::string& path,
        ChunkHandle chunk_handle);

    bool RemoveChunkFromFile(
        const std::string& path,
        ChunkHandle chunk_handle);

    std::optional<ChunkMetadata> GetChunk(
        ChunkHandle chunk_handle) const;

    bool ChunkExists(ChunkHandle chunk_handle) const;

    bool DeleteChunk(ChunkHandle chunk_handle);

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

private:
    ChunkHandle GenerateChunkHandle();

    std::unordered_map<std::string, FileMetadata> files_;
    std::unordered_map<ChunkHandle, ChunkMetadata> chunks_;

    ChunkHandle next_chunk_handle_ = 1;
};

}  // namespace gfs::master::metadata