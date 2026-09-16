#pragma once

#include "gfs/common/types.hpp"

#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

namespace gfs::master::metadata {

class FileMetadata {
public:
    FileMetadata() = default;

    explicit FileMetadata(
        std::string path,
        std::uint32_t replication_factor = 3);

    const std::string& GetPath() const noexcept;
    void SetPath(std::string path);

    std::uint64_t GetSize() const noexcept;
    void SetSize(std::uint64_t size) noexcept;

    std::uint32_t GetReplicationFactor() const noexcept;
    void SetReplicationFactor(std::uint32_t replication_factor) noexcept;

    bool AddChunk(ChunkHandle chunk_handle);
    bool RemoveChunk(ChunkHandle chunk_handle);
    bool HasChunk(ChunkHandle chunk_handle) const;

    const std::vector<ChunkHandle>& GetChunkHandles() const noexcept;
    std::size_t ChunkCount() const noexcept;

    void ClearChunks() noexcept;

private:
    std::string path_;
    std::uint64_t size_ = 0;
    std::uint32_t replication_factor_ = 3;
    std::vector<ChunkHandle> chunk_handles_;
};

}  // namespace gfs::master::metadata