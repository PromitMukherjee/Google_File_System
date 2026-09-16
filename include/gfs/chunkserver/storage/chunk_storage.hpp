#pragma once

#include "gfs/chunkserver/storage/chunk_file.hpp"
#include "gfs/common/types.hpp"

#include <cstddef>
#include <cstdint>
#include <memory>
#include <shared_mutex>
#include <string>
#include <unordered_map>
#include <vector>

namespace gfs::chunkserver::storage {

class ChunkStorage {
public:
    explicit ChunkStorage(std::string storage_directory);
    ~ChunkStorage();

    ChunkStorage(const ChunkStorage&) = delete;
    ChunkStorage& operator=(const ChunkStorage&) = delete;
    ChunkStorage(ChunkStorage&&) = delete;
    ChunkStorage& operator=(ChunkStorage&&) = delete;

    bool Initialize();

    bool CreateChunk(ChunkHandle handle);
    bool OpenChunk(ChunkHandle handle);
    bool DeleteChunk(ChunkHandle handle);

    bool ChunkExists(ChunkHandle handle) const;

    std::shared_ptr<ChunkFile> GetChunk(ChunkHandle handle);
    std::shared_ptr<const ChunkFile> GetChunk(ChunkHandle handle) const;

    bool ReadChunk(
        ChunkHandle handle,
        std::uint64_t offset,
        std::size_t length,
        std::vector<std::uint8_t>& data);

    bool WriteChunk(
        ChunkHandle handle,
        std::uint64_t offset,
        const std::vector<std::uint8_t>& data);

    std::uint64_t GetChunkSize(ChunkHandle handle) const;

    std::string GetChunkPath(ChunkHandle handle) const;

    std::vector<ChunkHandle> ListChunks() const;

    const std::string& GetStorageDirectory() const noexcept;

    std::size_t ChunkCount() const;

private:
    std::shared_ptr<ChunkFile> GetOrOpenChunk(ChunkHandle handle);

    std::string storage_directory_;

    mutable std::shared_mutex mutex_;

    std::unordered_map<
        ChunkHandle,
        std::shared_ptr<ChunkFile>> chunks_;
};

}  // namespace gfs::chunkserver::storage