#pragma once

#include "gfs/chunkserver/storage/chunk_file.hpp"
#include "gfs/chunkserver/storage/chunk_storage.hpp"
#include "gfs/common/types.hpp"

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

namespace gfs::chunkserver::storage {

class StorageManager {
public:
    explicit StorageManager(std::string storage_directory);
    ~StorageManager();

    StorageManager(const StorageManager&) = delete;
    StorageManager& operator=(const StorageManager&) = delete;
    StorageManager(StorageManager&&) = delete;
    StorageManager& operator=(StorageManager&&) = delete;

    bool Initialize();

    bool CreateChunk(ChunkHandle handle);
    bool OpenChunk(ChunkHandle handle);
    bool DeleteChunk(ChunkHandle handle);

    bool ChunkExists(ChunkHandle handle) const;

    std::shared_ptr<ChunkFile> GetChunk(
        ChunkHandle handle);

    std::shared_ptr<const ChunkFile> GetChunk(
        ChunkHandle handle) const;

    bool ReadChunk(
        ChunkHandle handle,
        std::uint64_t offset,
        std::size_t length,
        std::vector<std::uint8_t>& data);

    bool WriteChunk(
        ChunkHandle handle,
        std::uint64_t offset,
        const std::vector<std::uint8_t>& data);

    std::uint64_t GetChunkSize(
        ChunkHandle handle) const;

    std::vector<ChunkHandle> ListChunks() const;

    const std::string& GetStorageDirectory() const noexcept;

    std::size_t ChunkCount() const;

private:
    ChunkStorage storage_;
};

}  // namespace gfs::chunkserver::storage