#pragma once

#include "gfs/chunkserver/storage/storage_manager.hpp"
#include "gfs/common/types.hpp"

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

namespace gfs::chunkserver {

class Chunkserver {
public:
    Chunkserver(
        ServerId server_id,
        std::string storage_directory);

    ~Chunkserver();

    Chunkserver(const Chunkserver&) = delete;
    Chunkserver& operator=(const Chunkserver&) = delete;
    Chunkserver(Chunkserver&&) = delete;
    Chunkserver& operator=(Chunkserver&&) = delete;

    bool Initialize();

    ServerId GetServerId() const noexcept;

    const std::string&
    GetStorageDirectory() const noexcept;

    bool CreateChunk(ChunkHandle handle);
    bool OpenChunk(ChunkHandle handle);
    bool DeleteChunk(ChunkHandle handle);

    bool ChunkExists(ChunkHandle handle) const;

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

    std::size_t ChunkCount() const;

    storage::StorageManager&
    GetStorageManager() noexcept;

    const storage::StorageManager&
    GetStorageManager() const noexcept;

private:
    ServerId server_id_;
    storage::StorageManager storage_manager_;
};

}  // namespace gfs::chunkserver