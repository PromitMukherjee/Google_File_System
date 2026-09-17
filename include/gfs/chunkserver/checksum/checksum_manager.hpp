#pragma once

#include "gfs/chunkserver/checksum/checksum_block.hpp"
#include "gfs/chunkserver/storage/storage_manager.hpp"
#include "gfs/common/types.hpp"

#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <optional>
#include <shared_mutex>
#include <string>
#include <unordered_map>
#include <vector>

namespace gfs::chunkserver::checksum {

class ChecksumManager {
public:
    explicit ChecksumManager(
        storage::StorageManager& storage_manager);

    ChecksumManager(const ChecksumManager&) = delete;
    ChecksumManager& operator=(const ChecksumManager&) = delete;
    ChecksumManager(ChecksumManager&&) = delete;
    ChecksumManager& operator=(ChecksumManager&&) = delete;

    [[nodiscard]] bool Initialize();

    [[nodiscard]] static std::uint32_t
    ComputeCRC32(
        const std::vector<std::uint8_t>& data);

    [[nodiscard]] bool ComputeChunkChecksums(
        ChunkHandle handle);

    [[nodiscard]] bool UpdateAfterWrite(
        ChunkHandle handle,
        std::uint64_t offset,
        std::size_t length);

    [[nodiscard]] bool UpdateAfterTruncate(
        ChunkHandle handle,
        std::uint64_t size);

    [[nodiscard]] bool VerifyChunk(
        ChunkHandle handle);

    [[nodiscard]] bool VerifyChunkRange(
        ChunkHandle handle,
        std::uint64_t offset,
        std::size_t length) const;

    [[nodiscard]] std::optional<std::uint32_t>
    GetChecksum(
        ChunkHandle handle,
        std::uint64_t block_index) const;

    [[nodiscard]] std::vector<ChecksumBlock>
    GetChecksums(
        ChunkHandle handle) const;

    [[nodiscard]] std::size_t
    ChecksumBlockCount(
        ChunkHandle handle) const;

    [[nodiscard]] bool HasChecksums(
        ChunkHandle handle) const;

    [[nodiscard]] bool DeleteChecksums(
        ChunkHandle handle);

    [[nodiscard]] std::string
    GetChecksumPath(
        ChunkHandle handle) const;

private:
    [[nodiscard]] bool RecomputeBlocksLocked(
        ChunkHandle handle,
        std::uint64_t first_block,
        std::uint64_t last_block);

    [[nodiscard]] bool LoadChecksumsLocked(
        ChunkHandle handle);

    [[nodiscard]] bool PersistChecksumsLocked(
        ChunkHandle handle) const;

    [[nodiscard]] bool ReadBlockLocked(
        ChunkHandle handle,
        std::uint64_t block_index,
        std::vector<std::uint8_t>& data) const;

    [[nodiscard]] std::uint64_t
    ExpectedBlockCountLocked(
        std::uint64_t chunk_size) const;

    storage::StorageManager& storage_manager_;

    mutable std::shared_mutex mutex_;

    std::unordered_map<
        ChunkHandle,
        std::vector<ChecksumBlock>> checksums_;
};

}  // namespace gfs::chunkserver::checksum