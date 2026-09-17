#pragma once

#include "gfs/common/types.hpp"

#include <cstdint>

namespace gfs::chunkserver::checksum {

class ChecksumBlock {
public:
    ChecksumBlock() = default;

    ChecksumBlock(
        ChunkHandle chunk_handle,
        std::uint64_t block_index,
        std::uint32_t checksum);

    [[nodiscard]] ChunkHandle
    GetChunkHandle() const noexcept;

    [[nodiscard]] std::uint64_t
    GetBlockIndex() const noexcept;

    [[nodiscard]] std::uint32_t
    GetChecksum() const noexcept;

    void SetChecksum(
        std::uint32_t checksum) noexcept;

    friend bool operator==(
        const ChecksumBlock& lhs,
        const ChecksumBlock& rhs) = default;

private:
    ChunkHandle chunk_handle_ = 0;
    std::uint64_t block_index_ = 0;
    std::uint32_t checksum_ = 0;
};

}  // namespace gfs::chunkserver::checksum