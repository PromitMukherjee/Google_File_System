#include "gfs/chunkserver/checksum/checksum_block.hpp"

namespace gfs::chunkserver::checksum {

ChecksumBlock::ChecksumBlock(
    ChunkHandle chunk_handle,
    std::uint64_t block_index,
    std::uint32_t checksum)
    : chunk_handle_(chunk_handle),
      block_index_(block_index),
      checksum_(checksum) {
}

ChunkHandle ChecksumBlock::GetChunkHandle() const noexcept {
    return chunk_handle_;
}

std::uint64_t ChecksumBlock::GetBlockIndex() const noexcept {
    return block_index_;
}

std::uint32_t ChecksumBlock::GetChecksum() const noexcept {
    return checksum_;
}

void ChecksumBlock::SetChecksum(
    std::uint32_t checksum) noexcept {
    checksum_ = checksum;
}

}  // namespace gfs::chunkserver::checksum