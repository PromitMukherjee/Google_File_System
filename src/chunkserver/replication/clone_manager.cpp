#include "gfs/chunkserver/replication/clone_manager.hpp"

#include "gfs/chunkserver/chunkserver.hpp"
#include "gfs/common/constants.hpp"

#include <algorithm>
#include <cstddef>
#include <string>
#include <vector>

namespace gfs::chunkserver::replication {

CloneManager::CloneManager(
    chunkserver::Chunkserver& chunkserver)
    : chunkserver_(chunkserver) {}

bool CloneManager::Clone(
    ChunkHandle source_handle,
    ChunkHandle destination_handle) {
    if (!CanClone(
            source_handle,
            destination_handle)) {
        return false;
    }

    const std::uint64_t size =
        chunkserver_.GetChunkSize(source_handle);

    std::vector<std::uint8_t> data;

    if (!chunkserver_.ReadChunk(
            source_handle,
            0,
            static_cast<std::size_t>(size),
            data)) {
        return false;
    }

    if (!chunkserver_.CreateChunk(destination_handle)) {
        return false;
    }

    if (!data.empty() &&
        !chunkserver_.WriteChunk(
            destination_handle,
            0,
            data)) {
        chunkserver_.DeleteChunk(destination_handle);
        return false;
    }

    return true;
}

bool CloneManager::CloneRange(
    ChunkHandle source_handle,
    ChunkHandle destination_handle,
    std::uint64_t source_offset,
    std::uint64_t length) {
    if (!CanClone(
            source_handle,
            destination_handle)) {
        return false;
    }

    const std::uint64_t source_size =
        chunkserver_.GetChunkSize(source_handle);

    if (source_offset > source_size ||
        length > source_size - source_offset ||
        source_offset + length >
            gfs::constants::kChunkSize) {
        return false;
    }

    std::vector<std::uint8_t> data;

    if (!chunkserver_.ReadChunk(
            source_handle,
            source_offset,
            static_cast<std::size_t>(length),
            data)) {
        return false;
    }

    if (!chunkserver_.CreateChunk(destination_handle)) {
        return false;
    }

    if (!data.empty() &&
        !chunkserver_.WriteChunk(
            destination_handle,
            0,
            data)) {
        chunkserver_.DeleteChunk(destination_handle);
        return false;
    }

    return true;
}

bool CloneManager::CanClone(
    ChunkHandle source_handle,
    ChunkHandle destination_handle) const {
    if (source_handle == 0 ||
        destination_handle == 0 ||
        source_handle == destination_handle) {
        return false;
    }

    if (!chunkserver_.ChunkExists(source_handle)) {
        return false;
    }

    if (chunkserver_.ChunkExists(destination_handle)) {
        return false;
    }

    return chunkserver_.GetChunkSize(source_handle) <=
           gfs::constants::kChunkSize;
}

chunkserver::Chunkserver&
CloneManager::GetChunkserver() noexcept {
    return chunkserver_;
}

}  // namespace gfs::chunkserver::replication