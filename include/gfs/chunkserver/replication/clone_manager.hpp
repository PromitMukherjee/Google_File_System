#pragma once

#include "gfs/common/types.hpp"

#include <cstdint>

namespace gfs::chunkserver {
class Chunkserver;
}

namespace gfs::chunkserver::replication {

class CloneManager {
public:
    explicit CloneManager(
        chunkserver::Chunkserver& chunkserver);

    CloneManager(const CloneManager&) = delete;
    CloneManager& operator=(const CloneManager&) = delete;

    [[nodiscard]] bool Clone(
        ChunkHandle source_handle,
        ChunkHandle destination_handle);

    [[nodiscard]] bool CloneRange(
        ChunkHandle source_handle,
        ChunkHandle destination_handle,
        std::uint64_t source_offset,
        std::uint64_t length);

    [[nodiscard]] bool CanClone(
        ChunkHandle source_handle,
        ChunkHandle destination_handle) const;

    [[nodiscard]] chunkserver::Chunkserver&
    GetChunkserver() noexcept;

private:
    chunkserver::Chunkserver& chunkserver_;
};

}  // namespace gfs::chunkserver::replication