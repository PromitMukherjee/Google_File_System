#pragma once

#include "gfs/common/types.hpp"
#include "gfs/master/garbage_collection/orphan_chunk_manager.hpp"

#include <cstddef>
#include <vector>

namespace gfs::master {

class Master;

namespace garbage_collection {

class GarbageCollector {
public:
    GarbageCollector(
        Master& master,
        OrphanChunkManager& orphan_manager);

    GarbageCollector(
        const GarbageCollector&) = delete;

    GarbageCollector& operator=(
        const GarbageCollector&) = delete;

    [[nodiscard]] std::vector<ChunkHandle>
    IdentifyGarbage();

    [[nodiscard]] std::vector<ChunkHandle>
    Collect();

    [[nodiscard]] bool
    CollectChunk(
        ChunkHandle handle);

    [[nodiscard]] std::vector<ChunkHandle>
    GetCandidates() const;

    [[nodiscard]] std::size_t
    CandidateCount() const noexcept;

    void Clear();

    [[nodiscard]] OrphanChunkManager&
    GetOrphanChunkManager() noexcept;

    [[nodiscard]] const OrphanChunkManager&
    GetOrphanChunkManager() const noexcept;

private:
    Master& master_;
    OrphanChunkManager& orphan_manager_;
};

}  // namespace garbage_collection
}  // namespace gfs::master