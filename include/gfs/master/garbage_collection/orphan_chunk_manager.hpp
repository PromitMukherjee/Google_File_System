#pragma once

#include "gfs/common/types.hpp"
#include "gfs/master/metadata/metadata.hpp"

#include <cstddef>
#include <unordered_set>
#include <vector>

namespace gfs::master::garbage_collection {

class OrphanChunkManager {
public:
    OrphanChunkManager() = default;

    [[nodiscard]] bool MarkOrphan(
        ChunkHandle handle);

    [[nodiscard]] bool RemoveOrphan(
        ChunkHandle handle);

    [[nodiscard]] bool IsOrphan(
        ChunkHandle handle) const;

    [[nodiscard]] std::vector<ChunkHandle>
    GetOrphanChunks() const;

    [[nodiscard]] std::vector<ChunkHandle>
    IdentifyOrphans(
        const metadata::Metadata& metadata);

    [[nodiscard]] std::size_t
    Size() const noexcept;

    void Clear();

private:
    std::unordered_set<ChunkHandle>
        orphan_chunks_;
};

}  // namespace gfs::master::garbage_collection