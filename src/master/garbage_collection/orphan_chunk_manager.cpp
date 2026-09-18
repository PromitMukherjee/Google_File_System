#include "gfs/master/garbage_collection/orphan_chunk_manager.hpp"

#include <algorithm>

namespace gfs::master::garbage_collection {

bool OrphanChunkManager::MarkOrphan(
    ChunkHandle handle) {
    if (handle == 0) {
        return false;
    }

    return orphan_chunks_.insert(handle).second;
}

bool OrphanChunkManager::RemoveOrphan(
    ChunkHandle handle) {
    if (handle == 0) {
        return false;
    }

    return orphan_chunks_.erase(handle) != 0;
}

bool OrphanChunkManager::IsOrphan(
    ChunkHandle handle) const {
    return handle != 0 &&
           orphan_chunks_.contains(handle);
}

std::vector<ChunkHandle>
OrphanChunkManager::GetOrphanChunks() const {
    std::vector<ChunkHandle> result(
        orphan_chunks_.begin(),
        orphan_chunks_.end());

    std::sort(
        result.begin(),
        result.end());

    return result;
}

std::vector<ChunkHandle>
OrphanChunkManager::IdentifyOrphans(
    const metadata::Metadata& metadata) {
    for (const auto& chunk :
         metadata.ExportChunks()) {
        if (metadata.GetChunkReferenceCount(
                chunk.handle) == 0) {
            static_cast<void>(
                MarkOrphan(chunk.handle));
        } else {
            static_cast<void>(
                RemoveOrphan(chunk.handle));
        }
    }

    return GetOrphanChunks();
}

std::size_t
OrphanChunkManager::Size() const noexcept {
    return orphan_chunks_.size();
}

void OrphanChunkManager::Clear() {
    orphan_chunks_.clear();
}

}  // namespace gfs::master::garbage_collection