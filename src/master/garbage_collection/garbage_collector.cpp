#include "gfs/master/garbage_collection/garbage_collector.hpp"

#include "gfs/master/master.hpp"

namespace gfs::master::garbage_collection {

GarbageCollector::GarbageCollector(
    Master& master,
    OrphanChunkManager& orphan_manager)
    : master_(master),
      orphan_manager_(orphan_manager) {
}

std::vector<ChunkHandle>
GarbageCollector::IdentifyGarbage() {
    return orphan_manager_.IdentifyOrphans(
        master_.GetMetadata());
}

std::vector<ChunkHandle>
GarbageCollector::Collect() {
    const auto candidates =
        IdentifyGarbage();

    std::vector<ChunkHandle> collected;

    for (const ChunkHandle handle :
         candidates) {
        if (CollectChunk(handle)) {
            collected.push_back(handle);
        }
    }

    return collected;
}

bool GarbageCollector::CollectChunk(
    ChunkHandle handle) {
    if (handle == 0) {
        return false;
    }

    if (master_.GetChunkReferenceCount(handle) != 0) {
        static_cast<void>(
            orphan_manager_.RemoveOrphan(handle));

        return false;
    }

    if (!master_.GarbageCollectChunk(handle)) {
        return false;
    }

    static_cast<void>(
        orphan_manager_.RemoveOrphan(handle));

    return true;
}

std::vector<ChunkHandle>
GarbageCollector::GetCandidates() const {
    return orphan_manager_.GetOrphanChunks();
}

std::size_t
GarbageCollector::CandidateCount()
    const noexcept {
    return orphan_manager_.Size();
}

void GarbageCollector::Clear() {
    orphan_manager_.Clear();
}

OrphanChunkManager&
GarbageCollector::GetOrphanChunkManager()
    noexcept {
    return orphan_manager_;
}

const OrphanChunkManager&
GarbageCollector::GetOrphanChunkManager()
    const noexcept {
    return orphan_manager_;
}

}  // namespace gfs::master::garbage_collection