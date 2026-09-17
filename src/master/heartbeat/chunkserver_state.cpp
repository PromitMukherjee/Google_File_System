#include "gfs/master/heartbeat/chunkserver_state.hpp"

#include <algorithm>

namespace gfs::master::heartbeat {

ChunkserverState::ChunkserverState(
    ServerId server_id)
    : server_id_(server_id) {
}

ServerId ChunkserverState::GetServerId()
    const noexcept {
    return server_id_;
}

std::uint64_t ChunkserverState::GetLastHeartbeatMs()
    const noexcept {
    return last_heartbeat_ms_;
}

bool ChunkserverState::IsAlive() const noexcept {
    return alive_;
}

void ChunkserverState::UpdateHeartbeat(
    std::uint64_t timestamp_ms) {
    last_heartbeat_ms_ = timestamp_ms;
    alive_ = true;
}

void ChunkserverState::MarkFailed() noexcept {
    alive_ = false;
}

void ChunkserverState::SetReportedChunks(
    const std::vector<ReportedChunk>& chunks) {
    reported_chunks_.clear();

    for (const auto& chunk : chunks) {
        if (chunk.handle == 0 ||
            chunk.version == 0) {
            continue;
        }

        reported_chunks_[chunk.handle] =
            chunk.version;
    }
}

bool ChunkserverState::HasReportedChunk(
    ChunkHandle handle) const {
    if (handle == 0) {
        return false;
    }

    return reported_chunks_.contains(handle);
}

std::vector<ReportedChunk>
ChunkserverState::GetReportedChunks() const {
    std::vector<ReportedChunk> result;
    result.reserve(reported_chunks_.size());

    for (const auto& [handle, version] :
         reported_chunks_) {
        result.push_back(
            ReportedChunk{handle, version});
    }

    std::sort(
        result.begin(),
        result.end(),
        [](const ReportedChunk& lhs,
           const ReportedChunk& rhs) {
            return lhs.handle < rhs.handle;
        });

    return result;
}

std::size_t ChunkserverState::ReportedChunkCount()
    const noexcept {
    return reported_chunks_.size();
}

}  // namespace gfs::master::heartbeat