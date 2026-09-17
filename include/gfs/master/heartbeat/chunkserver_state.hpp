#pragma once

#include "gfs/common/types.hpp"

#include <cstdint>
#include <unordered_map>
#include <vector>

namespace gfs::master::heartbeat {

struct ReportedChunk {
    ChunkHandle handle = 0;
    ChunkVersion version = 1;

    friend bool operator==(const ReportedChunk& lhs,
                           const ReportedChunk& rhs) = default;
};

class ChunkserverState {
public:
    explicit ChunkserverState(ServerId server_id = 0);

    [[nodiscard]] ServerId GetServerId() const noexcept;

    [[nodiscard]] std::uint64_t GetLastHeartbeatMs()
        const noexcept;

    [[nodiscard]] bool IsAlive() const noexcept;

    void UpdateHeartbeat(
        std::uint64_t timestamp_ms);

    void MarkFailed() noexcept;

    void SetReportedChunks(
        const std::vector<ReportedChunk>& chunks);

    [[nodiscard]] bool HasReportedChunk(
        ChunkHandle handle) const;

    [[nodiscard]] std::vector<ReportedChunk>
    GetReportedChunks() const;

    [[nodiscard]] std::size_t ReportedChunkCount()
        const noexcept;

private:
    ServerId server_id_ = 0;
    std::uint64_t last_heartbeat_ms_ = 0;
    bool alive_ = false;

    std::unordered_map<ChunkHandle, ChunkVersion>
        reported_chunks_;
};

}  // namespace gfs::master::heartbeat