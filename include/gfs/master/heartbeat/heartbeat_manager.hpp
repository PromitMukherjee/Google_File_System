#pragma once

#include "gfs/common/types.hpp"
#include "gfs/master/heartbeat/chunkserver_state.hpp"

#include <cstddef>
#include <cstdint>
#include <optional>
#include <shared_mutex>
#include <unordered_map>
#include <vector>

namespace gfs::master::heartbeat {

class HeartbeatManager {
public:
    explicit HeartbeatManager(
        std::uint64_t failure_timeout_ms = 30000);

    HeartbeatManager(const HeartbeatManager&) = delete;
    HeartbeatManager& operator=(const HeartbeatManager&) = delete;

    [[nodiscard]] std::uint64_t
    GetFailureTimeoutMs() const noexcept;

    void SetFailureTimeoutMs(
        std::uint64_t failure_timeout_ms);

    [[nodiscard]] bool ProcessHeartbeat(
        ServerId server_id,
        std::uint64_t timestamp_ms,
        const std::vector<ReportedChunk>& chunks);

    [[nodiscard]] bool ProcessHeartbeat(
        ServerId server_id,
        std::uint64_t timestamp_ms);

    [[nodiscard]] bool HasServer(
        ServerId server_id) const;

    [[nodiscard]] bool IsServerAlive(
        ServerId server_id) const;

    [[nodiscard]] bool IsServerAlive(
        ServerId server_id,
        std::uint64_t now_ms) const;

    [[nodiscard]] std::optional<std::uint64_t>
    GetLastHeartbeat(
        ServerId server_id) const;

    [[nodiscard]] std::optional<ChunkserverState>
    GetServerState(
        ServerId server_id) const;

    [[nodiscard]] std::vector<ServerId>
    GetLiveServers() const;

    [[nodiscard]] std::vector<ServerId>
    GetFailedServers() const;

    [[nodiscard]] std::vector<ServerId>
    DetectFailedServers(
        std::uint64_t now_ms);

    [[nodiscard]] std::size_t ServerCount() const;

    void Clear();

private:
    mutable std::shared_mutex mutex_;

    std::unordered_map<ServerId, ChunkserverState>
        servers_;

    std::uint64_t failure_timeout_ms_;
};

}  // namespace gfs::master::heartbeat