#include "gfs/master/heartbeat/heartbeat_manager.hpp"

#include <algorithm>
#include <limits>
#include <mutex>

namespace gfs::master::heartbeat {

HeartbeatManager::HeartbeatManager(
    std::uint64_t failure_timeout_ms)
    : failure_timeout_ms_(failure_timeout_ms) {
}

std::uint64_t HeartbeatManager::GetFailureTimeoutMs()
    const noexcept {
    std::shared_lock lock(mutex_);
    return failure_timeout_ms_;
}

void HeartbeatManager::SetFailureTimeoutMs(
    std::uint64_t failure_timeout_ms) {
    std::unique_lock lock(mutex_);
    failure_timeout_ms_ = failure_timeout_ms;
}

bool HeartbeatManager::ProcessHeartbeat(
    ServerId server_id,
    std::uint64_t timestamp_ms,
    const std::vector<ReportedChunk>& chunks) {
    if (server_id == 0) {
        return false;
    }

    std::unique_lock lock(mutex_);

    auto [it, inserted] =
        servers_.try_emplace(
            server_id,
            ChunkserverState(server_id));

    ChunkserverState& state = it->second;

    if (!inserted &&
        state.GetLastHeartbeatMs() > timestamp_ms) {
        return false;
    }

    state.UpdateHeartbeat(timestamp_ms);
    state.SetReportedChunks(chunks);

    return true;
}

bool HeartbeatManager::ProcessHeartbeat(
    ServerId server_id,
    std::uint64_t timestamp_ms) {
    return ProcessHeartbeat(
        server_id,
        timestamp_ms,
        {});
}

bool HeartbeatManager::HasServer(
    ServerId server_id) const {
    if (server_id == 0) {
        return false;
    }

    std::shared_lock lock(mutex_);
    return servers_.contains(server_id);
}

bool HeartbeatManager::IsServerAlive(
    ServerId server_id) const {
    if (server_id == 0) {
        return false;
    }

    std::shared_lock lock(mutex_);

    const auto it = servers_.find(server_id);

    if (it == servers_.end()) {
        return false;
    }

    return it->second.IsAlive();
}

bool HeartbeatManager::IsServerAlive(
    ServerId server_id,
    std::uint64_t now_ms) const {
    if (server_id == 0) {
        return false;
    }

    std::shared_lock lock(mutex_);

    const auto it = servers_.find(server_id);

    if (it == servers_.end() ||
        !it->second.IsAlive()) {
        return false;
    }

    const std::uint64_t last =
        it->second.GetLastHeartbeatMs();

    if (now_ms < last) {
        return false;
    }

    if (failure_timeout_ms_ == 0) {
        return false;
    }

    return now_ms - last <
           failure_timeout_ms_;
}

std::optional<std::uint64_t>
HeartbeatManager::GetLastHeartbeat(
    ServerId server_id) const {
    if (server_id == 0) {
        return std::nullopt;
    }

    std::shared_lock lock(mutex_);

    const auto it = servers_.find(server_id);

    if (it == servers_.end()) {
        return std::nullopt;
    }

    return it->second.GetLastHeartbeatMs();
}

std::optional<ChunkserverState>
HeartbeatManager::GetServerState(
    ServerId server_id) const {
    if (server_id == 0) {
        return std::nullopt;
    }

    std::shared_lock lock(mutex_);

    const auto it = servers_.find(server_id);

    if (it == servers_.end()) {
        return std::nullopt;
    }

    return it->second;
}

std::vector<ServerId>
HeartbeatManager::GetLiveServers() const {
    std::vector<ServerId> result;

    std::shared_lock lock(mutex_);

    for (const auto& [server_id, state] : servers_) {
        if (state.IsAlive()) {
            result.push_back(server_id);
        }
    }

    std::sort(
        result.begin(),
        result.end());

    return result;
}

std::vector<ServerId>
HeartbeatManager::GetFailedServers() const {
    std::vector<ServerId> result;

    std::shared_lock lock(mutex_);

    for (const auto& [server_id, state] : servers_) {
        if (!state.IsAlive()) {
            result.push_back(server_id);
        }
    }

    std::sort(
        result.begin(),
        result.end());

    return result;
}

std::vector<ServerId>
HeartbeatManager::DetectFailedServers(
    std::uint64_t now_ms) {
    std::vector<ServerId> failed;

    std::unique_lock lock(mutex_);

    for (auto& [server_id, state] : servers_) {
        if (!state.IsAlive()) {
            continue;
        }

        const std::uint64_t last =
            state.GetLastHeartbeatMs();

        if (now_ms < last) {
            continue;
        }

        const bool timed_out =
            failure_timeout_ms_ == 0 ||
            now_ms - last >= failure_timeout_ms_;

        if (timed_out) {
            state.MarkFailed();
            failed.push_back(server_id);
        }
    }

    std::sort(
        failed.begin(),
        failed.end());

    return failed;
}

std::size_t HeartbeatManager::ServerCount() const {
    std::shared_lock lock(mutex_);
    return servers_.size();
}

void HeartbeatManager::Clear() {
    std::unique_lock lock(mutex_);
    servers_.clear();
}

}  // namespace gfs::master::heartbeat