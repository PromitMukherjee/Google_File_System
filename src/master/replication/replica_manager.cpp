#include "gfs/master/replication/replica_manager.hpp"

#include <algorithm>
#include <mutex>

namespace gfs::master::replication {

bool ReplicaManager::RegisterReplica(
    ChunkHandle handle,
    ServerId server_id,
    bool is_primary) {
    if (handle == 0 || server_id == 0) {
        return false;
    }

    std::unique_lock lock(mutex_);

    auto& replicas = replicas_[handle];

    const auto existing = std::find_if(
        replicas.begin(),
        replicas.end(),
        [server_id](const ReplicaInfo& replica) {
            return replica.server_id == server_id;
        });

    if (existing != replicas.end()) {
        if (is_primary) {
            for (auto& replica : replicas) {
                replica.is_primary = false;
            }

            existing->is_primary = true;
        }

        return false;
    }

    if (is_primary) {
        for (auto& replica : replicas) {
            replica.is_primary = false;
        }
    }

    replicas.push_back(
        ReplicaInfo{server_id, is_primary});

    return true;
}

bool ReplicaManager::RemoveReplica(
    ChunkHandle handle,
    ServerId server_id) {
    if (handle == 0 || server_id == 0) {
        return false;
    }

    std::unique_lock lock(mutex_);

    const auto chunk_it = replicas_.find(handle);

    if (chunk_it == replicas_.end()) {
        return false;
    }

    auto& replicas = chunk_it->second;

    const auto it = std::find_if(
        replicas.begin(),
        replicas.end(),
        [server_id](const ReplicaInfo& replica) {
            return replica.server_id == server_id;
        });

    if (it == replicas.end()) {
        return false;
    }

    replicas.erase(it);

    if (replicas.empty()) {
        replicas_.erase(chunk_it);
    } else if (!std::any_of(
                   replicas.begin(),
                   replicas.end(),
                   [](const ReplicaInfo& replica) {
                       return replica.is_primary;
                   })) {
        replicas.front().is_primary = true;
    }

    return true;
}

bool ReplicaManager::HasReplica(
    ChunkHandle handle,
    ServerId server_id) const {
    if (handle == 0 || server_id == 0) {
        return false;
    }

    std::shared_lock lock(mutex_);

    const auto it = replicas_.find(handle);

    if (it == replicas_.end()) {
        return false;
    }

    return std::any_of(
        it->second.begin(),
        it->second.end(),
        [server_id](const ReplicaInfo& replica) {
            return replica.server_id == server_id;
        });
}

bool ReplicaManager::HasChunk(
    ChunkHandle handle) const {
    if (handle == 0) {
        return false;
    }

    std::shared_lock lock(mutex_);
    return replicas_.contains(handle);
}

std::vector<ServerId> ReplicaManager::GetReplicaServers(
    ChunkHandle handle) const {
    std::vector<ServerId> result;

    std::shared_lock lock(mutex_);

    const auto it = replicas_.find(handle);

    if (it == replicas_.end()) {
        return result;
    }

    result.reserve(it->second.size());

    for (const auto& replica : it->second) {
        result.push_back(replica.server_id);
    }

    return result;
}

std::vector<ReplicaInfo> ReplicaManager::GetReplicas(
    ChunkHandle handle) const {
    std::shared_lock lock(mutex_);

    const auto it = replicas_.find(handle);

    if (it == replicas_.end()) {
        return {};
    }

    return it->second;
}

std::optional<ServerId> ReplicaManager::GetPrimary(
    ChunkHandle handle) const {
    std::shared_lock lock(mutex_);

    const auto it = replicas_.find(handle);

    if (it == replicas_.end()) {
        return std::nullopt;
    }

    const auto primary = std::find_if(
        it->second.begin(),
        it->second.end(),
        [](const ReplicaInfo& replica) {
            return replica.is_primary;
        });

    if (primary == it->second.end()) {
        return std::nullopt;
    }

    return primary->server_id;
}

bool ReplicaManager::SetPrimary(
    ChunkHandle handle,
    ServerId server_id) {
    if (handle == 0 || server_id == 0) {
        return false;
    }

    std::unique_lock lock(mutex_);

    const auto it = replicas_.find(handle);

    if (it == replicas_.end()) {
        return false;
    }

    bool found = false;

    for (auto& replica : it->second) {
        if (replica.server_id == server_id) {
            replica.is_primary = true;
            found = true;
        } else {
            replica.is_primary = false;
        }
    }

    return found;
}

std::size_t ReplicaManager::ReplicaCount(
    ChunkHandle handle) const {
    std::shared_lock lock(mutex_);

    const auto it = replicas_.find(handle);

    if (it == replicas_.end()) {
        return 0;
    }

    return it->second.size();
}

std::size_t ReplicaManager::ChunkCount() const {
    std::shared_lock lock(mutex_);
    return replicas_.size();
}

bool ReplicaManager::RemoveChunk(
    ChunkHandle handle) {
    if (handle == 0) {
        return false;
    }

    std::unique_lock lock(mutex_);
    return replicas_.erase(handle) > 0;
}

std::vector<ChunkHandle> ReplicaManager::GetChunksForServer(
    ServerId server_id) const {
    if (server_id == 0) {
        return {};
    }

    std::vector<ChunkHandle> result;

    std::shared_lock lock(mutex_);

    for (const auto& [handle, replicas] : replicas_) {
        const auto found = std::any_of(
            replicas.begin(),
            replicas.end(),
            [server_id](const ReplicaInfo& replica) {
                return replica.server_id == server_id;
            });

        if (found) {
            result.push_back(handle);
        }
    }

    std::sort(result.begin(), result.end());

    return result;
}

std::vector<ChunkHandle> ReplicaManager::GetAllChunks() const {
    std::vector<ChunkHandle> result;

    std::shared_lock lock(mutex_);

    result.reserve(replicas_.size());

    for (const auto& [handle, replicas] : replicas_) {
        if (!replicas.empty()) {
            result.push_back(handle);
        }
    }

    std::sort(result.begin(), result.end());

    return result;
}

ChunkReplicaSet ReplicaManager::GetReplicaSet(
    ChunkHandle handle) const {
    ChunkReplicaSet result;
    result.handle = handle;
    result.replicas = GetReplicas(handle);
    return result;
}

void ReplicaManager::Clear() {
    std::unique_lock lock(mutex_);
    replicas_.clear();
}

}  // namespace gfs::master::replication