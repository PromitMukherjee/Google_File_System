#pragma once

#include "gfs/common/types.hpp"

#include <cstddef>
#include <optional>
#include <shared_mutex>
#include <unordered_map>
#include <unordered_set>
#include <vector>

namespace gfs::master::replication {

struct ReplicaInfo {
    ServerId server_id = 0;
    bool is_primary = false;

    friend bool operator==(const ReplicaInfo& lhs,
                           const ReplicaInfo& rhs) = default;
};

struct ChunkReplicaSet {
    ChunkHandle handle = 0;
    std::vector<ReplicaInfo> replicas;

    [[nodiscard]] std::size_t Size() const noexcept {
        return replicas.size();
    }

    [[nodiscard]] bool Empty() const noexcept {
        return replicas.empty();
    }
};

class ReplicaManager {
public:
    ReplicaManager() = default;

    ReplicaManager(const ReplicaManager&) = delete;
    ReplicaManager& operator=(const ReplicaManager&) = delete;

    [[nodiscard]] bool RegisterReplica(
        ChunkHandle handle,
        ServerId server_id,
        bool is_primary = false);

    [[nodiscard]] bool RemoveReplica(
        ChunkHandle handle,
        ServerId server_id);

    [[nodiscard]] bool HasReplica(
        ChunkHandle handle,
        ServerId server_id) const;

    [[nodiscard]] bool HasChunk(
        ChunkHandle handle) const;

    [[nodiscard]] std::vector<ServerId> GetReplicaServers(
        ChunkHandle handle) const;

    [[nodiscard]] std::vector<ReplicaInfo> GetReplicas(
        ChunkHandle handle) const;

    [[nodiscard]] std::optional<ServerId> GetPrimary(
        ChunkHandle handle) const;

    [[nodiscard]] bool SetPrimary(
        ChunkHandle handle,
        ServerId server_id);

    [[nodiscard]] std::size_t ReplicaCount(
        ChunkHandle handle) const;

    [[nodiscard]] std::size_t ChunkCount() const;

    [[nodiscard]] bool RemoveChunk(
        ChunkHandle handle);

    [[nodiscard]] std::vector<ChunkHandle> GetChunksForServer(
        ServerId server_id) const;

    [[nodiscard]] std::vector<ChunkHandle> GetAllChunks() const;

    [[nodiscard]] ChunkReplicaSet GetReplicaSet(
        ChunkHandle handle) const;

    void Clear();

private:
    mutable std::shared_mutex mutex_;

    std::unordered_map<
        ChunkHandle,
        std::vector<ReplicaInfo>> replicas_;
};

}  // namespace gfs::master::replication