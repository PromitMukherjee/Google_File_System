#pragma once

#include "gfs/common/types.hpp"

#include <cstddef>
#include <cstdint>
#include <unordered_set>
#include <vector>

namespace gfs::master::metadata {

class ChunkMetadata {
public:
    ChunkMetadata() = default;

    explicit ChunkMetadata(
        ChunkHandle handle,
        ChunkVersion version = 1);

    ChunkHandle GetHandle() const noexcept;

    ChunkVersion GetVersion() const noexcept;
    void SetVersion(ChunkVersion version) noexcept;

    std::uint64_t GetSize() const noexcept;
    void SetSize(std::uint64_t size) noexcept;

    bool AddReplica(ServerId server_id);
    bool RemoveReplica(ServerId server_id);
    bool HasReplica(ServerId server_id) const;

    const std::unordered_set<ServerId>&
    GetReplicas() const noexcept;

    std::vector<ServerId> GetReplicaServerIds() const;

    std::size_t ReplicaCount() const noexcept;

    void ClearReplicas() noexcept;

private:
    ChunkHandle handle_ = 0;
    ChunkVersion version_ = 1;
    std::uint64_t size_ = 0;

    std::unordered_set<ServerId> replica_server_ids_;
};

}  // namespace gfs::master::metadata