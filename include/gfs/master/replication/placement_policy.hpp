#pragma once

#include "gfs/common/constants.hpp"
#include "gfs/common/types.hpp"

#include <cstddef>
#include <vector>

namespace gfs::master::replication {

struct PlacementCandidate {
    ServerId server_id = 0;
    std::size_t current_chunk_count = 0;

    friend bool operator==(const PlacementCandidate& lhs,
                           const PlacementCandidate& rhs) = default;
};

struct PlacementRequest {
    ChunkHandle handle = 0;
    std::size_t replication_factor =
        gfs::constants::kDefaultReplicationFactor;
    std::vector<PlacementCandidate> candidates;
};

class PlacementPolicy {
public:
    explicit PlacementPolicy(
        std::size_t replication_factor =
            gfs::constants::kDefaultReplicationFactor);

    [[nodiscard]] std::size_t GetReplicationFactor() const noexcept;

    void SetReplicationFactor(
        std::size_t replication_factor);

    [[nodiscard]] std::vector<ServerId> SelectReplicas(
        const PlacementRequest& request) const;

    [[nodiscard]] std::vector<ServerId> SelectReplicas(
        ChunkHandle handle,
        const std::vector<PlacementCandidate>& candidates) const;

    [[nodiscard]] bool IsValidPlacement(
        const std::vector<ServerId>& servers,
        std::size_t replication_factor) const;

private:
    std::size_t replication_factor_;
};

}  // namespace gfs::master::replication