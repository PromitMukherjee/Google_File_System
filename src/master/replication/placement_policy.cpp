#include "gfs/master/replication/placement_policy.hpp"

#include <algorithm>
#include <unordered_set>

namespace gfs::master::replication {

PlacementPolicy::PlacementPolicy(
    std::size_t replication_factor)
    : replication_factor_(
          replication_factor == 0
              ? gfs::constants::kDefaultReplicationFactor
              : replication_factor) {
}

std::size_t PlacementPolicy::GetReplicationFactor() const noexcept {
    return replication_factor_;
}

void PlacementPolicy::SetReplicationFactor(
    std::size_t replication_factor) {
    if (replication_factor == 0) {
        replication_factor =
            gfs::constants::kDefaultReplicationFactor;
    }

    replication_factor_ = replication_factor;
}

std::vector<ServerId> PlacementPolicy::SelectReplicas(
    const PlacementRequest& request) const {
    const std::size_t factor =
        request.replication_factor == 0
            ? replication_factor_
            : request.replication_factor;

    if (factor == 0) {
        return {};
    }

    std::vector<PlacementCandidate> candidates =
        request.candidates;

    std::sort(
        candidates.begin(),
        candidates.end(),
        [](const PlacementCandidate& lhs,
           const PlacementCandidate& rhs) {
            if (lhs.current_chunk_count !=
                rhs.current_chunk_count) {
                return lhs.current_chunk_count <
                       rhs.current_chunk_count;
            }

            return lhs.server_id < rhs.server_id;
        });

    std::vector<ServerId> selected;
    std::unordered_set<ServerId> seen;

    selected.reserve(
        std::min(factor, candidates.size()));

    for (const auto& candidate : candidates) {
        if (candidate.server_id == 0) {
            continue;
        }

        if (!seen.insert(candidate.server_id).second) {
            continue;
        }

        selected.push_back(candidate.server_id);

        if (selected.size() == factor) {
            break;
        }
    }

    if (!IsValidPlacement(selected, factor)) {
        return {};
    }

    return selected;
}

std::vector<ServerId> PlacementPolicy::SelectReplicas(
    ChunkHandle handle,
    const std::vector<PlacementCandidate>& candidates) const {
    PlacementRequest request;
    request.handle = handle;
    request.replication_factor = replication_factor_;
    request.candidates = candidates;

    return SelectReplicas(request);
}

bool PlacementPolicy::IsValidPlacement(
    const std::vector<ServerId>& servers,
    std::size_t replication_factor) const {
    if (replication_factor == 0 ||
        servers.size() != replication_factor) {
        return false;
    }

    std::unordered_set<ServerId> unique_servers;

    for (const ServerId server_id : servers) {
        if (server_id == 0 ||
            !unique_servers.insert(server_id).second) {
            return false;
        }
    }

    return true;
}

}  // namespace gfs::master::replication