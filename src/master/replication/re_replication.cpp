#include "gfs/master/replication/re_replication.hpp"

#include "gfs/chunkserver/chunkserver.hpp"
#include "gfs/master/master.hpp"

#include <algorithm>
#include <limits>
#include <mutex>
#include <unordered_set>
#include <utility>

namespace gfs::master::replication {

ReReplicationManager::ReReplicationManager(Master& master)
    : master_(master) {
}

bool ReReplicationManager::RegisterChunkserver(
    chunkserver::Chunkserver& chunkserver) {
    const ServerId server_id =
        chunkserver.GetServerId();

    if (server_id == 0) {
        return false;
    }

    std::unique_lock lock(mutex_);

    const auto [it, inserted] =
        chunkservers_.emplace(
            server_id,
            &chunkserver);

    if (!inserted) {
        return it->second == &chunkserver;
    }

    return true;
}

bool ReReplicationManager::UnregisterChunkserver(
    ServerId server_id) {
    if (server_id == 0) {
        return false;
    }

    std::unique_lock lock(mutex_);

    return chunkservers_.erase(server_id) > 0;
}

bool ReReplicationManager::HasChunkserver(
    ServerId server_id) const {
    if (server_id == 0) {
        return false;
    }

    std::shared_lock lock(mutex_);

    return chunkservers_.contains(server_id);
}

std::size_t
ReReplicationManager::RegisteredChunkserverCount() const {
    std::shared_lock lock(mutex_);

    return chunkservers_.size();
}

std::uint32_t
ReReplicationManager::GetDesiredReplicationFactor(
    ChunkHandle handle) const {
    const auto factor =
        master_.GetChunkReplicationFactor(handle);

    if (factor.has_value() &&
        *factor != 0) {
        return *factor;
    }

    const std::size_t configured =
        master_.GetPlacementPolicy()
            .GetReplicationFactor();

    if (configured == 0) {
        return 1;
    }

    if (configured >
        static_cast<std::size_t>(
            std::numeric_limits<std::uint32_t>::max())) {
        return std::numeric_limits<std::uint32_t>::max();
    }

    return static_cast<std::uint32_t>(configured);
}

bool ReReplicationManager::IsHealthyCurrentReplica(
    ChunkHandle handle,
    ServerId server_id,
    std::uint64_t now_ms) const {
    if (handle == 0 ||
        server_id == 0) {
        return false;
    }

    if (!master_.IsChunkserverAlive(
            server_id,
            now_ms)) {
        return false;
    }

    if (master_.IsStaleReplica(
            handle,
            server_id)) {
        return false;
    }

    auto* chunkserver =
        GetChunkserver(server_id);

    if (chunkserver == nullptr ||
        !chunkserver->ChunkExists(handle)) {
        return false;
    }

    return true;
}

std::vector<ServerId>
ReReplicationManager::GetHealthyCurrentReplicas(
    ChunkHandle handle,
    std::uint64_t now_ms) const {
    std::vector<ServerId> healthy;

    const auto replicas =
        master_.GetReplicaServers(handle);

    healthy.reserve(replicas.size());

    for (const ServerId server_id : replicas) {
        if (IsHealthyCurrentReplica(
                handle,
                server_id,
                now_ms)) {
            healthy.push_back(server_id);
        }
    }

    return healthy;
}

std::size_t
ReReplicationManager::GetHealthyReplicaCount(
    ChunkHandle handle,
    std::uint64_t now_ms) const {
    return GetHealthyCurrentReplicas(
               handle,
               now_ms)
        .size();
}

bool ReReplicationManager::NeedsReReplication(
    ChunkHandle handle,
    std::uint64_t now_ms) const {
    if (handle == 0 ||
        !master_.GetChunkInfo(handle).has_value()) {
        return false;
    }

    const std::size_t desired =
        static_cast<std::size_t>(
            GetDesiredReplicationFactor(handle));

    return GetHealthyReplicaCount(
               handle,
               now_ms) <
           desired;
}

std::optional<ServerId>
ReReplicationManager::FindRecoverySource(
    ChunkHandle handle,
    std::uint64_t now_ms) const {
    if (handle == 0) {
        return std::nullopt;
    }

    const auto replicas =
        master_.GetReplicaManager()
            .GetReplicas(handle);

    const auto primary =
        master_.GetPrimary(handle);

    if (primary.has_value() &&
        IsHealthyCurrentReplica(
            handle,
            *primary,
            now_ms)) {
        return primary;
    }

    for (const auto& replica : replicas) {
        if (IsHealthyCurrentReplica(
                handle,
                replica.server_id,
                now_ms)) {
            return replica.server_id;
        }
    }

    return std::nullopt;
}

std::vector<ServerId>
ReReplicationManager::GetRecoveryDestinations(
    ChunkHandle handle,
    std::uint64_t now_ms) const {
    std::vector<ServerId> destinations;

    if (!NeedsReReplication(
            handle,
            now_ms)) {
        return destinations;
    }

    const std::size_t desired =
        static_cast<std::size_t>(
            GetDesiredReplicationFactor(handle));

    const std::size_t healthy_count =
        GetHealthyReplicaCount(
            handle,
            now_ms);

    if (healthy_count >= desired) {
        return destinations;
    }

    const std::size_t needed =
        desired - healthy_count;

    const auto replicas =
        master_.GetReplicaServers(handle);

    const auto stale =
        master_.GetStaleReplicas(handle);

    std::unordered_set<ServerId> excluded(
        replicas.begin(),
        replicas.end());

    excluded.insert(
        stale.begin(),
        stale.end());

    std::vector<PlacementCandidate> candidates;

    const auto live_servers =
        master_.GetHeartbeatManager()
            .GetLiveServers();

    candidates.reserve(
        live_servers.size());

    for (const ServerId server_id :
         live_servers) {
        if (server_id == 0 ||
            excluded.contains(server_id)) {
            continue;
        }

        if (!master_.IsChunkserverAlive(
                server_id,
                now_ms)) {
            continue;
        }

        if (!HasChunkserver(server_id)) {
            continue;
        }

        candidates.push_back(
            PlacementCandidate{
                server_id,
                master_.GetReplicaManager()
                    .GetChunksForServer(server_id)
                    .size()});
    }

    if (candidates.empty()) {
        return destinations;
    }

    PlacementRequest request;

    request.handle = handle;
    request.replication_factor = needed;
    request.candidates = std::move(candidates);

    return master_.GetPlacementPolicy()
        .SelectReplicas(request);
}

bool ReReplicationManager::TransferReplica(
    ChunkHandle handle,
    ServerId source_server_id,
    ServerId destination_server_id) const {
    if (handle == 0 ||
        source_server_id == 0 ||
        destination_server_id == 0 ||
        source_server_id == destination_server_id) {
        return false;
    }

    auto* source =
        GetChunkserver(source_server_id);

    auto* destination =
        GetChunkserver(destination_server_id);

    if (source == nullptr ||
        destination == nullptr ||
        !source->ChunkExists(handle)) {
        return false;
    }

    return source->GetReplicaSender().Send(
        handle,
        destination_server_id,
        [destination](
            ServerId target_server_id,
            ChunkHandle target_handle,
            const std::string& data) {
            if (target_server_id !=
                destination->GetServerId()) {
                return false;
            }

            return destination
                ->GetReplicaReceiver()
                .Receive(
                    target_handle,
                    data);
        });
}

bool ReReplicationManager::RecoverChunk(
    ChunkHandle handle,
    std::uint64_t now_ms) {
    if (handle == 0 ||
        !master_.GetChunkInfo(handle).has_value()) {
        return false;
    }

    const auto stale =
        master_.GetStaleReplicas(handle);

    const auto replicas =
        master_.GetReplicaServers(handle);

    for (const ServerId server_id : replicas) {
        if (!master_.IsChunkserverAlive(
                server_id,
                now_ms)) {
            static_cast<void>(
                master_.RemoveReplica(
                    handle,
                    server_id));
        }
    }

    const auto healthy =
        GetHealthyCurrentReplicas(
            handle,
            now_ms);

    if (healthy.empty()) {
        for (const ServerId server_id : stale) {
            static_cast<void>(
                master_.RemoveReplica(
                    handle,
                    server_id));
        }

        return false;
    }

    const auto current_primary =
        master_.GetPrimary(handle);

    if (!current_primary.has_value() ||
        !IsHealthyCurrentReplica(
            handle,
            *current_primary,
            now_ms)) {
        if (!master_.SetPrimary(
                handle,
                healthy.front())) {
            return false;
        }
    }

    if (!NeedsReReplication(
            handle,
            now_ms)) {
        for (const ServerId server_id : stale) {
            static_cast<void>(
                master_.RemoveReplica(
                    handle,
                    server_id));
        }

        return true;
    }

    const auto source =
        FindRecoverySource(
            handle,
            now_ms);

    if (!source.has_value()) {
        for (const ServerId server_id : stale) {
            static_cast<void>(
                master_.RemoveReplica(
                    handle,
                    server_id));
        }

        return false;
    }

    const auto destinations =
        GetRecoveryDestinations(
            handle,
            now_ms);

    for (const ServerId server_id : stale) {
        static_cast<void>(
            master_.RemoveReplica(
                handle,
                server_id));
    }

    if (destinations.empty()) {
        return false;
    }

    bool recovered = false;

    for (const ServerId destination :
         destinations) {
        if (GetHealthyReplicaCount(
                handle,
                now_ms) >=
            static_cast<std::size_t>(
                GetDesiredReplicationFactor(handle))) {
            break;
        }

        if (master_.HasReplica(
                handle,
                destination)) {
            continue;
        }

        if (!TransferReplica(
                handle,
                *source,
                destination)) {
            continue;
        }

        if (!master_.RegisterReplica(
                handle,
                destination,
                false)) {
            continue;
        }

        recovered = true;
    }

    return recovered ||
           !NeedsReReplication(
               handle,
               now_ms);
}

std::vector<ChunkHandle>
ReReplicationManager::RecoverFailedChunkservers(
    std::uint64_t now_ms) {
    static_cast<void>(
        master_.DetectFailedChunkservers(now_ms));

    std::unordered_set<ChunkHandle> affected_set;

    const auto failed =
        master_.GetHeartbeatManager()
            .GetFailedServers();

    for (const ServerId server_id :
         failed) {
        const auto chunks =
            master_.GetReplicaManager()
                .GetChunksForServer(server_id);

        affected_set.insert(
            chunks.begin(),
            chunks.end());
    }

    std::vector<ChunkHandle> affected(
        affected_set.begin(),
        affected_set.end());

    std::sort(
        affected.begin(),
        affected.end());

    std::vector<ChunkHandle> recovered;

    for (const ChunkHandle handle :
         affected) {
        if (RecoverChunk(
                handle,
                now_ms)) {
            recovered.push_back(handle);
        }
    }

    return recovered;
}

std::vector<ChunkHandle>
ReReplicationManager::RecoverAll(
    std::uint64_t now_ms) {
    static_cast<void>(
        master_.DetectFailedChunkservers(now_ms));

    std::unordered_set<ChunkHandle> candidates;

    for (const ChunkHandle handle :
         master_.GetReplicaManager()
             .GetAllChunks()) {
        if (NeedsReReplication(
                handle,
                now_ms) ||
            !master_.GetStaleReplicas(handle)
                 .empty()) {
            candidates.insert(handle);
        }
    }

    for (const ServerId server_id :
         master_.GetHeartbeatManager()
             .GetFailedServers()) {
        const auto chunks =
            master_.GetReplicaManager()
                .GetChunksForServer(server_id);

        candidates.insert(
            chunks.begin(),
            chunks.end());
    }

    std::vector<ChunkHandle> ordered(
        candidates.begin(),
        candidates.end());

    std::sort(
        ordered.begin(),
        ordered.end());

    std::vector<ChunkHandle> recovered;

    for (const ChunkHandle handle :
         ordered) {
        if (RecoverChunk(
                handle,
                now_ms)) {
            recovered.push_back(handle);
        }
    }

    return recovered;
}

chunkserver::Chunkserver*
ReReplicationManager::GetChunkserver(
    ServerId server_id) const {
    std::shared_lock lock(mutex_);

    const auto it =
        chunkservers_.find(server_id);

    if (it == chunkservers_.end()) {
        return nullptr;
    }

    return it->second;
}

}  // namespace gfs::master::replication