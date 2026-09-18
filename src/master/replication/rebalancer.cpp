#include "gfs/master/replication/rebalancer.hpp"

#include "gfs/master/master.hpp"

#include <algorithm>
#include <cstddef>
#include <limits>
#include <unordered_set>
#include <vector>

namespace gfs::master::replication {

Rebalancer::Rebalancer(
    Master& master)
    : master_(master) {
}

bool Rebalancer::IsEligibleSource(
    ChunkHandle handle,
    ServerId server_id,
    std::uint64_t now_ms) const {
    return handle != 0 &&
           server_id != 0 &&
           master_.IsChunkserverAlive(
               server_id,
               now_ms) &&
           master_.HasReplica(
               handle,
               server_id) &&
           !master_.IsStaleReplica(
               handle,
               server_id) &&
           master_.GetReReplicationManager()
               .HasChunkserver(server_id);
}

bool Rebalancer::IsEligibleDestination(
    ChunkHandle handle,
    ServerId server_id,
    std::uint64_t now_ms) const {
    if (handle == 0 ||
        server_id == 0 ||
        !master_.IsChunkserverAlive(
            server_id,
            now_ms) ||
        !master_.GetReReplicationManager()
            .HasChunkserver(server_id)) {
        return false;
    }

    return !master_.HasReplica(
        handle,
        server_id);
}

std::vector<ReplicaMove>
Rebalancer::Plan(
    std::uint64_t now_ms) const {
    const auto live_servers =
        master_.GetHeartbeatManager()
            .GetLiveServers();

    if (live_servers.size() < 2) {
        return {};
    }

    std::vector<ServerId>
        healthy_servers;

    for (const ServerId server_id :
         live_servers) {
        if (master_.GetReReplicationManager()
                .HasChunkserver(server_id) &&
            master_.IsChunkserverAlive(
                server_id,
                now_ms)) {
            healthy_servers.push_back(
                server_id);
        }
    }

    std::sort(
        healthy_servers.begin(),
        healthy_servers.end());

    if (healthy_servers.size() < 2) {
        return {};
    }

    std::vector<ReplicaMove> moves;
    std::unordered_set<ChunkHandle>
        moved_chunks;

    for (;;) {
        ServerId source = 0;
        ServerId destination = 0;

        std::size_t source_count = 0;
        std::size_t destination_count =
            std::numeric_limits<std::size_t>::max();

        for (const ServerId server_id :
             healthy_servers) {
            const std::size_t count =
                master_.GetReplicaManager()
                    .GetChunksForServer(server_id)
                    .size();

            if (count > source_count ||
                (count == source_count &&
                 (source == 0 ||
                  server_id < source))) {
                source = server_id;
                source_count = count;
            }

            if (count < destination_count ||
                (count == destination_count &&
                 (destination == 0 ||
                  server_id < destination))) {
                destination = server_id;
                destination_count = count;
            }
        }

        if (source == 0 ||
            destination == 0 ||
            source == destination ||
            source_count <=
                destination_count + 1) {
            break;
        }

        const auto source_chunks =
            master_.GetReplicaManager()
                .GetChunksForServer(source);

        bool moved = false;

        for (const ChunkHandle handle :
             source_chunks) {
            if (moved_chunks.contains(handle) ||
                !IsEligibleSource(
                    handle,
                    source,
                    now_ms) ||
                !IsEligibleDestination(
                    handle,
                    destination,
                    now_ms)) {
                continue;
            }

            const auto primary =
                master_.GetPrimary(handle);

            if (primary.has_value() &&
                *primary == source &&
                master_.GetReplicaManager()
                        .ReplicaCount(handle) >
                    1) {
                continue;
            }

            moves.push_back(
                ReplicaMove{
                    handle,
                    source,
                    destination});

            moved_chunks.insert(handle);

            --source_count;
            ++destination_count;

            moved = true;
            break;
        }

        if (!moved) {
            break;
        }
    }

    std::sort(
        moves.begin(),
        moves.end(),
        [](const ReplicaMove& lhs,
           const ReplicaMove& rhs) {
            if (lhs.handle != rhs.handle) {
                return lhs.handle <
                       rhs.handle;
            }

            if (lhs.source_server_id !=
                rhs.source_server_id) {
                return lhs.source_server_id <
                       rhs.source_server_id;
            }

            return lhs.destination_server_id <
                   rhs.destination_server_id;
        });

    return moves;
}

std::vector<ReplicaMove>
Rebalancer::FindMoves(
    std::uint64_t now_ms) const {
    return Plan(now_ms);
}

bool Rebalancer::ExecuteMove(
    const ReplicaMove& move,
    std::uint64_t now_ms) {
    if (!IsEligibleSource(
            move.handle,
            move.source_server_id,
            now_ms) ||
        !IsEligibleDestination(
            move.handle,
            move.destination_server_id,
            now_ms)) {
        return false;
    }

    const auto primary =
        master_.GetPrimary(move.handle);

    const bool moving_primary =
        primary.has_value() &&
        *primary ==
            move.source_server_id;

    if (moving_primary &&
        master_.GetReplicaManager()
                .ReplicaCount(move.handle) >
            1) {
        return false;
    }

    if (!master_.GetReReplicationManager()
            .TransferReplica(
                move.handle,
                move.source_server_id,
                move.destination_server_id)) {
        return false;
    }

    if (!master_.RegisterReplica(
            move.handle,
            move.destination_server_id,
            moving_primary)) {
        return false;
    }

    if (!master_.RemoveReplica(
            move.handle,
            move.source_server_id)) {
        static_cast<void>(
            master_.RemoveReplica(
                move.handle,
                move.destination_server_id));

        return false;
    }

    if (moving_primary) {
        return master_.SetPrimary(
            move.handle,
            move.destination_server_id);
    }

    return true;
}

std::vector<ReplicaMove>
Rebalancer::Rebalance(
    std::uint64_t now_ms) {
    const auto planned =
        Plan(now_ms);

    std::vector<ReplicaMove>
        completed;

    for (const auto& move :
         planned) {
        if (ExecuteMove(
                move,
                now_ms)) {
            completed.push_back(move);
        }
    }

    return completed;
}

}  // namespace gfs::master::replication