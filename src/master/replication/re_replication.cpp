#include "gfs/master/replication/re_replication.hpp"

#include "gfs/chunkserver/chunkserver.hpp"
#include "gfs/master/master.hpp"

#include <algorithm>
#include <chrono>
#include <grpcpp/grpcpp.h>
#include <limits>
#include <mutex>
#include <unordered_set>
#include <utility>
#include <vector>

#include "chunkserver.grpc.pb.h"

namespace gfs::master::replication {

ReReplicationManager::ReReplicationManager(
    Master& master)
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

    return chunkservers_.erase(
        server_id) > 0;
}

bool ReReplicationManager::HasChunkserver(
    ServerId server_id) const {
    if (server_id == 0) {
        return false;
    }

    std::shared_lock lock(mutex_);

    return chunkservers_.contains(server_id) ||
           endpoints_.contains(server_id);
}

bool ReReplicationManager::RegisterChunkserverEndpoint(
    ServerId server_id,
    std::string address) {
    if (server_id == 0 ||
        address.empty()) {
        return false;
    }

    std::unique_lock lock(mutex_);

    endpoints_[server_id] =
        std::move(address);

    return true;
}

bool ReReplicationManager::UnregisterChunkserverEndpoint(
    ServerId server_id) {
    if (server_id == 0) {
        return false;
    }

    std::unique_lock lock(mutex_);

    return endpoints_.erase(
        server_id) > 0;
}

std::optional<std::string>
ReReplicationManager::GetChunkserverEndpoint(
    ServerId server_id) const {
    std::shared_lock lock(mutex_);

    const auto it =
        endpoints_.find(server_id);

    if (it == endpoints_.end()) {
        return std::nullopt;
    }

    return it->second;
}

std::size_t
ReReplicationManager::RegisteredChunkserverCount()
    const {
    std::shared_lock lock(mutex_);

    return chunkservers_.size();
}

std::uint32_t
ReReplicationManager::GetDesiredReplicationFactor(
    ChunkHandle handle) const {
    const auto factor =
        master_.GetChunkReplicationFactor(
            handle);

    if (!factor.has_value()) {
        return 0;
    }

    return *factor;
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

    if (chunkserver != nullptr) {
        return chunkserver->ChunkExists(
            handle);
    }

    return GetChunkserverEndpoint(
        server_id).has_value();
}

std::vector<ServerId>
ReReplicationManager::GetHealthyCurrentReplicas(
    ChunkHandle handle,
    std::uint64_t now_ms) const {
    std::vector<ServerId> healthy;

    const auto replicas =
        master_.GetReplicaServers(handle);

    healthy.reserve(
        replicas.size());

    for (const ServerId server_id :
         replicas) {
        if (IsHealthyCurrentReplica(
                handle,
                server_id,
                now_ms)) {
            healthy.push_back(
                server_id);
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
            GetDesiredReplicationFactor(
                handle));

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

    for (const auto& replica :
         replicas) {
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
    std::vector<ServerId>
        destinations;

    if (!NeedsReReplication(
            handle,
            now_ms)) {
        return destinations;
    }

    const std::size_t desired =
        static_cast<std::size_t>(
            GetDesiredReplicationFactor(
                handle));

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

    std::unordered_set<ServerId>
        excluded(
            replicas.begin(),
            replicas.end());

    excluded.insert(
        stale.begin(),
        stale.end());

    std::vector<PlacementCandidate>
        candidates;

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
                    .GetChunksForServer(
                        server_id)
                    .size()});
    }

    if (candidates.empty()) {
        return destinations;
    }

    PlacementRequest request;

    request.handle = handle;
    request.replication_factor =
        static_cast<std::uint32_t>(
            needed);

    request.candidates =
        std::move(candidates);

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
        source_server_id ==
            destination_server_id) {
        return false;
    }

    auto* source =
        GetChunkserver(
            source_server_id);

    auto* destination =
        GetChunkserver(
            destination_server_id);

    if (source != nullptr &&
        destination != nullptr) {
        if (!source->ChunkExists(
                handle)) {
            return false;
        }

        return source->GetReplicaSender()
            .Send(
                handle,
                destination_server_id,
                [destination](
                    ServerId target,
                    ChunkHandle target_handle,
                    const std::string& data) {
                    if (target !=
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

    const auto source_endpoint =
        GetChunkserverEndpoint(
            source_server_id);

    const auto destination_endpoint =
        GetChunkserverEndpoint(
            destination_server_id);

    if (!source_endpoint.has_value() ||
        !destination_endpoint.has_value()) {
        return false;
    }

    const auto source_channel =
        ::grpc::CreateChannel(
            *source_endpoint,
            ::grpc::InsecureChannelCredentials());

    const auto destination_channel =
        ::grpc::CreateChannel(
            *destination_endpoint,
            ::grpc::InsecureChannelCredentials());

    auto source_stub =
        ::gfs::protocol::ChunkserverService::
            NewStub(source_channel);

    auto destination_stub =
        ::gfs::protocol::ChunkserverService::
            NewStub(destination_channel);

    ::gfs::protocol::TransferChunkRequest
        transfer_request;

    transfer_request.set_chunk_handle(
        handle);

    transfer_request.set_chunk_version(1);
    transfer_request.set_offset(0);
    transfer_request.set_length(0);

    ::gfs::protocol::TransferChunkResponse
        transfer_response;

    ::grpc::ClientContext source_context;

    source_context.set_deadline(
        std::chrono::system_clock::now() +
        std::chrono::seconds(5));

    const auto source_status =
        source_stub->TransferChunk(
            &source_context,
            transfer_request,
            &transfer_response);

    if (!source_status.ok() ||
        !transfer_response.success()) {
        return false;
    }

    ::gfs::protocol::CreateReplicaRequest
        create_request;

    create_request.set_chunk_handle(
        handle);

    create_request.set_chunk_version(1);

    ::gfs::protocol::CreateReplicaResponse
        create_response;

    ::grpc::ClientContext create_context;

    create_context.set_deadline(
        std::chrono::system_clock::now() +
        std::chrono::seconds(5));

    const auto create_status =
        destination_stub->CreateReplica(
            &create_context,
            create_request,
            &create_response);

    if (!create_status.ok() ||
        !create_response.success()) {
        return false;
    }

    ::gfs::protocol::WriteChunkRequest
        write_request;

    write_request.set_chunk_handle(
        handle);

    write_request.set_chunk_version(1);
    write_request.set_offset(0);

    write_request.set_data(
        transfer_response.data());

    ::gfs::protocol::WriteChunkResponse
        write_response;

    ::grpc::ClientContext write_context;

    write_context.set_deadline(
        std::chrono::system_clock::now() +
        std::chrono::seconds(5));

    const auto write_status =
        destination_stub->WriteChunk(
            &write_context,
            write_request,
            &write_response);

    return write_status.ok() &&
           write_response.success();
}

bool ReReplicationManager::
DeleteChunkFromChunkservers(
    ChunkHandle handle) {
    if (handle == 0) {
        return false;
    }

    std::vector<
        chunkserver::Chunkserver*> servers;

    {
        std::shared_lock lock(mutex_);

        servers.reserve(
            chunkservers_.size());

        for (const auto& [
                 server_id,
                 server] :
             chunkservers_) {
            static_cast<void>(
                server_id);

            if (server != nullptr) {
                servers.push_back(
                    server);
            }
        }
    }

    std::sort(
        servers.begin(),
        servers.end(),
        [](const auto* lhs,
           const auto* rhs) {
            return lhs->GetServerId() <
                   rhs->GetServerId();
        });

    for (auto* server :
         servers) {
        if (server->ChunkExists(handle) &&
            !server->DeleteChunk(handle)) {
            return false;
        }
    }

    return true;
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

    for (const ServerId server_id :
         replicas) {
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
        for (const ServerId server_id :
             stale) {
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
        for (const ServerId server_id :
             stale) {
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
        for (const ServerId server_id :
             stale) {
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

    for (const ServerId server_id :
         stale) {
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
                GetDesiredReplicationFactor(
                    handle))) {
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
ReReplicationManager::
RecoverFailedChunkservers(
    std::uint64_t now_ms) {
    static_cast<void>(
        master_.DetectFailedChunkservers(
            now_ms));

    std::unordered_set<ChunkHandle>
        affected_set;

    const auto failed =
        master_.GetHeartbeatManager()
            .GetFailedServers();

    for (const ServerId server_id :
         failed) {
        const auto chunks =
            master_.GetReplicaManager()
                .GetChunksForServer(
                    server_id);

        affected_set.insert(
            chunks.begin(),
            chunks.end());
    }

    std::vector<ChunkHandle>
        affected(
            affected_set.begin(),
            affected_set.end());

    std::sort(
        affected.begin(),
        affected.end());

    std::vector<ChunkHandle>
        recovered;

    for (const ChunkHandle handle :
         affected) {
        if (RecoverChunk(
                handle,
                now_ms)) {
            recovered.push_back(
                handle);
        }
    }

    return recovered;
}

std::vector<ChunkHandle>
ReReplicationManager::RecoverAll(
    std::uint64_t now_ms) {
    const auto handles =
        master_.GetAllChunkHandles();

    std::vector<ChunkHandle>
        recovered;

    for (const ChunkHandle handle :
         handles) {
        if (NeedsReReplication(
                handle,
                now_ms) &&
            RecoverChunk(
                handle,
                now_ms)) {
            recovered.push_back(
                handle);
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