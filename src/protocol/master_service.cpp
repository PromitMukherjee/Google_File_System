#include "gfs/protocol/master_service.hpp"

#include "gfs/common/utils.hpp"

#include "chunkserver.grpc.pb.h"

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <mutex>
#include <string>
#include <vector>

namespace gfs::protocol {

MasterServiceImpl::MasterServiceImpl(
    ::gfs::master::Master& master) noexcept
    : master_(&master) {
}

void MasterServiceImpl::SetMaster(
    ::gfs::master::Master& master) noexcept {
    std::lock_guard<std::mutex> lock(mutex_);
    master_ = &master;
}

::grpc::Status MasterServiceImpl::CreateFile(
    ::grpc::ServerContext*,
    const ::gfs::protocol::CreateFileRequest* request,
    ::gfs::protocol::CreateFileResponse* response) {

    response->Clear();

    std::lock_guard<std::mutex> lock(mutex_);

    if (master_ == nullptr) {
        response->set_success(false);
        response->set_error_message(
            "Master instance is not configured");
        return ::grpc::Status::OK;
    }

    if (request == nullptr ||
        request->path().empty()) {
        response->set_success(false);
        response->set_error_message(
            "Invalid create-file request");
        return ::grpc::Status::OK;
    }

    const bool success =
        master_->CreateFile(
            request->path(),
            request->replication_factor());

    response->set_success(success);

    if (!success) {
        response->set_error_message(
            "Failed to create file");
    }

    return ::grpc::Status::OK;
}

::grpc::Status MasterServiceImpl::DeleteFile(
    ::grpc::ServerContext*,
    const ::gfs::protocol::DeleteFileRequest* request,
    ::gfs::protocol::DeleteFileResponse* response) {

    response->Clear();

    std::lock_guard<std::mutex> lock(mutex_);

    if (master_ == nullptr) {
        response->set_success(false);
        response->set_error_message(
            "Master instance is not configured");
        return ::grpc::Status::OK;
    }

    if (request == nullptr ||
        request->path().empty()) {
        response->set_success(false);
        response->set_error_message(
            "Invalid delete-file request");
        return ::grpc::Status::OK;
    }

    const bool success =
        master_->DeleteFile(
            request->path());

    response->set_success(success);

    if (!success) {
        response->set_error_message(
            "Failed to delete file");
    }

    return ::grpc::Status::OK;
}

::grpc::Status MasterServiceImpl::RenameFile(
    ::grpc::ServerContext*,
    const ::gfs::protocol::RenameFileRequest* request,
    ::gfs::protocol::RenameFileResponse* response) {

    response->Clear();

    std::lock_guard<std::mutex> lock(mutex_);

    if (master_ == nullptr) {
        response->set_success(false);
        response->set_error_message(
            "Master instance is not configured");
        return ::grpc::Status::OK;
    }

    if (request == nullptr ||
        request->source_path().empty() ||
        request->destination_path().empty()) {
        response->set_success(false);
        response->set_error_message(
            "Invalid rename request");
        return ::grpc::Status::OK;
    }

    const bool success =
        master_->RenameFile(
            request->source_path(),
            request->destination_path());

    response->set_success(success);

    if (!success) {
        response->set_error_message(
            "Failed to rename file");
    }

    return ::grpc::Status::OK;
}

::grpc::Status MasterServiceImpl::GetFileInfo(
    ::grpc::ServerContext*,
    const ::gfs::protocol::GetFileInfoRequest* request,
    ::gfs::protocol::GetFileInfoResponse* response) {

    response->Clear();

    std::lock_guard<std::mutex> lock(mutex_);

    if (master_ == nullptr) {
        response->set_success(false);
        response->set_error_message(
            "Master instance is not configured");
        return ::grpc::Status::OK;
    }

    if (request == nullptr ||
        request->path().empty()) {
        response->set_success(false);
        response->set_error_message(
            "Invalid file-info request");
        return ::grpc::Status::OK;
    }

    const auto metadata =
        master_->GetFileInfo(
            request->path());

    if (!metadata.has_value()) {
        response->set_success(false);
        response->set_error_message(
            "File not found");
        return ::grpc::Status::OK;
    }

    auto* output =
        response->mutable_metadata();

    output->set_path(
        metadata->path);

    output->set_size(
        metadata->size);

    output->set_replication_factor(
        metadata->replication_factor);

    const auto chunks =
        master_->GetFileChunks(
            request->path());

    for (const ChunkHandle handle :
         chunks) {
        output->add_chunk_handles(
            handle);
    }

    response->set_success(true);

    return ::grpc::Status::OK;
}

::grpc::Status MasterServiceImpl::GetChunkLocations(
    ::grpc::ServerContext*,
    const ::gfs::protocol::GetChunkLocationsRequest* request,
    ::gfs::protocol::GetChunkLocationsResponse* response) {

    response->Clear();

    std::lock_guard<std::mutex> lock(mutex_);

    if (master_ == nullptr) {
        response->set_success(false);
        response->set_error_message(
            "Master instance is not configured");
        return ::grpc::Status::OK;
    }

    if (request == nullptr ||
        request->path().empty()) {
        response->set_success(false);
        response->set_error_message(
            "Invalid chunk-location request");
        return ::grpc::Status::OK;
    }

    const auto chunks =
        master_->GetFileChunks(
            request->path());

    const std::size_t index =
        static_cast<std::size_t>(
            request->chunk_index());

    if (index >= chunks.size()) {
        response->set_success(false);
        response->set_error_message(
            "Chunk not found");
        return ::grpc::Status::OK;
    }

    const ChunkHandle handle =
        chunks[index];

    const auto metadata =
        master_->GetChunkInfo(handle);

    if (!metadata.has_value()) {
        response->set_success(false);
        response->set_error_message(
            "Chunk metadata not found");
        return ::grpc::Status::OK;
    }

    auto* location =
        response->mutable_location();

    location->set_chunk_handle(
        handle);

    location->set_version(
        metadata->version);

    const auto replicas =
        master_->GetChunkReplicas(handle);

    for (const ServerId server_id :
         replicas) {

        location->add_replica_server_ids(
            server_id);

        const auto endpoint =
            master_->GetReReplicationManager()
                .GetChunkserverEndpoint(
                    server_id);

        if (!endpoint.has_value() ||
            endpoint->empty()) {
            response->set_success(false);
            response->set_error_message(
                "Missing chunkserver endpoint");
            response->clear_location();

            return ::grpc::Status::OK;
        }

        location->add_replica_addresses(
            *endpoint);
    }

    response->set_success(true);

    return ::grpc::Status::OK;
}

::grpc::Status MasterServiceImpl::GetLeaseHolder(
    ::grpc::ServerContext*,
    const ::gfs::protocol::GetLeaseHolderRequest* request,
    ::gfs::protocol::GetLeaseHolderResponse* response) {

    response->Clear();

    std::lock_guard<std::mutex> lock(mutex_);

    if (master_ == nullptr) {
        response->set_success(false);
        response->set_error_message(
            "Master instance is not configured");
        return ::grpc::Status::OK;
    }

    if (request == nullptr ||
        request->chunk_handle() == 0) {
        response->set_success(false);
        response->set_error_message(
            "Invalid lease request");
        return ::grpc::Status::OK;
    }

    const auto lease =
        master_->GetLease(
            request->chunk_handle());

    if (!lease.has_value()) {
        response->set_success(false);
        response->set_error_message(
            "Lease not found");
        return ::grpc::Status::OK;
    }

    auto* output =
        response->mutable_lease();

    output->set_chunk_handle(
        lease->chunk_handle);

    output->set_primary_server_id(
        lease->primary_server_id);

    output->set_version(
        lease->version);

    output->set_expiration_time_ms(
        lease->expiration_time_ms);

    response->set_success(
        master_->IsLeaseValid(
            request->chunk_handle()));

    if (!response->success()) {
        response->set_error_message(
            "Lease is no longer valid");
    }

    return ::grpc::Status::OK;
}

::grpc::Status MasterServiceImpl::AllocateChunk(
    ::grpc::ServerContext*,
    const ::gfs::protocol::AllocateChunkRequest* request,
    ::gfs::protocol::AllocateChunkResponse* response) {

    response->Clear();

    std::lock_guard<std::mutex> lock(mutex_);

    if (master_ == nullptr) {
        response->set_success(false);
        response->set_error_message(
            "Master instance is not configured");
        return ::grpc::Status::OK;
    }

    if (request == nullptr ||
        request->path().empty()) {
        response->set_success(false);
        response->set_error_message(
            "Invalid allocate-chunk request");
        return ::grpc::Status::OK;
    }

    const auto handle =
        master_->AllocateChunk(
            request->path());

    if (!handle.has_value()) {
        response->set_success(false);
        response->set_error_message(
            "Failed to allocate chunk");
        return ::grpc::Status::OK;
    }

    response->set_chunk_handle(
        *handle);

    const auto metadata =
        master_->GetChunkInfo(
            *handle);

    if (metadata.has_value()) {
        response->set_chunk_version(
            metadata->version);
    } else {
        response->set_chunk_version(1);
    }

    /*
     * Master::AllocateChunk() creates the metadata object but does
     * not select/register its physical replicas. For the network RPC,
     * select the currently live chunkservers and register the
     * resulting placement before returning their locations.
     */
    const auto replication_factor =
        master_->GetChunkReplicationFactor(
            *handle);

    if (!replication_factor.has_value() ||
        *replication_factor == 0) {
        response->set_success(false);
        response->set_error_message(
            "Invalid chunk replication factor");
        return ::grpc::Status::OK;
    }

    const auto live_servers =
        master_->GetHeartbeatManager()
            .GetLiveServers();

    std::vector<
        ::gfs::master::replication::PlacementCandidate>
        candidates;

    candidates.reserve(
        live_servers.size());

    for (const ServerId server_id :
         live_servers) {
        candidates.push_back(
            ::gfs::master::replication::PlacementCandidate{
                server_id,
                master_->GetReplicaManager()
                    .GetChunksForServer(
                        server_id)
                    .size()});
    }

    const auto replicas =
        master_->PlaceChunkReplicas(
            *handle,
            candidates);

    if (replicas.size() !=
        static_cast<std::size_t>(
            *replication_factor)) {
        response->set_success(false);
        response->set_error_message(
            "Unable to place requested chunk replicas");
        response->clear_replica_server_ids();
        response->clear_replica_addresses();

        return ::grpc::Status::OK;
    }

    /*
     * Placement updates the master's replica metadata. The network
     * chunkservers still need an actual empty chunk before a client
     * can issue the first WriteChunk RPC. Create that physical chunk
     * on every selected replica before reporting successful allocation.
     */
    std::vector<std::string> replica_endpoints;

    replica_endpoints.reserve(
        replicas.size());

    for (const ServerId server_id :
         replicas) {

        const auto endpoint =
            master_->GetReReplicationManager()
                .GetChunkserverEndpoint(
                    server_id);

        if (!endpoint.has_value() ||
            endpoint->empty()) {
            response->set_success(false);
            response->set_error_message(
                "Missing chunkserver endpoint");
            response->clear_replica_server_ids();
            response->clear_replica_addresses();

            return ::grpc::Status::OK;
        }

        replica_endpoints.push_back(
            *endpoint);
    }

    for (std::size_t index = 0;
         index < replicas.size();
         ++index) {

        const auto channel =
            ::grpc::CreateChannel(
                replica_endpoints[index],
                ::grpc::InsecureChannelCredentials());

        auto stub =
            ::gfs::protocol::ChunkserverService::
                NewStub(channel);

        ::gfs::protocol::CreateReplicaRequest
            create_request;

        create_request.set_chunk_handle(
            *handle);

        create_request.set_chunk_version(
            metadata.has_value()
                ? metadata->version
                : 1);

        ::gfs::protocol::CreateReplicaResponse
            create_response;

        ::grpc::ClientContext context;

        context.set_deadline(
            std::chrono::system_clock::now() +
            std::chrono::seconds(5));

        const auto status =
            stub->CreateReplica(
                &context,
                create_request,
                &create_response);

        if (!status.ok() ||
            !create_response.success()) {

            for (const ServerId placed_server :
                 replicas) {
                static_cast<void>(
                    master_->RemoveReplica(
                        *handle,
                        placed_server));
            }

            response->set_success(false);
            response->set_error_message(
                "Failed to create chunk replica");
            response->clear_replica_server_ids();
            response->clear_replica_addresses();

            return ::grpc::Status::OK;
        }
    }

    for (std::size_t index = 0;
         index < replicas.size();
         ++index) {

        response->add_replica_server_ids(
            replicas[index]);

        response->add_replica_addresses(
            replica_endpoints[index]);
    }

    response->set_success(true);

    return ::grpc::Status::OK;
}

::grpc::Status MasterServiceImpl::UpdateFileSize(
    ::grpc::ServerContext*,
    const ::gfs::protocol::UpdateFileSizeRequest* request,
    ::gfs::protocol::UpdateFileSizeResponse* response) {

    response->Clear();

    std::lock_guard<std::mutex> lock(mutex_);

    if (master_ == nullptr) {
        response->set_success(false);
        response->set_error_message(
            "Master instance is not configured");
        return ::grpc::Status::OK;
    }

    if (request == nullptr ||
        request->path().empty()) {
        response->set_success(false);
        response->set_error_message(
            "Invalid update-file-size request");
        return ::grpc::Status::OK;
    }

    const bool success =
        master_->UpdateFileSize(
            request->path(),
            request->size());

    response->set_success(success);

    if (!success) {
        response->set_error_message(
            "Failed to update file size");
    }

    return ::grpc::Status::OK;
}

::grpc::Status MasterServiceImpl::Heartbeat(
    ::grpc::ServerContext*,
    const ::gfs::protocol::HeartbeatRequest* request,
    ::gfs::protocol::HeartbeatResponse* response) {

    response->Clear();

    if (master_ == nullptr) {
        response->set_success(false);
        response->set_error_message(
            "Master instance is not configured");
        response->set_master_timestamp_ms(
            ::gfs::UnixTimeMillis());

        return ::grpc::Status::OK;
    }

    if (request == nullptr ||
        request->server_id() == 0) {
        response->set_success(false);
        response->set_error_message(
            "Invalid heartbeat request");
        response->set_master_timestamp_ms(
            ::gfs::UnixTimeMillis());

        return ::grpc::Status::OK;
    }

    std::vector<
        ::gfs::master::heartbeat::ReportedChunk>
        chunks;

    chunks.reserve(
        static_cast<std::size_t>(
            request->chunks_size()));

    for (const auto& report :
         request->chunks()) {

        if (report.chunk_handle() == 0 ||
            report.version() == 0) {
            response->set_success(false);
            response->set_error_message(
                "Invalid chunk information");
            response->set_master_timestamp_ms(
                ::gfs::UnixTimeMillis());

            return ::grpc::Status::OK;
        }

        chunks.push_back(
            ::gfs::master::heartbeat::ReportedChunk{
                report.chunk_handle(),
                report.version()});
    }

    if (!master_->ProcessHeartbeat(
            request->server_id(),
            request->timestamp_ms(),
            chunks)) {

        response->set_success(false);
        response->set_error_message(
            "Heartbeat rejected");
        response->set_master_timestamp_ms(
            ::gfs::UnixTimeMillis());

        return ::grpc::Status::OK;
    }

    /*
     * Phase 1-16 heartbeat callers do not provide a network endpoint.
     * Preserve their successful heartbeat behavior while registering
     * the endpoint whenever the Phase-17 network heartbeat provides one.
     */
    if (!request->server_address().empty()) {
        if (!master_->GetReReplicationManager()
                .RegisterChunkserverEndpoint(
                    request->server_id(),
                    request->server_address())) {

            response->set_success(false);
            response->set_error_message(
                "Chunkserver endpoint registration failed");
            response->set_master_timestamp_ms(
                ::gfs::UnixTimeMillis());

            return ::grpc::Status::OK;
        }
    }

    response->set_success(true);
    response->set_master_timestamp_ms(
        ::gfs::UnixTimeMillis());

    return ::grpc::Status::OK;
}

}  // namespace gfs::protocol