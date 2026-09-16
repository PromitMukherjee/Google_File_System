#pragma once

#include <grpcpp/grpcpp.h>

#include "chunkserver.grpc.pb.h"

namespace gfs::protocol {

class ChunkserverServiceImpl final
    : public ::gfs::protocol::ChunkserverService::Service {
public:
    ChunkserverServiceImpl() = default;
    ~ChunkserverServiceImpl() override = default;

    ::grpc::Status ReadChunk(
        ::grpc::ServerContext* context,
        const ::gfs::protocol::ReadChunkRequest* request,
        ::gfs::protocol::ReadChunkResponse* response
    ) override;

    ::grpc::Status PushData(
        ::grpc::ServerContext* context,
        const ::gfs::protocol::PushDataRequest* request,
        ::gfs::protocol::PushDataResponse* response
    ) override;

    ::grpc::Status WriteChunk(
        ::grpc::ServerContext* context,
        const ::gfs::protocol::WriteChunkRequest* request,
        ::gfs::protocol::WriteChunkResponse* response
    ) override;

    ::grpc::Status ApplyMutation(
        ::grpc::ServerContext* context,
        const ::gfs::protocol::ApplyMutationRequest* request,
        ::gfs::protocol::ApplyMutationResponse* response
    ) override;

    ::grpc::Status TransferChunk(
        ::grpc::ServerContext* context,
        const ::gfs::protocol::TransferChunkRequest* request,
        ::gfs::protocol::TransferChunkResponse* response
    ) override;

    ::grpc::Status CloneChunk(
        ::grpc::ServerContext* context,
        const ::gfs::protocol::CloneChunkRequest* request,
        ::gfs::protocol::CloneChunkResponse* response
    ) override;

    ::grpc::Status CreateReplica(
        ::grpc::ServerContext* context,
        const ::gfs::protocol::CreateReplicaRequest* request,
        ::gfs::protocol::CreateReplicaResponse* response
    ) override;
};

}  // namespace gfs::protocol