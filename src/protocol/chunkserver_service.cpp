#include "gfs/protocol/chunkserver_service.hpp"

namespace gfs::protocol {

::grpc::Status ChunkserverServiceImpl::ReadChunk(
    ::grpc::ServerContext*,
    const ::gfs::protocol::ReadChunkRequest*,
    ::gfs::protocol::ReadChunkResponse* response
) {
    response->set_success(false);
    response->set_error_message(
        "Chunkserver storage logic is not implemented in Phase 2"
    );

    return ::grpc::Status::OK;
}

::grpc::Status ChunkserverServiceImpl::PushData(
    ::grpc::ServerContext*,
    const ::gfs::protocol::PushDataRequest*,
    ::gfs::protocol::PushDataResponse* response
) {
    response->set_success(false);
    response->set_error_message(
        "Chunkserver storage logic is not implemented in Phase 2"
    );

    return ::grpc::Status::OK;
}

::grpc::Status ChunkserverServiceImpl::WriteChunk(
    ::grpc::ServerContext*,
    const ::gfs::protocol::WriteChunkRequest*,
    ::gfs::protocol::WriteChunkResponse* response
) {
    response->set_success(false);
    response->set_error_message(
        "Chunkserver storage logic is not implemented in Phase 2"
    );

    return ::grpc::Status::OK;
}

::grpc::Status ChunkserverServiceImpl::ApplyMutation(
    ::grpc::ServerContext*,
    const ::gfs::protocol::ApplyMutationRequest*,
    ::gfs::protocol::ApplyMutationResponse* response
) {
    response->set_success(false);
    response->set_error_message(
        "Mutation logic is not implemented in Phase 2"
    );

    return ::grpc::Status::OK;
}

::grpc::Status ChunkserverServiceImpl::TransferChunk(
    ::grpc::ServerContext*,
    const ::gfs::protocol::TransferChunkRequest*,
    ::gfs::protocol::TransferChunkResponse* response
) {
    response->set_success(false);
    response->set_error_message(
        "Replica transfer logic is not implemented in Phase 2"
    );

    return ::grpc::Status::OK;
}

::grpc::Status ChunkserverServiceImpl::CloneChunk(
    ::grpc::ServerContext*,
    const ::gfs::protocol::CloneChunkRequest*,
    ::gfs::protocol::CloneChunkResponse* response
) {
    response->set_success(false);
    response->set_error_message(
        "Chunk cloning logic is not implemented in Phase 2"
    );

    return ::grpc::Status::OK;
}

::grpc::Status ChunkserverServiceImpl::CreateReplica(
    ::grpc::ServerContext*,
    const ::gfs::protocol::CreateReplicaRequest*,
    ::gfs::protocol::CreateReplicaResponse* response
) {
    response->set_success(false);
    response->set_error_message(
        "Replica creation logic is not implemented in Phase 2"
    );

    return ::grpc::Status::OK;
}

}  // namespace gfs::protocol