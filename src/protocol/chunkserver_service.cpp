#include "gfs/protocol/chunkserver_service.hpp"

#include "gfs/chunkserver/mutation/mutation.hpp"
#include "gfs/common/constants.hpp"

#include <grpcpp/grpcpp.h>
#include <string>

namespace gfs::protocol {
namespace {

::grpc::Status InvalidRequest(
    const char* message) {
    return ::grpc::Status(
        ::grpc::StatusCode::INVALID_ARGUMENT,
        message);
}

}  // namespace

ChunkserverServiceImpl::ChunkserverServiceImpl(
    ::gfs::chunkserver::Chunkserver& chunkserver) noexcept
    : chunkserver_(&chunkserver) {
}

void ChunkserverServiceImpl::SetChunkserver(
    ::gfs::chunkserver::Chunkserver& chunkserver) noexcept {
    chunkserver_ = &chunkserver;
}

::grpc::Status ChunkserverServiceImpl::ReadChunk(
    ::grpc::ServerContext*,
    const ::gfs::protocol::ReadChunkRequest* request,
    ::gfs::protocol::ReadChunkResponse* response) {
    if (chunkserver_ == nullptr ||
        request == nullptr ||
        response == nullptr) {
        return InvalidRequest(
            "invalid read request");
    }

    response->Clear();

    std::string data;

    if (!chunkserver_->ReadChunk(
            request->chunk_handle(),
            request->offset(),
            static_cast<std::size_t>(
                request->length()),
            data)) {
        response->set_error_message(
            "chunk read failed");

        return ::grpc::Status::OK;
    }

    response->set_success(true);
    response->set_data(data);

    return ::grpc::Status::OK;
}

::grpc::Status ChunkserverServiceImpl::PushData(
    ::grpc::ServerContext*,
    const ::gfs::protocol::PushDataRequest* request,
    ::gfs::protocol::PushDataResponse* response) {
    if (chunkserver_ == nullptr ||
        request == nullptr ||
        response == nullptr) {
        return InvalidRequest(
            "invalid push-data request");
    }

    response->Clear();

    const bool ok =
        chunkserver_->GetReplicaReceiver()
            .Receive(
                request->chunk_handle(),
                request->data());

    response->set_success(ok);

    if (!ok) {
        response->set_error_message(
            "push data failed");
    }

    return ::grpc::Status::OK;
}

::grpc::Status ChunkserverServiceImpl::WriteChunk(
    ::grpc::ServerContext*,
    const ::gfs::protocol::WriteChunkRequest* request,
    ::gfs::protocol::WriteChunkResponse* response) {
    if (chunkserver_ == nullptr ||
        request == nullptr ||
        response == nullptr) {
        return InvalidRequest(
            "invalid write request");
    }

    response->Clear();

    const bool ok =
        chunkserver_->WriteChunk(
            request->chunk_handle(),
            request->offset(),
            request->data());

    response->set_success(ok);
    response->set_mutation_id(
        request->mutation_id());

    if (!ok) {
        response->set_error_message(
            "chunk write failed");
    }

    return ::grpc::Status::OK;
}

::grpc::Status ChunkserverServiceImpl::ApplyMutation(
    ::grpc::ServerContext*,
    const ::gfs::protocol::ApplyMutationRequest* request,
    ::gfs::protocol::ApplyMutationResponse* response) {
    if (chunkserver_ == nullptr ||
        request == nullptr ||
        response == nullptr) {
        return InvalidRequest(
            "invalid mutation request");
    }

    response->Clear();

    ::gfs::chunkserver::mutation::Mutation mutation;

    mutation.chunk_handle =
        request->chunk_handle();

    mutation.chunk_version =
        request->chunk_version();

    mutation.mutation_id =
        request->mutation_id();

    mutation.offset =
        request->offset();

    mutation.data =
        request->data();

    const bool ok =
        chunkserver_->GetMutationManager()
            .ApplyMutation(mutation);

    response->set_success(ok);
    response->set_mutation_id(
        mutation.mutation_id);

    if (!ok) {
        response->set_error_message(
            "mutation failed");
    }

    return ::grpc::Status::OK;
}

::grpc::Status ChunkserverServiceImpl::TransferChunk(
    ::grpc::ServerContext*,
    const ::gfs::protocol::TransferChunkRequest* request,
    ::gfs::protocol::TransferChunkResponse* response) {
    if (chunkserver_ == nullptr ||
        request == nullptr ||
        response == nullptr) {
        return InvalidRequest(
            "invalid transfer request");
    }

    response->Clear();

    const std::uint64_t size =
        chunkserver_->GetChunkSize(
            request->chunk_handle());

    const std::uint64_t length =
        request->length() == 0
            ? size
            : request->length();

    if (request->offset() > size ||
        length > size - request->offset() ||
        length >
            static_cast<std::uint64_t>(
                gfs::constants::kChunkSize)) {
        response->set_error_message(
            "invalid transfer range");

        return ::grpc::Status::OK;
    }

    std::string data;

    if (!chunkserver_->ReadChunk(
            request->chunk_handle(),
            request->offset(),
            static_cast<std::size_t>(
                length),
            data)) {
        response->set_error_message(
            "chunk transfer read failed");

        return ::grpc::Status::OK;
    }

    response->set_success(true);
    response->set_data(data);

    return ::grpc::Status::OK;
}

::grpc::Status ChunkserverServiceImpl::CloneChunk(
    ::grpc::ServerContext*,
    const ::gfs::protocol::CloneChunkRequest* request,
    ::gfs::protocol::CloneChunkResponse* response) {
    if (chunkserver_ == nullptr ||
        request == nullptr ||
        response == nullptr) {
        return InvalidRequest(
            "invalid clone request");
    }

    response->Clear();

    const bool ok =
        chunkserver_->GetCloneManager()
            .Clone(
                request->source_chunk_handle(),
                request->destination_chunk_handle());

    response->set_success(ok);

    if (!ok) {
        response->set_error_message(
            "clone failed");
    }

    return ::grpc::Status::OK;
}

::grpc::Status ChunkserverServiceImpl::CreateReplica(
    ::grpc::ServerContext*,
    const ::gfs::protocol::CreateReplicaRequest* request,
    ::gfs::protocol::CreateReplicaResponse* response) {
    if (chunkserver_ == nullptr ||
        request == nullptr ||
        response == nullptr) {
        return InvalidRequest(
            "invalid create-replica request");
    }

    response->Clear();

    const bool ok =
        chunkserver_->ChunkExists(
            request->chunk_handle()) ||
        chunkserver_->CreateChunk(
            request->chunk_handle());

    response->set_success(ok);

    if (!ok) {
        response->set_error_message(
            "replica creation failed");
    }

    return ::grpc::Status::OK;
}

}  // namespace gfs::protocol