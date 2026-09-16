#pragma once

#include <grpcpp/grpcpp.h>

#include "client.grpc.pb.h"

namespace gfs::protocol {

class ClientServiceImpl final
    : public ::gfs::protocol::ClientService::Service {
public:
    ClientServiceImpl() = default;
    ~ClientServiceImpl() override = default;

    ::grpc::Status ExecuteOperation(
        ::grpc::ServerContext* context,
        const ::gfs::protocol::ClientOperationRequest* request,
        ::gfs::protocol::ClientOperationResponse* response
    ) override;

    ::grpc::Status Read(
        ::grpc::ServerContext* context,
        const ::gfs::protocol::ClientReadRequest* request,
        ::gfs::protocol::ClientReadResponse* response
    ) override;

    ::grpc::Status Write(
        ::grpc::ServerContext* context,
        const ::gfs::protocol::ClientWriteRequest* request,
        ::gfs::protocol::ClientWriteResponse* response
    ) override;

    ::grpc::Status Append(
        ::grpc::ServerContext* context,
        const ::gfs::protocol::ClientAppendRequest* request,
        ::gfs::protocol::ClientAppendResponse* response
    ) override;
};

}  // namespace gfs::protocol