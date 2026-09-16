#pragma once

#include <grpcpp/grpcpp.h>

#include "master.grpc.pb.h"

namespace gfs::protocol {

class MasterServiceImpl final
    : public ::gfs::protocol::MasterService::Service {
public:
    MasterServiceImpl() = default;
    ~MasterServiceImpl() override = default;

    ::grpc::Status CreateFile(
        ::grpc::ServerContext* context,
        const ::gfs::protocol::CreateFileRequest* request,
        ::gfs::protocol::CreateFileResponse* response
    ) override;

    ::grpc::Status DeleteFile(
        ::grpc::ServerContext* context,
        const ::gfs::protocol::DeleteFileRequest* request,
        ::gfs::protocol::DeleteFileResponse* response
    ) override;

    ::grpc::Status RenameFile(
        ::grpc::ServerContext* context,
        const ::gfs::protocol::RenameFileRequest* request,
        ::gfs::protocol::RenameFileResponse* response
    ) override;

    ::grpc::Status GetFileInfo(
        ::grpc::ServerContext* context,
        const ::gfs::protocol::GetFileInfoRequest* request,
        ::gfs::protocol::GetFileInfoResponse* response
    ) override;

    ::grpc::Status GetChunkLocations(
        ::grpc::ServerContext* context,
        const ::gfs::protocol::GetChunkLocationsRequest* request,
        ::gfs::protocol::GetChunkLocationsResponse* response
    ) override;

    ::grpc::Status GetLeaseHolder(
        ::grpc::ServerContext* context,
        const ::gfs::protocol::GetLeaseHolderRequest* request,
        ::gfs::protocol::GetLeaseHolderResponse* response
    ) override;

    ::grpc::Status Heartbeat(
        ::grpc::ServerContext* context,
        const ::gfs::protocol::HeartbeatRequest* request,
        ::gfs::protocol::HeartbeatResponse* response
    ) override;
};

}  // namespace gfs::protocol