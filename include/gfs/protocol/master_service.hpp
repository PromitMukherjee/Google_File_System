#pragma once

#include <grpcpp/grpcpp.h>

#include <mutex>

#include "gfs/master/master.hpp"
#include "master.grpc.pb.h"

namespace gfs::protocol {

class MasterServiceImpl final
    : public ::gfs::protocol::MasterService::Service {
public:
    MasterServiceImpl() = default;

    explicit MasterServiceImpl(
        ::gfs::master::Master& master) noexcept;

    ~MasterServiceImpl() override = default;

    void SetMaster(
        ::gfs::master::Master& master) noexcept;

    ::grpc::Status CreateFile(
        ::grpc::ServerContext* context,
        const ::gfs::protocol::CreateFileRequest* request,
        ::gfs::protocol::CreateFileResponse* response
    ) override;

    ::grpc::Status CreateDirectory(
        ::grpc::ServerContext* context,
        const ::gfs::protocol::CreateDirectoryRequest* request,
        ::gfs::protocol::CreateDirectoryResponse* response
    ) override;

    ::grpc::Status ListDirectory(
        ::grpc::ServerContext* context,
        const ::gfs::protocol::ListDirectoryRequest* request,
        ::gfs::protocol::ListDirectoryResponse* response
    ) override;

    ::grpc::Status CreateSnapshot(
        ::grpc::ServerContext* context,
        const ::gfs::protocol::CreateSnapshotRequest* request,
        ::gfs::protocol::CreateSnapshotResponse* response
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

    ::grpc::Status AllocateChunk(
        ::grpc::ServerContext* context,
        const ::gfs::protocol::AllocateChunkRequest* request,
        ::gfs::protocol::AllocateChunkResponse* response
    ) override;

    ::grpc::Status UpdateFileSize(
        ::grpc::ServerContext* context,
        const ::gfs::protocol::UpdateFileSizeRequest* request,
        ::gfs::protocol::UpdateFileSizeResponse* response
    ) override;

    ::grpc::Status Heartbeat(
        ::grpc::ServerContext* context,
        const ::gfs::protocol::HeartbeatRequest* request,
        ::gfs::protocol::HeartbeatResponse* response
    ) override;

private:
    mutable std::mutex mutex_;

    ::gfs::master::Master* master_ = nullptr;
};

}  // namespace gfs::protocol