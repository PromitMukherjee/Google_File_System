#include "gfs/protocol/master_service.hpp"

namespace gfs::protocol {

::grpc::Status MasterServiceImpl::CreateFile(
    ::grpc::ServerContext*,
    const ::gfs::protocol::CreateFileRequest*,
    ::gfs::protocol::CreateFileResponse* response
) {
    response->set_success(false);
    response->set_error_message(
        "Master metadata logic is not implemented in Phase 2"
    );

    return ::grpc::Status::OK;
}

::grpc::Status MasterServiceImpl::DeleteFile(
    ::grpc::ServerContext*,
    const ::gfs::protocol::DeleteFileRequest*,
    ::gfs::protocol::DeleteFileResponse* response
) {
    response->set_success(false);
    response->set_error_message(
        "Master metadata logic is not implemented in Phase 2"
    );

    return ::grpc::Status::OK;
}

::grpc::Status MasterServiceImpl::RenameFile(
    ::grpc::ServerContext*,
    const ::gfs::protocol::RenameFileRequest*,
    ::gfs::protocol::RenameFileResponse* response
) {
    response->set_success(false);
    response->set_error_message(
        "Master metadata logic is not implemented in Phase 2"
    );

    return ::grpc::Status::OK;
}

::grpc::Status MasterServiceImpl::GetFileInfo(
    ::grpc::ServerContext*,
    const ::gfs::protocol::GetFileInfoRequest*,
    ::gfs::protocol::GetFileInfoResponse* response
) {
    response->set_success(false);
    response->set_error_message(
        "Master metadata logic is not implemented in Phase 2"
    );

    return ::grpc::Status::OK;
}

::grpc::Status MasterServiceImpl::GetChunkLocations(
    ::grpc::ServerContext*,
    const ::gfs::protocol::GetChunkLocationsRequest*,
    ::gfs::protocol::GetChunkLocationsResponse* response
) {
    response->set_success(false);
    response->set_error_message(
        "Master metadata logic is not implemented in Phase 2"
    );

    return ::grpc::Status::OK;
}

::grpc::Status MasterServiceImpl::GetLeaseHolder(
    ::grpc::ServerContext*,
    const ::gfs::protocol::GetLeaseHolderRequest*,
    ::gfs::protocol::GetLeaseHolderResponse* response
) {
    response->set_success(false);
    response->set_error_message(
        "Lease management is not implemented in Phase 2"
    );

    return ::grpc::Status::OK;
}

::grpc::Status MasterServiceImpl::Heartbeat(
    ::grpc::ServerContext*,
    const ::gfs::protocol::HeartbeatRequest*,
    ::gfs::protocol::HeartbeatResponse* response
) {
    response->set_success(false);
    response->set_error_message(
        "Heartbeat implementation is not implemented in Phase 2"
    );

    return ::grpc::Status::OK;
}

}  // namespace gfs::protocol