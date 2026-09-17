#include "gfs/protocol/master_service.hpp"

#include "gfs/common/utils.hpp"

namespace gfs::protocol {

MasterServiceImpl::MasterServiceImpl(
    ::gfs::master::Master& master) noexcept
    : master_(&master) {
}

void MasterServiceImpl::SetMaster(
    ::gfs::master::Master& master) noexcept {
    master_ = &master;
}

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
    const ::gfs::protocol::HeartbeatRequest* request,
    ::gfs::protocol::HeartbeatResponse* response
) {
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

    response->set_success(true);
    response->set_master_timestamp_ms(
        ::gfs::UnixTimeMillis());

    return ::grpc::Status::OK;
}

}  // namespace gfs::protocol