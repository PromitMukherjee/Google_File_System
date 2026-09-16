#include "gfs/protocol/client_service.hpp"

namespace gfs::protocol {

::grpc::Status ClientServiceImpl::ExecuteOperation(
    ::grpc::ServerContext*,
    const ::gfs::protocol::ClientOperationRequest*,
    ::gfs::protocol::ClientOperationResponse* response
) {
    response->set_success(false);
    response->set_error_message(
        "Client filesystem logic is not implemented in Phase 2"
    );

    return ::grpc::Status::OK;
}

::grpc::Status ClientServiceImpl::Read(
    ::grpc::ServerContext*,
    const ::gfs::protocol::ClientReadRequest*,
    ::gfs::protocol::ClientReadResponse* response
) {
    response->set_success(false);
    response->set_error_message(
        "Client read logic is not implemented in Phase 2"
    );

    return ::grpc::Status::OK;
}

::grpc::Status ClientServiceImpl::Write(
    ::grpc::ServerContext*,
    const ::gfs::protocol::ClientWriteRequest*,
    ::gfs::protocol::ClientWriteResponse* response
) {
    response->set_success(false);
    response->set_error_message(
        "Client write logic is not implemented in Phase 2"
    );

    return ::grpc::Status::OK;
}

::grpc::Status ClientServiceImpl::Append(
    ::grpc::ServerContext*,
    const ::gfs::protocol::ClientAppendRequest*,
    ::gfs::protocol::ClientAppendResponse* response
) {
    response->set_success(false);
    response->set_error_message(
        "Client append logic is not implemented in Phase 2"
    );

    return ::grpc::Status::OK;
}

}  // namespace gfs::protocol