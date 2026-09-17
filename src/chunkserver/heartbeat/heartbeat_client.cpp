#include "gfs/chunkserver/heartbeat/heartbeat_client.hpp"

#include <grpcpp/grpcpp.h>

#include "gfs/common/types.hpp"
#include "master.grpc.pb.h"

namespace gfs::chunkserver::heartbeat {

HeartbeatClient::HeartbeatClient(
    Chunkserver& chunkserver)
    : chunkserver_(chunkserver) {
}

::gfs::protocol::HeartbeatRequest
HeartbeatClient::BuildHeartbeatRequest(
    std::uint64_t timestamp_ms) const {
    ::gfs::protocol::HeartbeatRequest request;

    request.set_server_id(
        chunkserver_.GetServerId());

    request.set_timestamp_ms(timestamp_ms);

    const auto handles =
        chunkserver_.ListChunks();

    for (const ChunkHandle handle : handles) {
        if (handle == 0) {
            continue;
        }

        auto* report =
            request.add_chunks();

        report->set_chunk_handle(handle);
        report->set_version(1);
    }

    return request;
}

bool HeartbeatClient::SendHeartbeat(
    const std::string& master_address,
    std::uint64_t timestamp_ms) {
    if (master_address.empty() ||
        chunkserver_.GetServerId() == 0) {
        return false;
    }

    const auto channel =
        ::grpc::CreateChannel(
            master_address,
            ::grpc::InsecureChannelCredentials());

    if (!channel) {
        return false;
    }

    auto stub =
        ::gfs::protocol::MasterService::
            NewStub(channel);

    if (!stub) {
        return false;
    }

    const auto request =
        BuildHeartbeatRequest(timestamp_ms);

    ::gfs::protocol::HeartbeatResponse response;
    ::grpc::ClientContext context;

    const ::grpc::Status status =
        stub->Heartbeat(
            &context,
            request,
            &response);

    return status.ok() &&
           response.success();
}

}  // namespace gfs::chunkserver::heartbeat