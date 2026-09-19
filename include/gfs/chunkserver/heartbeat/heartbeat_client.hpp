#pragma once

#include "gfs/chunkserver/chunkserver.hpp"

#include <cstdint>
#include <string>

#include "master.pb.h"

namespace gfs::chunkserver::heartbeat {

class HeartbeatClient {
public:
    explicit HeartbeatClient(
        Chunkserver& chunkserver);

    HeartbeatClient(
        const HeartbeatClient&) = delete;

    HeartbeatClient& operator=(
        const HeartbeatClient&) = delete;

    [[nodiscard]] ::gfs::protocol::HeartbeatRequest
    BuildHeartbeatRequest(
        std::uint64_t timestamp_ms) const;

    void SetServerAddress(
        std::string server_address);

    [[nodiscard]] bool
    SendHeartbeat(
        const std::string& master_address,
        std::uint64_t timestamp_ms);

private:
    Chunkserver& chunkserver_;

    std::string server_address_;
};

}  // namespace gfs::chunkserver::heartbeat