#pragma once

#include "gfs/chunkserver/chunkserver.hpp"
#include "gfs/chunkserver/heartbeat/heartbeat_client.hpp"
#include "gfs/protocol/chunkserver_service.hpp"

#include <cstdint>
#include <memory>
#include <string>
#include <thread>

namespace gfs::network {

class ChunkserverServer {
public:
    ChunkserverServer(
        ServerId server_id,
        std::string host,
        std::uint16_t port,
        std::string master_address,
        std::string storage_directory);

    ChunkserverServer(
        const ChunkserverServer&) = delete;

    ChunkserverServer& operator=(
        const ChunkserverServer&) = delete;

    ~ChunkserverServer();

    [[nodiscard]] bool Start();

    void Shutdown();

    void Wait();

    [[nodiscard]] bool IsRunning()
        const noexcept;

    [[nodiscard]] std::uint16_t GetPort()
        const noexcept;

    [[nodiscard]] std::string
    GetAddress() const;

    [[nodiscard]] chunkserver::Chunkserver&
    GetChunkserver() noexcept;

    [[nodiscard]] const chunkserver::Chunkserver&
    GetChunkserver() const noexcept;

private:
    void HeartbeatLoop(
        std::stop_token stop_token);

    ServerId server_id_ = 0;

    std::string host_;

    std::uint16_t port_ = 0;

    std::string master_address_;

    std::string storage_directory_;

    chunkserver::Chunkserver
        chunkserver_;

    chunkserver::heartbeat::HeartbeatClient
        heartbeat_client_;

    protocol::ChunkserverServiceImpl
        service_;

    std::unique_ptr<
        ::grpc::Server>
        server_;

    std::jthread heartbeat_thread_;
};

}  // namespace gfs::network