// PATH: src/network/chunkserver_server.cpp

#include "gfs/network/chunkserver_server.hpp"

#include "gfs/common/utils.hpp"

#include <grpcpp/grpcpp.h>

#include <chrono>
#include <thread>
#include <utility>

namespace gfs::network {

ChunkserverServer::ChunkserverServer(
    ServerId server_id,
    std::string host,
    std::uint16_t port,
    std::string master_address,
    std::string storage_directory)
    : server_id_(server_id),
      host_(std::move(host)),
      port_(port),
      master_address_(std::move(master_address)),
      storage_directory_(std::move(storage_directory)),
      chunkserver_(
          server_id_,
          storage_directory_),
      heartbeat_client_(chunkserver_),
      service_(chunkserver_) {
}

ChunkserverServer::~ChunkserverServer() {
    Shutdown();
}

bool ChunkserverServer::Start() {
    if (server_ ||
        server_id_ == 0 ||
        master_address_.empty()) {
        return false;
    }

    if (!chunkserver_.Initialize()) {
        return false;
    }

    ::grpc::ServerBuilder builder;

    const std::string endpoint =
        host_ + ":" + std::to_string(port_);

    const int selected_port =
        builder.AddListeningPort(
            endpoint,
            ::grpc::InsecureServerCredentials());

    if (selected_port <= 0) {
        return false;
    }

    builder.RegisterService(&service_);

    server_ = builder.BuildAndStart();

    if (!server_) {
        return false;
    }

    port_ =
        static_cast<std::uint16_t>(
            selected_port);

    /*
     * Configure the heartbeat client with the
     * actual chunkserver endpoint. This is
     * especially important when port_ was 0
     * and gRPC selected an available port.
     */
    heartbeat_client_.SetServerAddress(
        GetAddress());

    heartbeat_thread_ =
        std::jthread(
            [this](
                std::stop_token stop_token) {
                HeartbeatLoop(stop_token);
            });

    return true;
}

void ChunkserverServer::Shutdown() {
    heartbeat_thread_.request_stop();

    if (server_) {
        server_->Shutdown();
        server_.reset();
    }

    if (heartbeat_thread_.joinable()) {
        heartbeat_thread_.join();
    }
}

void ChunkserverServer::Wait() {
    if (server_) {
        server_->Wait();
    }
}

bool ChunkserverServer::IsRunning() const noexcept {
    return static_cast<bool>(server_);
}

std::uint16_t ChunkserverServer::GetPort() const noexcept {
    return port_;
}

std::string ChunkserverServer::GetAddress() const {
    return host_ + ":" + std::to_string(port_);
}

chunkserver::Chunkserver& ChunkserverServer::GetChunkserver() noexcept {
    return chunkserver_;
}

const chunkserver::Chunkserver&
ChunkserverServer::GetChunkserver() const noexcept {
    return chunkserver_;
}

void ChunkserverServer::HeartbeatLoop(
    std::stop_token stop_token) {

    while (!stop_token.stop_requested()) {
        static_cast<void>(
            heartbeat_client_.SendHeartbeat(
                master_address_,
                ::gfs::UnixTimeMillis()));

        std::this_thread::sleep_for(
            std::chrono::seconds(1));
    }
}

}  // namespace gfs::network