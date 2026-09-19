// PATH: src/network/chunkserver_server.cpp

#include "gfs/network/chunkserver_server.hpp"

#include "gfs/common/utils.hpp"

#include <grpcpp/grpcpp.h>

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <chrono>
#include <cstdint>
#include <optional>
#include <thread>
#include <utility>

namespace {

std::optional<std::uint16_t> FindAvailableTcpPort() {
    const int socket_fd =
        ::socket(
            AF_INET,
            SOCK_STREAM,
            0);

    if (socket_fd < 0) {
        return std::nullopt;
    }

    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    address.sin_port = htons(0);

    if (::bind(
            socket_fd,
            reinterpret_cast<const sockaddr*>(&address),
            sizeof(address)) < 0) {
        ::close(socket_fd);
        return std::nullopt;
    }

    socklen_t address_length =
        static_cast<socklen_t>(sizeof(address));

    if (::getsockname(
            socket_fd,
            reinterpret_cast<sockaddr*>(&address),
            &address_length) < 0) {
        ::close(socket_fd);
        return std::nullopt;
    }

    const std::uint16_t port =
        ntohs(address.sin_port);

    ::close(socket_fd);

    if (port == 0) {
        return std::nullopt;
    }

    return port;
}

}  // namespace

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

    const bool requested_ephemeral_port =
        (port_ == 0);

    if (requested_ephemeral_port) {
        const std::optional<std::uint16_t> selected_port =
            FindAvailableTcpPort();

        if (!selected_port.has_value()) {
            return false;
        }

        port_ = *selected_port;
    }

    ::grpc::ServerBuilder builder;

    const std::string endpoint =
        host_ + ":" + std::to_string(port_);

    builder.AddListeningPort(
        endpoint,
        ::grpc::InsecureServerCredentials());

    builder.RegisterService(&service_);

    server_ = builder.BuildAndStart();

    if (!server_) {
        if (requested_ephemeral_port) {
            port_ = 0;
        }

        return false;
    }

    /*
     * The heartbeat client must advertise the actual
     * chunkserver endpoint, including the dynamically
     * selected port when port_ was initially zero.
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