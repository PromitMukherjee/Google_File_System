// PATH: src/network/master_server.cpp

#include "gfs/network/master_server.hpp"

#include <grpcpp/grpcpp.h>

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cstdint>
#include <optional>
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

MasterServer::MasterServer(
    std::string host,
    std::uint16_t port,
    std::uint32_t replication_factor,
    std::filesystem::path persistence_directory)
    : host_(std::move(host)),
      port_(port),
      replication_factor_(replication_factor),
      persistence_directory_(std::move(persistence_directory)),
      master_(
          replication_factor_,
          persistence_directory_),
      service_(master_) {
}

MasterServer::~MasterServer() {
    Shutdown();
}

bool MasterServer::Start() {
    if (server_) {
        return false;
    }

    if (!master_.Initialize()) {
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

    return true;
}

void MasterServer::Shutdown() {
    if (server_) {
        server_->Shutdown();
        server_.reset();
    }
}

void MasterServer::Wait() {
    if (server_) {
        server_->Wait();
    }
}

bool MasterServer::IsRunning() const noexcept {
    return static_cast<bool>(server_);
}

std::uint16_t MasterServer::GetPort() const noexcept {
    return port_;
}

std::string MasterServer::GetAddress() const {
    return host_ + ":" + std::to_string(port_);
}

master::Master& MasterServer::GetMaster() noexcept {
    return master_;
}

const master::Master& MasterServer::GetMaster() const noexcept {
    return master_;
}

}  // namespace gfs::network