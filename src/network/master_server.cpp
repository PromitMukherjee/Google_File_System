// PATH: src/network/master_server.cpp

#include "gfs/network/master_server.hpp"

#include <grpcpp/grpcpp.h>

#include <utility>

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