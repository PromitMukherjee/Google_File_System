#pragma once

#include "gfs/master/master.hpp"
#include "gfs/protocol/master_service.hpp"

#include <cstdint>
#include <filesystem>
#include <memory>
#include <string>

namespace gfs::network {

class MasterServer {
public:
    MasterServer(
        std::string host,
        std::uint16_t port,
        std::uint32_t replication_factor = 3,
        std::filesystem::path persistence_directory = {});

    MasterServer(
        const MasterServer&) = delete;

    MasterServer& operator=(
        const MasterServer&) = delete;

    ~MasterServer();

    [[nodiscard]] bool Start();

    void Shutdown();

    void Wait();

    [[nodiscard]] bool IsRunning()
        const noexcept;

    [[nodiscard]] std::uint16_t GetPort()
        const noexcept;

    [[nodiscard]] std::string
    GetAddress() const;

    [[nodiscard]] master::Master&
    GetMaster() noexcept;

    [[nodiscard]] const master::Master&
    GetMaster() const noexcept;

private:
    std::string host_;

    std::uint16_t port_ = 0;

    std::uint32_t replication_factor_ = 3;

    std::filesystem::path
        persistence_directory_;

    master::Master master_;

    protocol::MasterServiceImpl
        service_;

    std::unique_ptr<
        ::grpc::Server>
        server_;
};

}  // namespace gfs::network