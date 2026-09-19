#include "gfs/network/master_server.hpp"

#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <iostream>
#include <string>

namespace {

std::string Value(
    int argc,
    char** argv,
    const std::string& name,
    std::string fallback) {
    for (int i = 1; i + 1 < argc; ++i) {
        if (std::string(argv[i]) == name) {
            return argv[i + 1];
        }
    }

    return fallback;
}

std::uint16_t ParsePort(
    int argc,
    char** argv) {
    return static_cast<std::uint16_t>(
        std::stoul(
            Value(
                argc,
                argv,
                "--port",
                "5000")));
}

std::uint32_t ParseReplicationFactor(
    int argc,
    char** argv) {
    return static_cast<std::uint32_t>(
        std::stoul(
            Value(
                argc,
                argv,
                "--replication",
                "3")));
}

}  // namespace

int main(
    int argc,
    char** argv) {
    const std::string host =
        Value(
            argc,
            argv,
            "--host",
            "127.0.0.1");

    const std::uint16_t port =
        ParsePort(
            argc,
            argv);

    const std::uint32_t replication_factor =
        ParseReplicationFactor(
            argc,
            argv);

    const std::filesystem::path
        persistence_directory =
            Value(
                argc,
                argv,
                "--persistence",
                "./data/master");

    gfs::network::MasterServer server(
        host,
        port,
        replication_factor,
        persistence_directory);

    if (!server.Start()) {
        std::cerr
            << "failed to start gfs-master\n";

        return EXIT_FAILURE;
    }

    std::cout
        << "gfs-master listening on "
        << server.GetAddress()
        << '\n';

    server.Wait();

    return EXIT_SUCCESS;
}