#include "gfs/network/chunkserver_server.hpp"

#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <string>

namespace {

std::string Value(
    int argc,
    char** argv,
    const std::string& name,
    std::string fallback) {
    for (int i = 1;
         i + 1 < argc;
         ++i) {
        if (argv[i] == name) {
            return argv[i + 1];
        }
    }

    return fallback;
}

}  // namespace

int main(
    int argc,
    char** argv) {
    const auto id =
        static_cast<gfs::ServerId>(
            std::stoull(
                Value(
                    argc,
                    argv,
                    "--id",
                    "1")));

    const std::string host =
        Value(
            argc,
            argv,
            "--host",
            "127.0.0.1");

    const auto port =
        static_cast<std::uint16_t>(
            std::stoul(
                Value(
                    argc,
                    argv,
                    "--port",
                    "5001")));

    const std::string master =
        Value(
            argc,
            argv,
            "--master",
            "127.0.0.1:5000");

    const std::string storage =
        Value(
            argc,
            argv,
            "--storage",
            "./data/chunkserver1");

    gfs::network::ChunkserverServer server(
        id,
        host,
        port,
        master,
        storage);

    if (!server.Start()) {
        std::cerr
            << "failed to start gfs-chunkserver\n";

        return EXIT_FAILURE;
    }

    std::cout
        << "gfs-chunkserver "
        << id
        << " listening on "
        << server.GetAddress()
        << '\n';

    server.Wait();

    return EXIT_SUCCESS;
}