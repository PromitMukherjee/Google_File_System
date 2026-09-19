#include "gfs/client/gfs_client.hpp"
#include "gfs/client/metadata/chunk_location_cache.hpp"
#include "gfs/client/metadata/master_client.hpp"

#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <grpcpp/grpcpp.h>
#include <iostream>
#include <memory>
#include <string>

#include "chunkserver.grpc.pb.h"
#include "master.grpc.pb.h"

namespace {

bool RpcCreateFile(
    const std::string& master_address,
    const std::string& path,
    std::uint32_t replication) {
    const auto channel =
        ::grpc::CreateChannel(
            master_address,
            ::grpc::InsecureChannelCredentials());

    auto stub =
        ::gfs::protocol::MasterService::
            NewStub(channel);

    ::gfs::protocol::CreateFileRequest
        request;

    request.set_path(path);
    request.set_replication_factor(
        replication);

    ::gfs::protocol::CreateFileResponse
        response;

    ::grpc::ClientContext context;

    context.set_deadline(
        std::chrono::system_clock::now() +
        std::chrono::seconds(3));

    const auto status =
        stub->CreateFile(
            &context,
            request,
            &response);

    return status.ok() &&
           response.success();
}

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
    const std::string master_address =
        Value(
            argc,
            argv,
            "--master",
            "127.0.0.1:5000");

    const std::string command =
        Value(
            argc,
            argv,
            "--command",
            "");

    const std::string path =
        Value(
            argc,
            argv,
            "--path",
            "");

    if (command == "create") {
        const auto replication =
            static_cast<std::uint32_t>(
                std::stoul(
                    Value(
                        argc,
                        argv,
                        "--replication",
                        "3")));

        return RpcCreateFile(
                   master_address,
                   path,
                   replication)
                   ? EXIT_SUCCESS
                   : EXIT_FAILURE;
    }

    auto master =
        std::make_unique<
            gfs::client::metadata::MasterClient>(
            master_address);

    auto cache =
        std::make_unique<
            gfs::client::metadata::ChunkLocationCache>();

    auto read_function =
        [](
            const gfs::client::metadata::ChunkLocation&
                location,
            gfs::ChunkHandle handle,
            std::uint64_t offset,
            std::size_t length,
            std::string& data) {
            if (location.address.empty()) {
                return false;
            }

            const auto channel =
                ::grpc::CreateChannel(
                    location.address,
                    ::grpc::InsecureChannelCredentials());

            auto stub =
                ::gfs::protocol::ChunkserverService::
                    NewStub(channel);

            ::gfs::protocol::ReadChunkRequest
                request;

            request.set_chunk_handle(
                handle);

            request.set_chunk_version(1);
            request.set_offset(offset);
            request.set_length(length);

            ::gfs::protocol::ReadChunkResponse
                response;

            ::grpc::ClientContext context;

            context.set_deadline(
                std::chrono::system_clock::now() +
                std::chrono::seconds(3));

            const auto status =
                stub->ReadChunk(
                    &context,
                    request,
                    &response);

            if (!status.ok() ||
                !response.success()) {
                return false;
            }

            data = response.data();

            return true;
        };

    auto write_function =
        [](
            const gfs::client::metadata::ChunkLocation&
                location,
            gfs::ChunkHandle handle,
            std::uint64_t offset,
            const std::string& data) {
            if (location.address.empty()) {
                return false;
            }

            const auto channel =
                ::grpc::CreateChannel(
                    location.address,
                    ::grpc::InsecureChannelCredentials());

            auto stub =
                ::gfs::protocol::ChunkserverService::
                    NewStub(channel);

            ::gfs::protocol::WriteChunkRequest
                request;

            request.set_chunk_handle(
                handle);

            request.set_chunk_version(1);
            request.set_offset(offset);
            request.set_data(data);

            ::gfs::protocol::WriteChunkResponse
                response;

            ::grpc::ClientContext context;

            context.set_deadline(
                std::chrono::system_clock::now() +
                std::chrono::seconds(3));

            const auto status =
                stub->WriteChunk(
                    &context,
                    request,
                    &response);

            return status.ok() &&
                   response.success();
        };

    auto* master_client =
        master.get();

    auto client =
        std::make_unique<
            gfs::client::GFSClient>(
            std::move(master),
            std::move(cache),
            std::move(read_function),
            std::move(write_function),
            [master_client](
                const gfs::FilePath& file_path,
                std::uint64_t size) {
                return master_client
                    ->UpdateFileSize(
                        file_path,
                        size);
            });

    client->SetFileCreateFunction(
        [master_client](
            const gfs::FilePath& file_path) {
            return master_client
                ->CreateFile(
                    file_path,
                    3);
        });

    if (command == "write") {
        const std::string data =
            Value(
                argc,
                argv,
                "--data",
                "");

        if (!client->Write(
                path,
                0,
                data)) {
            return EXIT_FAILURE;
        }

        return EXIT_SUCCESS;
    }

    if (command == "read") {
        const auto length =
            static_cast<std::size_t>(
                std::stoull(
                    Value(
                        argc,
                        argv,
                        "--length",
                        "0")));

        std::string data;

        if (!client->Read(
                path,
                0,
                length,
                data)) {
            return EXIT_FAILURE;
        }

        std::cout << data;

        return EXIT_SUCCESS;
    }

    std::cerr
        << "usage: "
        << "--master ADDR "
        << "--command create|write|read "
        << "--path PATH "
        << "[--data DATA|--length N]\n";

    return EXIT_FAILURE;
}