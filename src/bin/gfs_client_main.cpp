#include "gfs/client/cli.hpp"
#include "gfs/client/gfs_client.hpp"
#include "gfs/client/metadata/chunk_location_cache.hpp"
#include "gfs/client/metadata/master_client.hpp"
#include "gfs/common/constants.hpp"

#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <grpcpp/grpcpp.h>
#include <iostream>
#include <memory>
#include <string>
#include <vector>

#include "chunkserver.grpc.pb.h"
#include "master.grpc.pb.h"

namespace {

std::string Value(
    int argc,
    char** argv,
    const std::string& name,
    std::string fallback) {
    for (int i = 1;
         i + 1 < argc;
         ++i) {
        if (std::string(argv[i]) == name) {
            return argv[i + 1];
        }
    }

    return fallback;
}

bool HasFlag(
    int argc,
    char** argv,
    const std::string& flag) {
    for (int i = 1; i < argc; ++i) {
        if (std::string(argv[i]) == flag) {
            return true;
        }
    }

    return false;
}

void PrintStartupHelp() {
    std::cout
        << "GFS Client\n\n"
        << "Usage:\n"
        << "  gfs-client --master ADDRESS\n"
        << "  gfs-client --master ADDRESS "
           "--command create|write|read ...\n\n"
        << "Options:\n"
        << "  --master ADDRESS       Master gRPC address "
           "(default: 127.0.0.1:5000)\n"
        << "  --command COMMAND      Run one legacy "
           "non-interactive command\n"
        << "  --path PATH            Path for the command\n"
        << "  --data DATA            Data for write\n"
        << "  --length N             Read length\n"
        << "  --replication N        Create-file "
           "replication factor\n"
        << "  --help                 Show this help\n\n"
        << "Without --command, the interactive GFS REPL "
           "is started.\n";
}

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

    ::gfs::protocol::CreateFileRequest request;
    request.set_path(path);
    request.set_replication_factor(
        replication);

    ::gfs::protocol::CreateFileResponse response;
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

}  // namespace

int main(
    int argc,
    char** argv) {
    if (HasFlag(
            argc,
            argv,
            "--help") ||
        HasFlag(
            argc,
            argv,
            "-h")) {
        PrintStartupHelp();
        return EXIT_SUCCESS;
    }

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
        try {
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
        } catch (...) {
            std::cerr
                << "Error: invalid replication factor\n";
            return EXIT_FAILURE;
        }
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

            ::gfs::protocol::ReadChunkRequest request;

            request.set_chunk_handle(handle);
            request.set_chunk_version(1);
            request.set_offset(offset);
            request.set_length(length);

            ::gfs::protocol::ReadChunkResponse response;
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

            ::gfs::protocol::WriteChunkRequest request;

            request.set_chunk_handle(handle);
            request.set_chunk_version(1);
            request.set_offset(offset);
            request.set_data(data);

            ::gfs::protocol::WriteChunkResponse response;
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

    std::string append_path;

    client->SetRecordAppendFunction(
        [master_client,
         &append_path](
            const gfs::client::metadata::ChunkLocation&
                primary,
            gfs::ChunkHandle handle,
            gfs::ChunkVersion version,
            const std::vector<
                gfs::client::metadata::ChunkLocation>&
                replicas,
            const std::string& data)
            -> gfs::client::io::RecordAppender::AppendResult {
            using AppendResult =
                gfs::client::io::RecordAppender::AppendResult;

            using AppendStatus =
                gfs::client::io::RecordAppender::AppendStatus;

            if (append_path.empty() ||
                primary.address.empty() ||
                handle == 0 ||
                data.empty()) {
                return {
                    AppendStatus::Failed,
                    0,
                    0,
                    0};
            }

            const auto file =
                master_client->LookupFile(
                    append_path);

            if (!file.has_value()) {
                return {
                    AppendStatus::Failed,
                    0,
                    0,
                    0};
            }

            std::size_t chunk_index = 0;
            bool found = false;

            for (std::size_t index = 0;
                 index < file->chunk_handles.size();
                 ++index) {
                if (file->chunk_handles[index] ==
                    handle) {
                    chunk_index = index;
                    found = true;
                    break;
                }
            }

            if (!found) {
                return {
                    AppendStatus::Failed,
                    0,
                    0,
                    0};
            }

            const auto chunk_base =
                static_cast<std::uint64_t>(
                    chunk_index) *
                static_cast<std::uint64_t>(
                    gfs::constants::kChunkSize);

            const auto chunk_offset =
                file->size >= chunk_base
                    ? file->size - chunk_base
                    : 0;

            if (chunk_offset >
                    gfs::constants::kChunkSize ||
                data.size() >
                    gfs::constants::kChunkSize -
                        chunk_offset) {
                return {
                    AppendStatus::RetryNextChunk,
                    chunk_offset,
                    chunk_offset,
                    0};
            }

            const auto write_to_replica =
                [&](const gfs::client::metadata::ChunkLocation&
                        location) {
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

                    ::gfs::protocol::WriteChunkRequest request;

                    request.set_chunk_handle(handle);
                    request.set_chunk_version(version);
                    request.set_offset(chunk_offset);
                    request.set_data(data);

                    ::gfs::protocol::WriteChunkResponse response;
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

            if (!write_to_replica(primary)) {
                return {
                    AppendStatus::Failed,
                    chunk_offset,
                    chunk_offset,
                    0};
            }

            for (const auto& replica :
                 replicas) {
                if (replica.server_id ==
                    primary.server_id) {
                    continue;
                }

                if (!write_to_replica(replica)) {
                    return {
                        AppendStatus::Failed,
                        chunk_offset,
                        chunk_offset,
                        0};
                }
            }

            const auto new_chunk_size =
                chunk_offset +
                static_cast<std::uint64_t>(
                    data.size());

            return AppendResult{
                AppendStatus::Success,
                chunk_offset,
                new_chunk_size,
                data.size()};
        });

    client->SetRecordAppendFileSizeUpdater(
        [master_client](
            const gfs::FilePath& file_path,
            std::uint64_t size) {
            return master_client
                ->UpdateFileSize(
                    file_path,
                    size);
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
        try {
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
        } catch (...) {
            return EXIT_FAILURE;
        }
    }

    if (!command.empty()) {
        std::cerr
            << "usage: "
            << "--master ADDR "
            << "--command create|write|read "
            << "--path PATH "
            << "[--data DATA|--length N]\n";

        return EXIT_FAILURE;
    }

    gfs::client::cli::CLI cli(*client);

    return cli.Run(
        std::cin,
        std::cout);
}