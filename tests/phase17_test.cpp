#include "gfs/client/gfs_client.hpp"
#include "gfs/client/metadata/chunk_location_cache.hpp"
#include "gfs/client/metadata/master_client.hpp"
#include "gfs/common/utils.hpp"
#include "gfs/network/chunkserver_server.hpp"
#include "gfs/network/master_server.hpp"

#include <gtest/gtest.h>

#include <algorithm>
#include <chrono>
#include <filesystem>
#include <grpcpp/grpcpp.h>
#include <memory>
#include <string>
#include <thread>
#include <vector>

namespace {

using namespace std::chrono_literals;

using gfs::ChunkHandle;
using gfs::FilePath;
using gfs::client::GFSClient;
using gfs::client::metadata::ChunkLocation;
using gfs::client::metadata::ChunkLocationCache;
using gfs::client::metadata::MasterClient;
using gfs::network::ChunkserverServer;
using gfs::network::MasterServer;

class NetworkCluster {
public:
    NetworkCluster()
        : root_(
              std::filesystem::temp_directory_path() /
              ("gfs_phase17_" +
               std::to_string(
                   gfs::UnixTimeMillis()))) {
    }

    ~NetworkCluster() {
        Stop();

        std::error_code error;

        std::filesystem::remove_all(
            root_,
            error);
    }

    bool Start() {
        std::filesystem::create_directories(
            root_);

        master_ =
            std::make_unique<
                MasterServer>(
                "127.0.0.1",
                0,
                2,
                root_ / "master");

        if (!master_->Start()) {
            return false;
        }

        master_->GetMaster()
            .GetHeartbeatManager()
            .SetFailureTimeoutMs(500);

        for (gfs::ServerId id = 1;
             id <= 3;
             ++id) {
            auto server =
                std::make_unique<
                    ChunkserverServer>(
                    id,
                    "127.0.0.1",
                    0,
                    master_->GetAddress(),
                    (root_ /
                     ("chunkserver_" +
                      std::to_string(id)))
                        .string());

            if (!server->Start()) {
                return false;
            }

            chunkservers_.push_back(
                std::move(server));
        }

        for (int attempt = 0;
             attempt < 30;
             ++attempt) {
            if (master_->GetMaster()
                    .GetHeartbeatManager()
                    .ServerCount() == 3) {
                return true;
            }

            std::this_thread::sleep_for(
                100ms);
        }

        return false;
    }

    void StopChunkserver(
        std::size_t index) {
        if (index < chunkservers_.size() &&
            chunkservers_[index]) {
            chunkservers_[index]->Shutdown();
        }
    }

    void Stop() {
        for (auto& server :
             chunkservers_) {
            if (server) {
                server->Shutdown();
            }
        }

        chunkservers_.clear();

        if (master_) {
            master_->Shutdown();
            master_.reset();
        }
    }

    MasterServer& Master() {
        return *master_;
    }

    ChunkserverServer& Chunkserver(
        std::size_t index) {
        return *chunkservers_[index];
    }

private:
    std::filesystem::path root_;

    std::unique_ptr<
        MasterServer>
        master_;

    std::vector<
        std::unique_ptr<
            ChunkserverServer>>
        chunkservers_;
};

std::unique_ptr<GFSClient>
MakeClient(
    const std::string& master_address) {
    auto master =
        std::make_unique<
            MasterClient>(
            master_address);

    auto cache =
        std::make_unique<
            ChunkLocationCache>();

    auto read =
        [](
            const ChunkLocation& location,
            ChunkHandle handle,
            std::uint64_t offset,
            std::size_t length,
            std::string& data) {
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
                2s);

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

    auto write =
        [](
            const ChunkLocation& location,
            ChunkHandle handle,
            std::uint64_t offset,
            const std::string& data) {
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
                2s);

            const auto status =
                stub->WriteChunk(
                    &context,
                    request,
                    &response);

            return status.ok() &&
                   response.success();
        };

    auto* master_ptr =
        master.get();

    auto client =
        std::make_unique<GFSClient>(
            std::move(master),
            std::move(cache),
            std::move(read),
            std::move(write),
            [master_ptr](
                const FilePath& path,
                std::uint64_t size) {
                return master_ptr
                    ->UpdateFileSize(
                        path,
                        size);
            });

    client->SetFileCreateFunction(
        [master_ptr](
            const FilePath& path) {
            return master_ptr
                ->CreateFile(
                    path,
                    2);
        });

    return client;
}

}  // namespace

TEST(
    Phase17NetworkTest,
    MasterStartsAndAcceptsRpc) {
    NetworkCluster cluster;

    ASSERT_TRUE(
        cluster.Start());

    MasterClient client(
        cluster.Master()
            .GetAddress());

    ASSERT_TRUE(
        client.CreateFile(
            "/rpc-file",
            2));

    const auto file =
        client.LookupFile(
            "/rpc-file");

    ASSERT_TRUE(
        file.has_value());

    EXPECT_EQ(
        file->path,
        "/rpc-file");
}

TEST(
    Phase17NetworkTest,
    ChunkserverRegistersAndChunkRoundTripsOverGrpc) {
    NetworkCluster cluster;

    ASSERT_TRUE(
        cluster.Start());

    auto client =
        MakeClient(
            cluster.Master()
                .GetAddress());

    ASSERT_TRUE(
        client->CreateFile(
            "/data"));

    const std::string payload =
        "network-data";

    ASSERT_TRUE(
        client->Write(
            "/data",
            0,
            payload));

    std::string output;

    ASSERT_TRUE(
        client->Read(
            "/data",
            0,
            payload.size(),
            output));

    EXPECT_EQ(
        output,
        payload);
}

TEST(
    Phase17NetworkTest,
    MultipleChunkserversAreVisibleToMaster) {
    NetworkCluster cluster;

    ASSERT_TRUE(
        cluster.Start());

    EXPECT_EQ(
        cluster.Master()
            .GetMaster()
            .GetHeartbeatManager()
            .ServerCount(),
        3U);

    EXPECT_TRUE(
        cluster.Master()
            .GetMaster()
            .GetReReplicationManager()
            .GetChunkserverEndpoint(1)
            .has_value());

    EXPECT_TRUE(
        cluster.Master()
            .GetMaster()
            .GetReReplicationManager()
            .GetChunkserverEndpoint(2)
            .has_value());

    EXPECT_TRUE(
        cluster.Master()
            .GetMaster()
            .GetReReplicationManager()
            .GetChunkserverEndpoint(3)
            .has_value());
}

TEST(
    Phase17NetworkTest,
    AllocationAndReplicaTransferUseNetwork) {
    NetworkCluster cluster;

    ASSERT_TRUE(
        cluster.Start());

    MasterClient client(
        cluster.Master()
            .GetAddress());

    ASSERT_TRUE(
        client.CreateFile(
            "/replicated",
            2));

    auto chunk =
        client.AllocateChunk(
            "/replicated",
            0);

    ASSERT_TRUE(
        chunk.has_value());

    ASSERT_EQ(
        chunk->locations.size(),
        2U);

    const auto source =
        chunk->locations.front()
            .server_id;

    const auto destination =
        chunk->locations.back()
            .server_id;

    ASSERT_NE(
        source,
        destination);

    const std::string payload =
        "replica-payload";

    const std::size_t source_index =
        static_cast<std::size_t>(
            source - 1U);

    const std::size_t destination_index =
        static_cast<std::size_t>(
            destination - 1U);

    ASSERT_TRUE(
        cluster.Chunkserver(
                source_index)
            .GetChunkserver()
            .WriteChunk(
                chunk->handle,
                0,
                payload));

    ASSERT_TRUE(
        cluster.Master()
            .GetMaster()
            .GetReReplicationManager()
            .TransferReplica(
                chunk->handle,
                source,
                destination));

    std::string data;

    ASSERT_TRUE(
        cluster.Chunkserver(
                destination_index)
            .GetChunkserver()
            .ReadChunk(
                chunk->handle,
                0,
                payload.size(),
                data));

    EXPECT_EQ(
        data,
        payload);
}

TEST(
    Phase17NetworkTest,
    HeartbeatFailureDetectionAndRereplicationWork) {
    NetworkCluster cluster;

    ASSERT_TRUE(
        cluster.Start());

    MasterClient client(
        cluster.Master()
            .GetAddress());

    ASSERT_TRUE(
        client.CreateFile(
            "/failure",
            2));

    const auto chunk =
        client.AllocateChunk(
            "/failure",
            0);

    ASSERT_TRUE(
        chunk.has_value());

    ASSERT_EQ(
        chunk->locations.size(),
        2U);

    const gfs::ServerId failed_id =
        chunk->locations.front()
            .server_id;

    const std::size_t failed_index =
        failed_id - 1U;

    cluster.StopChunkserver(
        failed_index);

    std::this_thread::sleep_for(
        900ms);

    const auto failed =
        cluster.Master()
            .GetMaster()
            .DetectFailedChunkservers(
                gfs::UnixTimeMillis());

    EXPECT_NE(
        std::find(
            failed.begin(),
            failed.end(),
            failed_id),
        failed.end());

    ASSERT_TRUE(
        cluster.Master()
            .GetMaster()
            .GetRecoveryManager()
            .RecoverChunk(
                chunk->handle,
                gfs::UnixTimeMillis()));

    EXPECT_GE(
        cluster.Master()
            .GetMaster()
            .GetReplicaServers(
                chunk->handle)
            .size(),
        2U);
}

TEST(
    Phase17NetworkTest,
    ClientReadsFromSecondReplicaWhenFirstIsUnavailable) {
    NetworkCluster cluster;

    ASSERT_TRUE(
        cluster.Start());

    auto client =
        MakeClient(
            cluster.Master()
                .GetAddress());

    ASSERT_TRUE(
        client->CreateFile(
            "/retry"));

    const std::string payload =
        "retry-payload";

    ASSERT_TRUE(
        client->Write(
            "/retry",
            0,
            payload));

    const auto metadata =
        client->LookupChunk(
            "/retry",
            0);

    ASSERT_TRUE(
        metadata.has_value());

    ASSERT_GE(
        metadata->locations.size(),
        2U);

    const auto failed_id =
        metadata->locations.front()
            .server_id;

    const auto surviving_id =
        metadata->locations.back()
            .server_id;

    ASSERT_TRUE(
        cluster.Master()
            .GetMaster()
            .GetReReplicationManager()
            .TransferReplica(
                metadata->handle,
                failed_id,
                surviving_id));

    cluster.StopChunkserver(
        failed_id - 1U);

    std::string output;

    ASSERT_TRUE(
        client->Read(
            "/retry",
            0,
            payload.size(),
            output));

    EXPECT_EQ(
        output,
        payload);
}