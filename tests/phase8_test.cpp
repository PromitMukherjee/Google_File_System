#include "gfs/chunkserver/chunkserver.hpp"
#include "gfs/chunkserver/heartbeat/heartbeat_client.hpp"
#include "gfs/common/types.hpp"
#include "gfs/master/heartbeat/chunkserver_state.hpp"
#include "gfs/master/heartbeat/heartbeat_manager.hpp"
#include "gfs/master/master.hpp"
#include "gfs/protocol/master_service.hpp"

#include <gtest/gtest.h>

#include <cstdint>
#include <filesystem>
#include <string>
#include <vector>

namespace {

using gfs::ChunkHandle;
using gfs::ServerId;
using gfs::chunkserver::Chunkserver;
using gfs::chunkserver::heartbeat::HeartbeatClient;
using gfs::master::Master;
using gfs::master::heartbeat::ChunkserverState;
using gfs::master::heartbeat::HeartbeatManager;
using gfs::master::heartbeat::ReportedChunk;

class TemporaryDirectory {
public:
    TemporaryDirectory() {
        path_ =
            std::filesystem::temp_directory_path() /
            "gfs_phase8_test";

        std::filesystem::create_directories(path_);
    }

    ~TemporaryDirectory() {
        std::error_code error;
        std::filesystem::remove_all(
            path_,
            error);
    }

    [[nodiscard]] std::string Path() const {
        return path_.string();
    }

private:
    std::filesystem::path path_;
};

TEST(ChunkserverStateTest, StoresServerAndHeartbeatState) {
    ChunkserverState state(7);

    EXPECT_EQ(
        state.GetServerId(),
        7U);

    EXPECT_EQ(
        state.GetLastHeartbeatMs(),
        0U);

    EXPECT_FALSE(
        state.IsAlive());

    state.UpdateHeartbeat(1000);

    EXPECT_TRUE(
        state.IsAlive());

    EXPECT_EQ(
        state.GetLastHeartbeatMs(),
        1000U);
}

TEST(ChunkserverStateTest, TracksReportedChunks) {
    ChunkserverState state(7);

    const std::vector<ReportedChunk> chunks{
        {42, 3},
        {10, 1},
        {42, 5}
    };

    state.SetReportedChunks(chunks);

    EXPECT_EQ(
        state.ReportedChunkCount(),
        2U);

    EXPECT_TRUE(
        state.HasReportedChunk(42));

    EXPECT_TRUE(
        state.HasReportedChunk(10));

    EXPECT_FALSE(
        state.HasReportedChunk(99));

    const auto reported =
        state.GetReportedChunks();

    ASSERT_EQ(
        reported.size(),
        2U);

    EXPECT_EQ(
        reported[0].handle,
        10U);

    EXPECT_EQ(
        reported[0].version,
        1U);

    EXPECT_EQ(
        reported[1].handle,
        42U);

    EXPECT_EQ(
        reported[1].version,
        5U);
}

TEST(ChunkserverStateTest, MarkFailedChangesLiveness) {
    ChunkserverState state(7);

    state.UpdateHeartbeat(1000);

    ASSERT_TRUE(
        state.IsAlive());

    state.MarkFailed();

    EXPECT_FALSE(
        state.IsAlive());
}

TEST(HeartbeatManagerTest, AcceptsAndUpdatesHeartbeat) {
    HeartbeatManager manager(100);

    ASSERT_TRUE(
        manager.ProcessHeartbeat(
            1,
            1000,
            {{42, 1}, {43, 2}}));

    EXPECT_TRUE(
        manager.HasServer(1));

    EXPECT_TRUE(
        manager.IsServerAlive(
            1,
            1050));

    const auto timestamp =
        manager.GetLastHeartbeat(1);

    ASSERT_TRUE(
        timestamp.has_value());

    EXPECT_EQ(
        *timestamp,
        1000U);

    const auto state =
        manager.GetServerState(1);

    ASSERT_TRUE(
        state.has_value());

    EXPECT_EQ(
        state->ReportedChunkCount(),
        2U);
}

TEST(HeartbeatManagerTest, RepeatedHeartbeatsKeepServerAlive) {
    HeartbeatManager manager(100);

    ASSERT_TRUE(
        manager.ProcessHeartbeat(
            1,
            1000));

    ASSERT_TRUE(
        manager.ProcessHeartbeat(
            1,
            1050));

    EXPECT_TRUE(
        manager.IsServerAlive(
            1,
            1149));

    EXPECT_EQ(
        manager.GetLastHeartbeat(1).value(),
        1050U);
}

TEST(HeartbeatManagerTest, TimeoutMarksServerFailed) {
    HeartbeatManager manager(100);

    ASSERT_TRUE(
        manager.ProcessHeartbeat(
            1,
            1000));

    const auto failed =
        manager.DetectFailedServers(1100);

    ASSERT_EQ(
        failed.size(),
        1U);

    EXPECT_EQ(
        failed[0],
        1U);

    EXPECT_FALSE(
        manager.IsServerAlive(1));

    EXPECT_EQ(
        manager.GetFailedServers(),
        std::vector<ServerId>{1});
}

TEST(HeartbeatManagerTest, HealthyServerIsNotMarkedFailed) {
    HeartbeatManager manager(100);

    ASSERT_TRUE(
        manager.ProcessHeartbeat(
            1,
            1000));

    const auto failed =
        manager.DetectFailedServers(1099);

    EXPECT_TRUE(
        failed.empty());

    EXPECT_TRUE(
        manager.IsServerAlive(1));

    EXPECT_TRUE(
        manager.GetFailedServers().empty());
}

TEST(HeartbeatManagerTest, MultipleServersAreIndependent) {
    HeartbeatManager manager(100);

    ASSERT_TRUE(
        manager.ProcessHeartbeat(
            1,
            1000));

    ASSERT_TRUE(
        manager.ProcessHeartbeat(
            2,
            1050));

    ASSERT_TRUE(
        manager.ProcessHeartbeat(
            3,
            1080));

    const auto failed =
        manager.DetectFailedServers(1100);

    ASSERT_EQ(
        failed.size(),
        1U);

    EXPECT_EQ(
        failed[0],
        1U);

    EXPECT_FALSE(
        manager.IsServerAlive(
            1));

    EXPECT_TRUE(
        manager.IsServerAlive(
            2));

    EXPECT_TRUE(
        manager.IsServerAlive(
            3));
}

TEST(HeartbeatManagerTest, OlderHeartbeatIsRejected) {
    HeartbeatManager manager(100);

    ASSERT_TRUE(
        manager.ProcessHeartbeat(
            1,
            1000));

    EXPECT_FALSE(
        manager.ProcessHeartbeat(
            1,
            999));

    EXPECT_EQ(
        manager.GetLastHeartbeat(1).value(),
        1000U);
}

TEST(MasterHeartbeatTest, MasterProcessesHeartbeat) {
    Master master;

    ASSERT_TRUE(
        master.ProcessHeartbeat(
            1,
            1000,
            {{42, 1}, {43, 1}}));

    EXPECT_TRUE(
        master.IsChunkserverAlive(
            1,
            1050));

    EXPECT_TRUE(
        master.HasChunkReplica(
            42,
            1));

    EXPECT_TRUE(
        master.HasChunkReplica(
            43,
            1));

    const auto state =
        master.GetHeartbeatManager()
            .GetServerState(1);

    ASSERT_TRUE(
        state.has_value());

    EXPECT_EQ(
        state->ReportedChunkCount(),
        2U);
}

TEST(MasterHeartbeatTest, MasterDetectsFailedChunkserver) {
    Master master;

    master.GetHeartbeatManager()
        .SetFailureTimeoutMs(100);

    ASSERT_TRUE(
        master.ProcessHeartbeat(
            1,
            1000));

    const auto failed =
        master.DetectFailedChunkservers(1100);

    ASSERT_EQ(
        failed.size(),
        1U);

    EXPECT_EQ(
        failed[0],
        1U);

    EXPECT_FALSE(
        master.IsChunkserverAlive(1));
}

TEST(HeartbeatClientTest, BuildsHeartbeatFromChunkserverStorage) {
    TemporaryDirectory directory;

    Chunkserver server(
        7,
        directory.Path());

    ASSERT_TRUE(
        server.Initialize());

    ASSERT_TRUE(
        server.CreateChunk(42));

    ASSERT_TRUE(
        server.CreateChunk(10));

    HeartbeatClient client(server);

    const auto request =
        client.BuildHeartbeatRequest(5000);

    EXPECT_EQ(
        request.server_id(),
        7U);

    EXPECT_EQ(
        request.timestamp_ms(),
        5000U);

    ASSERT_EQ(
        request.chunks_size(),
        2);

    EXPECT_EQ(
        request.chunks(0).chunk_handle(),
        10U);

    EXPECT_EQ(
        request.chunks(0).version(),
        1U);

    EXPECT_EQ(
        request.chunks(1).chunk_handle(),
        42U);

    EXPECT_EQ(
        request.chunks(1).version(),
        1U);
}

TEST(MasterServiceHeartbeatTest, ProcessesHeartbeatRPC) {
    Master master;

    ::gfs::protocol::MasterServiceImpl service(master);

    ::gfs::protocol::HeartbeatRequest request;
    request.set_server_id(7);
    request.set_timestamp_ms(5000);

    auto* chunk = request.add_chunks();
    chunk->set_chunk_handle(42);
    chunk->set_version(1);

    ::gfs::protocol::HeartbeatResponse response;

    ::grpc::ServerContext context;

    const auto status =
        service.Heartbeat(
            &context,
            &request,
            &response);

    EXPECT_TRUE(
        status.ok());

    EXPECT_TRUE(
        response.success());

    EXPECT_GT(
        response.master_timestamp_ms(),
        0U);

    EXPECT_TRUE(
        master.IsChunkserverAlive(
            7,
            5050));

    EXPECT_TRUE(
        master.HasChunkReplica(
            42,
            7));
}

TEST(MasterServiceHeartbeatTest, RejectsInvalidServerId) {
    Master master;

    ::gfs::protocol::MasterServiceImpl service(master);

    ::gfs::protocol::HeartbeatRequest request;
    request.set_server_id(0);
    request.set_timestamp_ms(5000);

    ::gfs::protocol::HeartbeatResponse response;

    ::grpc::ServerContext context;

    const auto status =
        service.Heartbeat(
            &context,
            &request,
            &response);

    EXPECT_TRUE(
        status.ok());

    EXPECT_FALSE(
        response.success());

    EXPECT_FALSE(
        response.error_message().empty());
}

TEST(MasterServiceHeartbeatTest, RejectsInvalidChunkReport) {
    Master master;

    ::gfs::protocol::MasterServiceImpl service(master);

    ::gfs::protocol::HeartbeatRequest request;
    request.set_server_id(7);
    request.set_timestamp_ms(5000);

    auto* chunk = request.add_chunks();
    chunk->set_chunk_handle(42);
    chunk->set_version(0);

    ::gfs::protocol::HeartbeatResponse response;

    ::grpc::ServerContext context;

    const auto status =
        service.Heartbeat(
            &context,
            &request,
            &response);

    EXPECT_TRUE(
        status.ok());

    EXPECT_FALSE(
        response.success());

    EXPECT_FALSE(
        response.error_message().empty());
}

}  // namespace