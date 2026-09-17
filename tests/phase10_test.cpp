#include "gfs/chunkserver/chunkserver.hpp"
#include "gfs/master/master.hpp"
#include "gfs/master/recovery/recovery_manager.hpp"
#include "gfs/master/replication/re_replication.hpp"

#include <gtest/gtest.h>

#include <cstdint>
#include <filesystem>
#include <string>
#include <vector>

namespace {

using gfs::ChunkHandle;
using gfs::ServerId;
using gfs::chunkserver::Chunkserver;
using gfs::master::Master;

class TemporaryDirectory {
public:
    explicit TemporaryDirectory(const std::string& name) {
        path_ = std::filesystem::temp_directory_path() /
                (name +
                 std::to_string(
                     reinterpret_cast<std::uintptr_t>(this)));

        std::filesystem::create_directories(path_);
    }

    ~TemporaryDirectory() {
        std::error_code error;
        std::filesystem::remove_all(path_, error);
    }

    [[nodiscard]] std::string Path() const {
        return path_.string();
    }

private:
    std::filesystem::path path_;
};

struct TestChunkservers {
    TemporaryDirectory directory1{"gfs_phase10_1_"};
    TemporaryDirectory directory2{"gfs_phase10_2_"};
    TemporaryDirectory directory3{"gfs_phase10_3_"};
    TemporaryDirectory directory4{"gfs_phase10_4_"};

    Chunkserver server1{1, directory1.Path()};
    Chunkserver server2{2, directory2.Path()};
    Chunkserver server3{3, directory3.Path()};
    Chunkserver server4{4, directory4.Path()};

    bool Initialize() {
        return server1.Initialize() &&
               server2.Initialize() &&
               server3.Initialize() &&
               server4.Initialize();
    }
};

void ReportAlive(
    Master& master,
    ServerId server_id,
    std::uint64_t timestamp_ms,
    const std::vector<
        gfs::master::heartbeat::ReportedChunk>& chunks) {
    ASSERT_TRUE(
        master.ProcessHeartbeat(
            server_id,
            timestamp_ms,
            chunks));
}

void CreateChunkInMaster(
    Master& master,
    const std::string& path,
    std::uint32_t replication_factor,
    ChunkHandle& handle) {
    ASSERT_TRUE(
        master.DirectoryExists("/data") ||
        master.CreateDirectory("/data"));

    ASSERT_TRUE(
        master.CreateFile(
            path,
            replication_factor));

    const auto allocated =
        master.AllocateChunk(path);

    ASSERT_TRUE(allocated.has_value());

    handle = *allocated;
}

TEST(
    Phase10ReReplicationTest,
    IdentifiesUnderReplicatedChunk) {
    Master master(3);

    ASSERT_TRUE(master.Initialize());

    ChunkHandle handle = 0;

    CreateChunkInMaster(
        master,
        "/data/file",
        3,
        handle);

    TestChunkservers servers;

    ASSERT_TRUE(servers.Initialize());

    ASSERT_TRUE(
        master.GetRecoveryManager()
            .RegisterChunkserver(
                servers.server1));

    ASSERT_TRUE(
        master.GetRecoveryManager()
            .RegisterChunkserver(
                servers.server2));

    ASSERT_TRUE(
        servers.server1.CreateChunk(handle));

    ASSERT_TRUE(
        servers.server2.CreateChunk(handle));

    ASSERT_TRUE(
        master.RegisterChunkReplica(
            handle,
            1,
            true));

    ASSERT_TRUE(
        master.RegisterChunkReplica(
            handle,
            2));

    ReportAlive(
        master,
        1,
        1000,
        {{handle, 1}});

    ReportAlive(
        master,
        2,
        1000,
        {{handle, 1}});

    EXPECT_EQ(
        master.GetReReplicationManager()
            .GetHealthyReplicaCount(
                handle,
                1100),
        2U);

    EXPECT_TRUE(
        master.GetReReplicationManager()
            .NeedsReReplication(
                handle,
                1100));
}

TEST(
    Phase10ReReplicationTest,
    FailedReplicaIsExcludedAndReplaced) {
    Master master(3);

    ASSERT_TRUE(master.Initialize());

    ChunkHandle handle = 0;

    CreateChunkInMaster(
        master,
        "/data/file",
        3,
        handle);

    TestChunkservers servers;

    ASSERT_TRUE(servers.Initialize());

    ASSERT_TRUE(
        master.GetRecoveryManager()
            .RegisterChunkserver(
                servers.server1));

    ASSERT_TRUE(
        master.GetRecoveryManager()
            .RegisterChunkserver(
                servers.server2));

    ASSERT_TRUE(
        master.GetRecoveryManager()
            .RegisterChunkserver(
                servers.server4));

    const std::string data =
        "recoverable chunk";

    ASSERT_TRUE(
        servers.server1.CreateChunk(
            handle));

    ASSERT_TRUE(
        servers.server1.WriteChunk(
            handle,
            0,
            data));

    ASSERT_TRUE(
        servers.server2.CreateChunk(
            handle));

    ASSERT_TRUE(
        servers.server2.WriteChunk(
            handle,
            0,
            data));

    ASSERT_TRUE(
        master.RegisterChunkReplica(
            handle,
            1,
            true));

    ASSERT_TRUE(
        master.RegisterChunkReplica(
            handle,
            2));

    ASSERT_TRUE(
        master.RegisterChunkReplica(
            handle,
            3));

    ReportAlive(
        master,
        1,
        900,
        {{handle, 1}});

    ReportAlive(
        master,
        2,
        900,
        {{handle, 1}});

    ReportAlive(
        master,
        3,
        800,
        {{handle, 1}});

    ReportAlive(
        master,
        4,
        900,
        {});

    master.GetHeartbeatManager()
        .SetFailureTimeoutMs(100);

    ASSERT_TRUE(
        master.GetRecoveryManager()
            .RecoverChunk(
                handle,
                999));

    EXPECT_FALSE(
        master.HasReplica(
            handle,
            3));

    EXPECT_TRUE(
        master.HasReplica(
            handle,
            4));

    EXPECT_EQ(
        master.GetReplicaManager()
            .ReplicaCount(handle),
        3U);

    EXPECT_TRUE(
        servers.server4.ChunkExists(
            handle));

    std::string output;

    ASSERT_TRUE(
        servers.server4.ReadChunk(
            handle,
            0,
            data.size(),
            output));

    EXPECT_EQ(
        output,
        data);
}

TEST(
    Phase10ReReplicationTest,
    StaleReplicaIsNotCountedAndIsRebuilt) {
    Master master(3);

    ASSERT_TRUE(master.Initialize());

    ChunkHandle handle = 0;

    CreateChunkInMaster(
        master,
        "/data/file",
        3,
        handle);

    ASSERT_TRUE(
        master.GetChunkInfo(handle)
            .has_value());

    ASSERT_TRUE(
        master.SetChunkVersion(
            handle,
            2));

    ASSERT_TRUE(
        master.GetReplicaManager()
            .RegisterReplica(
                handle,
                1,
                true));

    ASSERT_TRUE(
        master.GetReplicaManager()
            .RegisterReplica(
                handle,
                2));

    ASSERT_TRUE(
        master.GetReplicaManager()
            .RegisterReplica(
                handle,
                3));

    TestChunkservers servers;

    ASSERT_TRUE(servers.Initialize());

    ASSERT_TRUE(
        master.GetRecoveryManager()
            .RegisterChunkserver(
                servers.server1));

    ASSERT_TRUE(
        master.GetRecoveryManager()
            .RegisterChunkserver(
                servers.server2));

    ASSERT_TRUE(
        master.GetRecoveryManager()
            .RegisterChunkserver(
                servers.server3));

    ASSERT_TRUE(
        master.GetRecoveryManager()
            .RegisterChunkserver(
                servers.server4));

    ASSERT_TRUE(
        servers.server1.CreateChunk(
            handle));

    ASSERT_TRUE(
        servers.server1.WriteChunk(
            handle,
            0,
            "current data"));

    ASSERT_TRUE(
        servers.server2.CreateChunk(
            handle));

    ASSERT_TRUE(
        servers.server2.WriteChunk(
            handle,
            0,
            "stale data"));

    ASSERT_TRUE(
        servers.server3.CreateChunk(
            handle));

    ASSERT_TRUE(
        servers.server3.WriteChunk(
            handle,
            0,
            "current data"));

    ASSERT_TRUE(
        master.GetHeartbeatManager()
            .ProcessHeartbeat(
                1,
                1000,
                {{handle, 2}}));

    ASSERT_TRUE(
        master.GetHeartbeatManager()
            .ProcessHeartbeat(
                2,
                1000,
                {{handle, 1}}));

    ASSERT_TRUE(
        master.GetHeartbeatManager()
            .ProcessHeartbeat(
                3,
                1000,
                {{handle, 2}}));

    ASSERT_TRUE(
        master.GetHeartbeatManager()
            .ProcessHeartbeat(
                4,
                1000,
                {}));

    const auto stale =
        master.GetStaleReplicas(handle);

    ASSERT_EQ(
        stale.size(),
        1U);

    EXPECT_EQ(
        stale.front(),
        2U);

    EXPECT_EQ(
        master.GetRecoveryManager()
            .GetHealthyReplicaCount(
                handle,
                1100),
        2U);

    ASSERT_TRUE(
        master.GetRecoveryManager()
            .RecoverChunk(
                handle,
                1100));

    EXPECT_FALSE(
        master.HasReplica(
            handle,
            2));

    EXPECT_TRUE(
        master.HasReplica(
            handle,
            4));

    EXPECT_EQ(
        master.GetRecoveryManager()
            .GetHealthyReplicaCount(
                handle,
                1100),
        3U);

    std::string output;

    ASSERT_TRUE(
        servers.server4.ReadChunk(
            handle,
            0,
            std::string(
                "current data").size(),
            output));

    EXPECT_EQ(
        output,
        "current data");
}

TEST(
    Phase10RecoveryTest,
    PrimaryFailurePromotesSurvivingReplica) {
    Master master(3);

    ASSERT_TRUE(master.Initialize());

    ChunkHandle handle = 0;

    CreateChunkInMaster(
        master,
        "/data/file",
        3,
        handle);

    TestChunkservers servers;

    ASSERT_TRUE(servers.Initialize());

    for (Chunkserver* server :
         {&servers.server1,
          &servers.server2,
          &servers.server3,
          &servers.server4}) {
        ASSERT_TRUE(
            master.GetRecoveryManager()
                .RegisterChunkserver(
                    *server));
    }

    for (Chunkserver* server :
         {&servers.server1,
          &servers.server2,
          &servers.server3}) {
        ASSERT_TRUE(
            server->CreateChunk(handle));

        ASSERT_TRUE(
            server->WriteChunk(
                handle,
                0,
                "primary recovery"));
    }

    ASSERT_TRUE(
        master.RegisterChunkReplica(
            handle,
            1,
            true));

    ASSERT_TRUE(
        master.RegisterChunkReplica(
            handle,
            2));

    ASSERT_TRUE(
        master.RegisterChunkReplica(
            handle,
            3));

    ReportAlive(
        master,
        1,
        900,
        {{handle, 1}});

    ReportAlive(
        master,
        2,
        950,
        {{handle, 1}});

    ReportAlive(
        master,
        3,
        950,
        {{handle, 1}});

    ReportAlive(
        master,
        4,
        950,
        {});

    master.GetHeartbeatManager()
        .SetFailureTimeoutMs(100);

    ASSERT_TRUE(
        master.GetRecoveryManager()
            .RecoverChunk(
                handle,
                1000));

    const auto primary =
        master.GetPrimary(handle);

    ASSERT_TRUE(
        primary.has_value());

    EXPECT_NE(
        *primary,
        1U);

    EXPECT_TRUE(
        *primary == 2U ||
        *primary == 3U);

    EXPECT_EQ(
        master.GetReplicaManager()
            .ReplicaCount(handle),
        3U);
}

TEST(
    Phase10RecoveryTest,
    InsufficientServersFailsGracefully) {
    Master master(3);

    ASSERT_TRUE(master.Initialize());

    ChunkHandle handle = 0;

    CreateChunkInMaster(
        master,
        "/data/file",
        3,
        handle);

    TestChunkservers servers;

    ASSERT_TRUE(servers.Initialize());

    ASSERT_TRUE(
        master.GetRecoveryManager()
            .RegisterChunkserver(
                servers.server1));

    ASSERT_TRUE(
        servers.server1.CreateChunk(
            handle));

    ASSERT_TRUE(
        servers.server1.WriteChunk(
            handle,
            0,
            "only surviving replica"));

    ASSERT_TRUE(
        master.RegisterChunkReplica(
            handle,
            1,
            true));

    ReportAlive(
        master,
        1,
        1000,
        {{handle, 1}});

    master.GetHeartbeatManager()
        .SetFailureTimeoutMs(100);

    EXPECT_FALSE(
        master.GetRecoveryManager()
            .RecoverChunk(
                handle,
                1050));

    EXPECT_EQ(
        master.GetRecoveryManager()
            .GetHealthyReplicaCount(
                handle,
                1050),
        1U);
}

TEST(
    Phase10RecoveryTest,
    RecoveryDoesNotDuplicateReplicas) {
    Master master(2);

    ASSERT_TRUE(master.Initialize());

    ChunkHandle handle = 0;

    CreateChunkInMaster(
        master,
        "/data/file",
        2,
        handle);

    TestChunkservers servers;

    ASSERT_TRUE(servers.Initialize());

    ASSERT_TRUE(
        master.GetRecoveryManager()
            .RegisterChunkserver(
                servers.server1));

    ASSERT_TRUE(
        master.GetRecoveryManager()
            .RegisterChunkserver(
                servers.server2));

    ASSERT_TRUE(
        master.GetRecoveryManager()
            .RegisterChunkserver(
                servers.server3));

    ASSERT_TRUE(
        servers.server1.CreateChunk(handle));

    ASSERT_TRUE(
        servers.server1.WriteChunk(
            handle,
            0,
            "no duplicates"));

    ASSERT_TRUE(
        servers.server2.CreateChunk(handle));

    ASSERT_TRUE(
        servers.server2.WriteChunk(
            handle,
            0,
            "no duplicates"));

    ASSERT_TRUE(
        master.RegisterChunkReplica(
            handle,
            1,
            true));

    ASSERT_TRUE(
        master.RegisterChunkReplica(
            handle,
            2));

    ReportAlive(
        master,
        1,
        1000,
        {{handle, 1}});

    ReportAlive(
        master,
        2,
        1000,
        {{handle, 1}});

    EXPECT_FALSE(
        master.GetRecoveryManager()
            .NeedsRecovery(
                handle,
                1100));

    EXPECT_TRUE(
        master.GetRecoveryManager()
            .RecoverChunk(
                handle,
                1100));

    EXPECT_EQ(
        master.GetReplicaManager()
            .ReplicaCount(handle),
        2U);
}

TEST(
    Phase10RecoveryTest,
    UnrelatedChunkIsNotModified) {
    Master master(2);

    ASSERT_TRUE(master.Initialize());

    ChunkHandle first = 0;
    ChunkHandle second = 0;

    CreateChunkInMaster(
        master,
        "/data/first",
        2,
        first);

    CreateChunkInMaster(
        master,
        "/data/second",
        2,
        second);

    TestChunkservers servers;

    ASSERT_TRUE(servers.Initialize());

    for (Chunkserver* server :
         {&servers.server1,
          &servers.server2,
          &servers.server3}) {
        ASSERT_TRUE(
            master.GetRecoveryManager()
                .RegisterChunkserver(
                    *server));
    }

    ASSERT_TRUE(
        servers.server1.CreateChunk(first));

    ASSERT_TRUE(
        servers.server1.WriteChunk(
            first,
            0,
            "first"));

    ASSERT_TRUE(
        servers.server2.CreateChunk(first));

    ASSERT_TRUE(
        servers.server2.WriteChunk(
            first,
            0,
            "first"));

    ASSERT_TRUE(
        servers.server1.CreateChunk(second));

    ASSERT_TRUE(
        servers.server1.WriteChunk(
            second,
            0,
            "second"));

    ASSERT_TRUE(
        master.RegisterChunkReplica(
            first,
            1,
            true));

    ASSERT_TRUE(
        master.RegisterChunkReplica(
            first,
            2));

    ASSERT_TRUE(
        master.RegisterChunkReplica(
            second,
            1,
            true));

    ReportAlive(
        master,
        1,
        1000,
        {{first, 1},
         {second, 1}});

    ReportAlive(
        master,
        2,
        1000,
        {{first, 1}});

    ReportAlive(
        master,
        3,
        1000,
        {});

    EXPECT_TRUE(
        master.GetRecoveryManager()
            .RecoverChunk(
                second,
                1100));

    EXPECT_EQ(
        master.GetReplicaManager()
            .ReplicaCount(first),
        2U);

    EXPECT_EQ(
        master.GetReplicaManager()
            .ReplicaCount(second),
        2U);

    EXPECT_TRUE(
        master.HasReplica(
            second,
            3));
}

}  // namespace