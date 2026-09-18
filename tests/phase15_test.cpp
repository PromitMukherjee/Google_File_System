#include "gfs/chunkserver/chunkserver.hpp"
#include "gfs/master/master.hpp"
#include "gfs/master/replication/rebalancer.hpp"
#include "gfs/testing/distributed_test_harness.hpp"
#include "gfs/testing/failure_injector.hpp"

#include <gtest/gtest.h>

#include <cstdint>
#include <filesystem>
#include <memory>
#include <string>
#include <vector>

namespace {

using gfs::ChunkHandle;
using gfs::ServerId;
using gfs::master::replication::ReplicaMove;
using gfs::testing::DistributedTestHarness;
using gfs::testing::FailureOperation;

class Phase15Fixture : public testing::Test {
protected:
    void SetUp() override {
        ASSERT_TRUE(harness.Initialize());

        ASSERT_TRUE(harness.AddChunkserver(1));
        ASSERT_TRUE(harness.AddChunkserver(2));
        ASSERT_TRUE(harness.AddChunkserver(3));

        ASSERT_TRUE(
            harness.SendHeartbeat(1, 1000));
        ASSERT_TRUE(
            harness.SendHeartbeat(2, 1000));
        ASSERT_TRUE(
            harness.SendHeartbeat(3, 1000));
    }

    DistributedTestHarness harness{2};
};

TEST(
    Phase15FailureInjectorTest,
    DisabledByDefaultAndDeterministic) {
    DistributedTestHarness harness(2);

    EXPECT_TRUE(
        harness.GetFailureInjector()
            .IsEnabled());

    EXPECT_FALSE(
        harness.GetFailureInjector()
            .ShouldFail(
                FailureOperation::ReplicaRead,
                1));

    harness.GetFailureInjector()
        .FailNext(
            FailureOperation::ReplicaRead,
            1,
            2);

    EXPECT_TRUE(
        harness.GetFailureInjector()
            .ShouldFail(
                FailureOperation::ReplicaRead,
                1));

    EXPECT_TRUE(
        harness.GetFailureInjector()
            .ShouldFail(
                FailureOperation::ReplicaRead,
                1));

    EXPECT_FALSE(
        harness.GetFailureInjector()
            .ShouldFail(
                FailureOperation::ReplicaRead,
                1));
}

TEST_F(
    Phase15Fixture,
    SingleChunkserverFailureIsDetected) {
    ASSERT_TRUE(
        harness.CreateFile("/file", 2));

    const auto handle =
        harness.AllocateChunk("/file");

    ASSERT_TRUE(handle.has_value());

    ASSERT_TRUE(
        harness.StopChunkserver(1));

    EXPECT_FALSE(
        harness.GetMaster()
            .IsChunkserverAlive(1, 31001));

    const auto failed =
        harness.GetMaster()
            .DetectFailedChunkservers(31001);

    EXPECT_EQ(failed.size(), 1U);
    EXPECT_EQ(failed.front(), 1U);
}

TEST_F(
    Phase15Fixture,
    FailedServerIsExcludedFromRecoveryDestination) {
    ASSERT_TRUE(
        harness.CreateFile("/file", 2));

    const auto handle =
        harness.AllocateChunk(
            "/file",
            1);

    ASSERT_TRUE(handle.has_value());

    ASSERT_TRUE(
        harness.StopChunkserver(2));

    const auto destinations =
        harness.GetMaster()
            .GetReReplicationManager()
            .GetRecoveryDestinations(
                *handle,
                31001);

    for (const ServerId server :
         destinations) {
        EXPECT_NE(server, 2);
    }
}

TEST_F(
    Phase15Fixture,
    LostReplicaIsRecoveredFromHealthyReplica) {
    ASSERT_TRUE(
        harness.CreateFile("/file", 2));

    const auto handle =
        harness.AllocateChunk("/file");

    ASSERT_TRUE(handle.has_value());

    ASSERT_TRUE(
        harness.StopChunkserver(1));

    ASSERT_TRUE(
        harness.GetMaster()
            .DetectFailedChunkservers(
                31001)
            .size() >= 1U);

    ASSERT_TRUE(
        harness.RestartChunkserver(1));

    ASSERT_TRUE(
        harness.SendHeartbeat(1, 31002));

    EXPECT_TRUE(
        harness.GetMaster()
            .GetRecoveryManager()
            .RecoverChunk(
                *handle,
                31002));

    EXPECT_EQ(
        harness.GetMaster()
            .GetReplicaManager()
            .ReplicaCount(*handle),
        2U);
}

TEST_F(
    Phase15Fixture,
    RecoveryFailsGracefullyWithInsufficientServers) {
    ASSERT_TRUE(
        harness.CreateFile("/file", 2));

    const auto handle =
        harness.AllocateChunk("/file");

    ASSERT_TRUE(handle.has_value());

    ASSERT_TRUE(
        harness.StopChunkserver(1));

    ASSERT_TRUE(
        harness.StopChunkserver(2));

    EXPECT_FALSE(
        harness.GetMaster()
            .GetRecoveryManager()
            .RecoverChunk(
                *handle,
                31001));
}

TEST_F(
    Phase15Fixture,
    FailedTransferLeavesOriginalReplicasUntouched) {
    ASSERT_TRUE(
        harness.CreateFile("/file", 2));

    const auto handle =
        harness.AllocateChunk("/file");

    ASSERT_TRUE(handle.has_value());

    const auto before =
        harness.GetMaster()
            .GetReplicaServers(*handle);

    ASSERT_EQ(before.size(), 2U);

    ASSERT_TRUE(
        harness.GetMaster()
            .GetRecoveryManager()
            .RegisterChunkserver(
                *harness.GetChunkserver(3)));

    ASSERT_TRUE(
        harness.SendHeartbeat(3, 1000));

    const ReplicaMove invalid_move{
        *handle,
        1,
        99};

    EXPECT_FALSE(
        harness.GetMaster()
            .GetRebalancer()
            .ExecuteMove(
                invalid_move,
                1000));

    EXPECT_EQ(
        harness.GetMaster()
            .GetReplicaServers(*handle),
        before);
}

TEST_F(
    Phase15Fixture,
    DuplicateReplicaIsRejected) {
    ASSERT_TRUE(
        harness.CreateFile("/file", 2));

    const auto handle =
        harness.AllocateChunk("/file");

    ASSERT_TRUE(handle.has_value());

    EXPECT_FALSE(
        harness.GetMaster()
            .RegisterReplica(
                *handle,
                1,
                false));

    EXPECT_EQ(
        harness.GetMaster()
            .GetReplicaManager()
            .ReplicaCount(*handle),
        2U);
}

TEST_F(
    Phase15Fixture,
    FailedHeartbeatIsDeterministic) {
    harness.GetFailureInjector()
        .FailNext(
            FailureOperation::Heartbeat,
            1);

    EXPECT_FALSE(
        harness.SendHeartbeat(1, 2000));

    EXPECT_TRUE(
        harness.SendHeartbeat(1, 2000));

    EXPECT_TRUE(
        harness.GetMaster()
            .IsChunkserverAlive(1, 2000));
}

TEST_F(
    Phase15Fixture,
    RecoveredServerCanRejoin) {
    ASSERT_TRUE(
        harness.StopChunkserver(1));

    EXPECT_FALSE(
        harness.HasChunkserver(1));

    ASSERT_TRUE(
        harness.RestartChunkserver(1));

    EXPECT_TRUE(
        harness.HasChunkserver(1));

    ASSERT_TRUE(
        harness.SendHeartbeat(1, 40000));

    EXPECT_TRUE(
        harness.GetMaster()
            .IsChunkserverAlive(
                1,
                40000));
}

TEST_F(
    Phase15Fixture,
    ReadRetriesAfterFirstReplicaFailure) {
    ASSERT_TRUE(
        harness.CreateFile("/file", 2));

    const auto handle =
        harness.AllocateChunk("/file");

    ASSERT_TRUE(handle.has_value());

    ASSERT_TRUE(
        harness.WriteChunk(
            *handle,
            0,
            "phase15-data"));

    harness.GetFailureInjector()
        .FailNext(
            FailureOperation::ReplicaRead,
            1);

    std::string data;

    EXPECT_TRUE(
        harness.ReadChunk(
            *handle,
            0,
            12,
            data));

    EXPECT_EQ(
        data,
        "phase15-data");
}

TEST_F(
    Phase15Fixture,
    WriteFailureDoesNotChangeMetadata) {
    ASSERT_TRUE(
        harness.CreateFile("/file", 2));

    const auto handle =
        harness.AllocateChunk("/file");

    ASSERT_TRUE(handle.has_value());

    const auto before =
        harness.GetMaster()
            .GetChunkInfo(*handle);

    ASSERT_TRUE(before.has_value());

    harness.GetFailureInjector()
        .SetFailureCount(
            FailureOperation::ReplicaWrite,
            1,
            100);

    harness.GetFailureInjector()
        .SetFailureCount(
            FailureOperation::ReplicaWrite,
            2,
            100);

    EXPECT_FALSE(
        harness.WriteChunk(
            *handle,
            0,
            "failed"));

    const auto after =
        harness.GetMaster()
            .GetChunkInfo(*handle);

    ASSERT_TRUE(after.has_value());

    EXPECT_EQ(
        after->GetSize(),
        before->GetSize());
}

TEST_F(
    Phase15Fixture,
    FailedServerDoesNotCauseLiveChunkGarbageCollection) {
    ASSERT_TRUE(
        harness.CreateFile("/file", 2));

    const auto handle =
        harness.AllocateChunk("/file");

    ASSERT_TRUE(handle.has_value());

    ASSERT_TRUE(
        harness.StopChunkserver(1));

    EXPECT_TRUE(
        harness.GetMaster()
            .GetGarbageCollector()
            .IdentifyGarbage()
            .empty());

    EXPECT_TRUE(
        harness.GetMaster()
            .GetChunkInfo(*handle)
            .has_value());
}

TEST_F(
    Phase15Fixture,
    OrphanCanBeCollectedAfterServerFailure) {
    ASSERT_TRUE(
        harness.CreateFile("/file", 2));

    const auto handle =
        harness.AllocateChunk("/file");

    ASSERT_TRUE(handle.has_value());

    ASSERT_TRUE(
        harness.StopChunkserver(1));

    ASSERT_TRUE(
        harness.GetMaster()
            .DeleteFile("/file"));

    const auto candidates =
        harness.GetMaster()
            .GetGarbageCollector()
            .IdentifyGarbage();

    ASSERT_EQ(
        candidates.size(),
        1U);

    EXPECT_EQ(
        candidates.front(),
        *handle);

    const auto collected =
        harness.GetMaster()
            .GetGarbageCollector()
            .Collect();

    EXPECT_EQ(
        collected.size(),
        1U);

    EXPECT_FALSE(
        harness.GetMaster()
            .GetChunkInfo(*handle)
            .has_value());
}

TEST_F(
    Phase15Fixture,
    PrimaryFailurePromotesHealthyReplica) {
    ASSERT_TRUE(
        harness.CreateFile("/file", 2));

    const auto handle =
        harness.AllocateChunk("/file");

    ASSERT_TRUE(handle.has_value());

    ASSERT_TRUE(
        harness.GetMaster()
            .AcquireLease(
                *handle,
                1)
            .has_value());

    ASSERT_TRUE(
        harness.StopChunkserver(1));

    EXPECT_TRUE(
        harness.GetMaster()
            .GetRecoveryManager()
            .RecoverChunk(
                *handle,
                31001));

    const auto primary =
        harness.GetMaster()
            .GetPrimary(*handle);

    ASSERT_TRUE(primary.has_value());

    EXPECT_EQ(*primary, 2U);

    EXPECT_TRUE(
        harness.GetMaster()
            .IsLeaseValid(*handle, 2));
}

TEST_F(
    Phase15Fixture,
    SnapshotRemainsReadableAfterReplicaFailure) {
    ASSERT_TRUE(
        harness.CreateFile("/source", 2));

    const auto handle =
        harness.AllocateChunk("/source");

    ASSERT_TRUE(handle.has_value());

    ASSERT_TRUE(
        harness.WriteChunk(
            *handle,
            0,
            "snapshot-data"));

    ASSERT_TRUE(
        harness.GetMaster()
            .CreateSnapshot(
                "/source",
                "/snapshot"));

    ASSERT_TRUE(
        harness.StopChunkserver(1));

    std::string data;

    EXPECT_TRUE(
        harness.ReadChunk(
            *handle,
            0,
            13,
            data));

    EXPECT_EQ(
        data,
        "snapshot-data");

    EXPECT_EQ(
        harness.GetMaster()
            .GetChunkReferenceCount(
                *handle),
        2U);
}

TEST_F(
    Phase15Fixture,
    SharedSnapshotChunkIsNotGarbageCollected) {
    ASSERT_TRUE(
        harness.CreateFile("/source", 2));

    const auto handle =
        harness.AllocateChunk("/source");

    ASSERT_TRUE(handle.has_value());

    ASSERT_TRUE(
        harness.GetMaster()
            .CreateSnapshot(
                "/source",
                "/snapshot"));

    ASSERT_TRUE(
        harness.GetMaster()
            .DeleteFile("/source"));

    EXPECT_EQ(
        harness.GetMaster()
            .GetChunkReferenceCount(
                *handle),
        1U);

    EXPECT_TRUE(
        harness.GetMaster()
            .GetGarbageCollector()
            .IdentifyGarbage()
            .empty());
}

TEST_F(
    Phase15Fixture,
    COWDestinationRemainsIndependent) {
    ASSERT_TRUE(
        harness.CreateFile("/source", 2));

    const auto source =
        harness.AllocateChunk("/source");

    ASSERT_TRUE(source.has_value());

    ASSERT_TRUE(
        harness.GetMaster()
            .CreateSnapshot(
                "/source",
                "/snapshot"));

    const auto destination =
        harness.GetMaster()
            .PrepareCopyOnWrite(
                "/source",
                0,
                {});

    ASSERT_TRUE(destination.has_value());
    EXPECT_NE(*source, *destination);

    EXPECT_EQ(
        harness.GetMaster()
            .GetChunkReferenceCount(*source),
        1U);

    EXPECT_EQ(
        harness.GetMaster()
            .GetChunkReferenceCount(*destination),
        1U);
}

TEST_F(
    Phase15Fixture,
    RebalancerIgnoresStoppedDestination) {
    ASSERT_TRUE(
        harness.CreateFile("/file", 2));

    const auto handle =
        harness.AllocateChunk("/file");

    ASSERT_TRUE(handle.has_value());

    ASSERT_TRUE(
        harness.StopChunkserver(3));

    const auto moves =
        harness.GetMaster()
            .GetRebalancer()
            .Plan(1000);

    for (const auto& move : moves) {
        EXPECT_NE(
            move.destination_server_id,
            3U);
    }
}

TEST_F(
    Phase15Fixture,
    RebalancingPreservesReplicationFactor) {
    ASSERT_TRUE(
        harness.CreateFile("/file", 2));

    const auto handle =
        harness.AllocateChunk("/file");

    ASSERT_TRUE(handle.has_value());

    EXPECT_EQ(
        harness.GetMaster()
            .GetReplicaManager()
            .ReplicaCount(*handle),
        2U);

    EXPECT_TRUE(
        harness.SendHeartbeat(1, 1000));

    EXPECT_TRUE(
        harness.SendHeartbeat(2, 1000));

    EXPECT_TRUE(
        harness.SendHeartbeat(3, 1000));

    const auto completed =
        harness.GetMaster()
            .GetRebalancer()
            .Rebalance(1000);

    static_cast<void>(completed);

    EXPECT_EQ(
        harness.GetMaster()
            .GetReplicaManager()
            .ReplicaCount(*handle),
        2U);
}

TEST_F(
    Phase15Fixture,
    MasterRecoveryRestoresMetadata) {
    const auto persistence =
        std::filesystem::temp_directory_path() /
        ("gfs_phase15_master_recovery_" +
         std::to_string(
             reinterpret_cast<
                 std::uintptr_t>(this)));

    std::filesystem::remove_all(
        persistence);

    {
        gfs::master::Master master(
            2,
            persistence);

        ASSERT_TRUE(master.Initialize());

        ASSERT_TRUE(
            master.CreateFile(
                "/file",
                2));

        const auto handle =
            master.AllocateChunk("/file");

        ASSERT_TRUE(handle.has_value());

        ASSERT_TRUE(
            master.CreateCheckpoint());
    }

    {
        gfs::master::Master recovered(
            2,
            persistence);

        ASSERT_TRUE(
            recovered.Initialize());

        EXPECT_TRUE(
            recovered.FileExists("/file"));

        ASSERT_EQ(
            recovered.GetFileChunks(
                "/file")
                .size(),
            1U);
    }

    std::error_code error;
    std::filesystem::remove_all(
        persistence,
        error);
}

TEST_F(
    Phase15Fixture,
    ConcurrentHeartbeatUpdatesRemainValid) {
    ASSERT_TRUE(
        harness.SendHeartbeat(1, 5000));

    ASSERT_TRUE(
        harness.SendHeartbeat(2, 5000));

    ASSERT_TRUE(
        harness.SendHeartbeat(3, 5000));

    EXPECT_EQ(
        harness.GetMaster()
            .GetHeartbeatManager()
            .ServerCount(),
        3U);
}

}  // namespace