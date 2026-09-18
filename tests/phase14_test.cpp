#include "gfs/chunkserver/chunkserver.hpp"
#include "gfs/master/master.hpp"

#include <gtest/gtest.h>

#include <cstdint>
#include <filesystem>
#include <string>

namespace {

using gfs::ChunkHandle;
using gfs::ServerId;
using gfs::chunkserver::Chunkserver;
using gfs::master::Master;
using gfs::master::replication::ReplicaMove;

class TempDir {
public:
    TempDir() {
        path_ =
            std::filesystem::temp_directory_path() /
            ("gfs_phase14_" +
             std::to_string(
                 reinterpret_cast<
                     std::uintptr_t>(this)));

        std::filesystem::create_directories(
            path_);
    }

    ~TempDir() {
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

TEST(
    Phase14GarbageCollectionTest,
    DeletedFileProducesGarbageCandidate) {
    Master master(2);

    ASSERT_TRUE(
        master.Initialize());

    ASSERT_TRUE(
        master.CreateFile(
            "/file",
            2));

    const auto handle =
        master.AllocateChunk("/file");

    ASSERT_TRUE(
        handle.has_value());

    ASSERT_TRUE(
        master.RegisterReplica(
            *handle,
            1,
            true));

    ASSERT_TRUE(
        master.RegisterReplica(
            *handle,
            2,
            false));

    ASSERT_TRUE(
        master.DeleteFile("/file"));

    const auto candidates =
        master.GetGarbageCollector()
            .IdentifyGarbage();

    ASSERT_EQ(
        candidates.size(),
        1U);

    EXPECT_EQ(
        candidates.front(),
        *handle);
}

TEST(
    Phase14GarbageCollectionTest,
    ReferencedChunkIsNotCollected) {
    Master master(2);

    ASSERT_TRUE(
        master.Initialize());

    ASSERT_TRUE(
        master.CreateFile(
            "/file",
            2));

    const auto handle =
        master.AllocateChunk("/file");

    ASSERT_TRUE(
        handle.has_value());

    EXPECT_TRUE(
        master.GetGarbageCollector()
            .IdentifyGarbage()
            .empty());

    EXPECT_TRUE(
        master.GetChunkInfo(
            *handle)
            .has_value());
}

TEST(
    Phase14GarbageCollectionTest,
    SnapshotKeepsSharedChunkLive) {
    Master master(2);

    ASSERT_TRUE(
        master.Initialize());

    ASSERT_TRUE(
        master.CreateFile(
            "/source",
            2));

    const auto handle =
        master.AllocateChunk("/source");

    ASSERT_TRUE(
        handle.has_value());

    ASSERT_TRUE(
        master.CreateSnapshot(
            "/source",
            "/snapshot"));

    EXPECT_EQ(
        master.GetChunkReferenceCount(
            *handle),
        2U);

    EXPECT_TRUE(
        master.GetGarbageCollector()
            .IdentifyGarbage()
            .empty());

    EXPECT_TRUE(
        master.GetChunkInfo(
            *handle)
            .has_value());
}

TEST(
    Phase14GarbageCollectionTest,
    CowSourceAndDestinationRemainLive) {
    Master master(2);

    ASSERT_TRUE(
        master.Initialize());

    ASSERT_TRUE(
        master.CreateFile(
            "/source",
            2));

    const auto source =
        master.AllocateChunk(
            "/source");

    ASSERT_TRUE(
        source.has_value());

    ASSERT_TRUE(
        master.CreateSnapshot(
            "/source",
            "/snapshot"));

    const auto destination =
        master.PrepareCopyOnWrite(
            "/source",
            0,
            {});

    ASSERT_TRUE(
        destination.has_value());

    EXPECT_NE(
        *source,
        *destination);

    EXPECT_EQ(
        master.GetChunkReferenceCount(
            *source),
        1U);

    EXPECT_EQ(
        master.GetChunkReferenceCount(
            *destination),
        1U);

    EXPECT_TRUE(
        master.GetChunkInfo(
            *source)
            .has_value());

    EXPECT_TRUE(
        master.GetChunkInfo(
            *destination)
            .has_value());

    EXPECT_TRUE(
        master.GetGarbageCollector()
            .IdentifyGarbage()
            .empty());
}

TEST(
    Phase14GarbageCollectionTest,
    OrphansAreCollectedIndependently) {
    Master master(2);

    ASSERT_TRUE(
        master.Initialize());

    ASSERT_TRUE(
        master.CreateFile(
            "/file",
            2));

    const auto first =
        master.AllocateChunk("/file");

    const auto second =
        master.AllocateChunk("/file");

    ASSERT_TRUE(
        first.has_value());

    ASSERT_TRUE(
        second.has_value());

    ASSERT_TRUE(
        master.DeleteFile("/file"));

    const auto candidates =
        master.GetGarbageCollector()
            .IdentifyGarbage();

    ASSERT_EQ(
        candidates.size(),
        2U);

    EXPECT_LT(
        candidates[0],
        candidates[1]);

    const auto collected =
        master.GetGarbageCollector()
            .Collect();

    ASSERT_EQ(
        collected.size(),
        2U);

    EXPECT_FALSE(
        master.GetChunkInfo(
            *first)
            .has_value());

    EXPECT_FALSE(
        master.GetChunkInfo(
            *second)
            .has_value());
}

TEST(
    Phase14GarbageCollectionTest,
    ReplicaMetadataIsCleaned) {
    Master master(2);

    ASSERT_TRUE(
        master.Initialize());

    ASSERT_TRUE(
        master.CreateFile(
            "/file",
            2));

    const auto handle =
        master.AllocateChunk("/file");

    ASSERT_TRUE(
        handle.has_value());

    ASSERT_TRUE(
        master.RegisterReplica(
            *handle,
            1,
            true));

    ASSERT_TRUE(
        master.RegisterReplica(
            *handle,
            2,
            false));

    ASSERT_TRUE(
        master.DeleteFile("/file"));

    EXPECT_FALSE(
        master.HasReplica(
            *handle,
            1));

    EXPECT_FALSE(
        master.HasReplica(
            *handle,
            2));
}

TEST(
    Phase14RebalancingTest,
    DoesNotMoveBalancedPlacement) {
    Master master(2);

    ASSERT_TRUE(
        master.Initialize());

    ASSERT_TRUE(
        master.CreateFile(
            "/file",
            2));

    const auto handle =
        master.AllocateChunk("/file");

    ASSERT_TRUE(
        handle.has_value());

    ASSERT_TRUE(
        master.RegisterReplica(
            *handle,
            1,
            true));

    ASSERT_TRUE(
        master.RegisterReplica(
            *handle,
            2,
            false));

    ASSERT_TRUE(
        master.ProcessHeartbeat(
            1,
            1000));

    ASSERT_TRUE(
        master.ProcessHeartbeat(
            2,
            1000));

    EXPECT_TRUE(
        master.GetRebalancer()
            .Plan(1000)
            .empty());
}

TEST(
    Phase14RebalancingTest,
    AvoidsDuplicateDestinationReplica) {
    Master master(2);

    ASSERT_TRUE(
        master.Initialize());

    ASSERT_TRUE(
        master.CreateFile(
            "/file",
            2));

    const auto handle =
        master.AllocateChunk("/file");

    ASSERT_TRUE(
        handle.has_value());

    ASSERT_TRUE(
        master.RegisterReplica(
            *handle,
            1,
            true));

    ASSERT_TRUE(
        master.RegisterReplica(
            *handle,
            2,
            false));

    ASSERT_TRUE(
        master.ProcessHeartbeat(
            1,
            1000));

    ASSERT_TRUE(
        master.ProcessHeartbeat(
            2,
            1000));

    EXPECT_FALSE(
        master.GetRebalancer()
            .ExecuteMove(
                ReplicaMove{
                    *handle,
                    1,
                    2},
                1000));

    EXPECT_EQ(
        master.GetReplicaManager()
            .ReplicaCount(*handle),
        2U);
}

TEST(
    Phase14RebalancingTest,
    FailedTransferPreservesReplicaState) {
    TempDir first_dir;
    TempDir second_dir;

    Chunkserver first(
        1,
        first_dir.Path());

    Chunkserver second(
        2,
        second_dir.Path());

    ASSERT_TRUE(
        first.Initialize());

    ASSERT_TRUE(
        second.Initialize());

    Master master(2);

    ASSERT_TRUE(
        master.Initialize());

    ASSERT_TRUE(
        master.CreateFile(
            "/file",
            2));

    const auto handle =
        master.AllocateChunk("/file");

    ASSERT_TRUE(
        handle.has_value());

    ASSERT_TRUE(
        master.RegisterReplica(
            *handle,
            1,
            true));

    ASSERT_TRUE(
        master.RegisterReplica(
            *handle,
            2,
            false));

    ASSERT_TRUE(
        master.GetRecoveryManager()
            .RegisterChunkserver(
                first));

    ASSERT_TRUE(
        master.GetRecoveryManager()
            .RegisterChunkserver(
                second));

    ASSERT_TRUE(
        master.ProcessHeartbeat(
            1,
            1000));

    ASSERT_TRUE(
        master.ProcessHeartbeat(
            2,
            1000));

    EXPECT_FALSE(
        master.GetRebalancer()
            .ExecuteMove(
                ReplicaMove{
                    *handle,
                    1,
                    3},
                1000));

    EXPECT_TRUE(
        master.HasReplica(
            *handle,
            1));

    EXPECT_TRUE(
        master.HasReplica(
            *handle,
            2));
}

}  // namespace