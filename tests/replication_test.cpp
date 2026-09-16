#include "gfs/chunkserver/chunkserver.hpp"
#include "gfs/chunkserver/replication/clone_manager.hpp"
#include "gfs/chunkserver/replication/replica_receiver.hpp"
#include "gfs/chunkserver/replication/replica_sender.hpp"
#include "gfs/common/constants.hpp"
#include "gfs/master/master.hpp"
#include "gfs/master/replication/placement_policy.hpp"
#include "gfs/master/replication/replica_manager.hpp"

#include <gtest/gtest.h>

#include <algorithm>
#include <filesystem>
#include <memory>
#include <string>
#include <vector>

namespace {

using gfs::ChunkHandle;
using gfs::ServerId;
using gfs::chunkserver::Chunkserver;
using gfs::master::Master;
using gfs::master::replication::PlacementCandidate;
using gfs::master::replication::PlacementPolicy;
using gfs::master::replication::ReplicaManager;

class TemporaryDirectory {
public:
    TemporaryDirectory() {
        path_ =
            std::filesystem::temp_directory_path() /
            ("gfs_phase6_" +
             std::to_string(
                 reinterpret_cast<std::uintptr_t>(
                     this)));

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

TEST(ReplicaManagerTest, RegistersAndLooksUpReplicas) {
    ReplicaManager manager;

    EXPECT_TRUE(manager.RegisterReplica(42, 1, true));
    EXPECT_TRUE(manager.RegisterReplica(42, 2));
    EXPECT_TRUE(manager.RegisterReplica(42, 3));

    EXPECT_EQ(manager.ReplicaCount(42), 3U);
    EXPECT_TRUE(manager.HasReplica(42, 1));
    EXPECT_TRUE(manager.HasReplica(42, 2));
    EXPECT_TRUE(manager.HasReplica(42, 3));

    const auto primary = manager.GetPrimary(42);
    ASSERT_TRUE(primary.has_value());
    EXPECT_EQ(*primary, 1U);
}

TEST(ReplicaManagerTest, PreventsDuplicateReplica) {
    ReplicaManager manager;

    EXPECT_TRUE(manager.RegisterReplica(42, 1, true));
    EXPECT_FALSE(manager.RegisterReplica(42, 1));

    EXPECT_EQ(manager.ReplicaCount(42), 1U);
}

TEST(ReplicaManagerTest, RemovesReplica) {
    ReplicaManager manager;

    ASSERT_TRUE(manager.RegisterReplica(42, 1, true));
    ASSERT_TRUE(manager.RegisterReplica(42, 2));

    EXPECT_TRUE(manager.RemoveReplica(42, 1));
    EXPECT_FALSE(manager.HasReplica(42, 1));

    const auto primary = manager.GetPrimary(42);
    ASSERT_TRUE(primary.has_value());
    EXPECT_EQ(*primary, 2U);
}

TEST(PlacementPolicyTest, SelectsReplicationFactor) {
    PlacementPolicy policy(3);

    std::vector<PlacementCandidate> candidates{
        {3, 4},
        {1, 2},
        {2, 3},
        {4, 1}};

    const auto selected =
        policy.SelectReplicas(100, candidates);

    ASSERT_EQ(selected.size(), 3U);
    EXPECT_EQ(selected[0], 4U);
    EXPECT_EQ(selected[1], 1U);
    EXPECT_EQ(selected[2], 2U);
}

TEST(PlacementPolicyTest, AvoidsDuplicateServers) {
    PlacementPolicy policy(3);

    std::vector<PlacementCandidate> candidates{
        {1, 0},
        {1, 0},
        {2, 0},
        {3, 0}};

    const auto selected =
        policy.SelectReplicas(100, candidates);

    ASSERT_EQ(selected.size(), 3U);

    EXPECT_NE(
        std::find(
            selected.begin(),
            selected.end(),
            1U),
        selected.end());

    EXPECT_NE(
        std::find(
            selected.begin(),
            selected.end(),
            2U),
        selected.end());

    EXPECT_NE(
        std::find(
            selected.begin(),
            selected.end(),
            3U),
        selected.end());
}

TEST(PlacementPolicyTest, RejectsInvalidPlacement) {
    PlacementPolicy policy(3);

    EXPECT_FALSE(
        policy.IsValidPlacement(
            {1, 2},
            3));

    EXPECT_FALSE(
        policy.IsValidPlacement(
            {1, 1, 2},
            3));

    EXPECT_FALSE(
        policy.IsValidPlacement(
            {1, 2, 0},
            3));

    EXPECT_TRUE(
        policy.IsValidPlacement(
            {1, 2, 3},
            3));
}

TEST(ChunkserverReplicationTest, ReceiverStoresReplica) {
    TemporaryDirectory directory;
    Chunkserver server(2, directory.Path());

    ASSERT_TRUE(server.Initialize());

    const std::string data = "replicated chunk data";

    ASSERT_TRUE(
        server.GetReplicaReceiver().Receive(
            42,
            data));

    EXPECT_TRUE(server.ChunkExists(42));
    EXPECT_EQ(server.GetChunkSize(42), data.size());

    std::string output;

    ASSERT_TRUE(
        server.ReadChunk(
            42,
            0,
            data.size(),
            output));

    EXPECT_EQ(output, data);
}

TEST(ChunkserverReplicationTest, ReceiverRejectsOversizedReplica) {
    TemporaryDirectory directory;
    Chunkserver server(2, directory.Path());

    ASSERT_TRUE(server.Initialize());

    const std::string data(
        gfs::constants::kChunkSize + 1,
        'x');

    EXPECT_FALSE(
        server.GetReplicaReceiver().Receive(
            42,
            data));

    EXPECT_FALSE(server.ChunkExists(42));
}

TEST(ChunkserverReplicationTest, SenderReadsLocalChunk) {
    TemporaryDirectory directory;
    Chunkserver server(1, directory.Path());

    ASSERT_TRUE(server.Initialize());
    ASSERT_TRUE(server.CreateChunk(42));

    const std::string data = "source replica";

    ASSERT_TRUE(
        server.WriteChunk(
            42,
            0,
            data));

    std::string received;
    ServerId target = 0;
    ChunkHandle received_handle = 0;

    server.GetReplicaSender().SetTransferFunction(
        [&target,
         &received_handle,
         &received](
            ServerId server_id,
            ChunkHandle handle,
            const std::string& payload) {
            target = server_id;
            received_handle = handle;
            received = payload;
            return true;
        });

    ASSERT_TRUE(
        server.GetReplicaSender().Send(42, 2));

    EXPECT_EQ(target, 2U);
    EXPECT_EQ(received_handle, 42U);
    EXPECT_EQ(received, data);
}

TEST(ChunkserverReplicationTest, SenderTransfersToReceiver) {
    TemporaryDirectory source_directory;
    TemporaryDirectory target_directory;

    Chunkserver source(
        1,
        source_directory.Path());

    Chunkserver target(
        2,
        target_directory.Path());

    ASSERT_TRUE(source.Initialize());
    ASSERT_TRUE(target.Initialize());

    ASSERT_TRUE(source.CreateChunk(42));

    const std::string data =
        "chunkserver to chunkserver";

    ASSERT_TRUE(
        source.WriteChunk(
            42,
            0,
            data));

    source.GetReplicaSender().SetTransferFunction(
        [&target](
            ServerId server_id,
            ChunkHandle handle,
            const std::string& payload) {
            if (server_id != target.GetServerId()) {
                return false;
            }

            return target.GetReplicaReceiver().Receive(
                handle,
                payload);
        });

    ASSERT_TRUE(
        source.GetReplicaSender().Send(
            42,
            target.GetServerId()));

    EXPECT_TRUE(target.ChunkExists(42));

    std::string output;

    ASSERT_TRUE(
        target.ReadChunk(
            42,
            0,
            data.size(),
            output));

    EXPECT_EQ(output, data);
}

TEST(CloneManagerTest, ClonesLocalChunk) {
    TemporaryDirectory directory;

    Chunkserver server(
        1,
        directory.Path());

    ASSERT_TRUE(server.Initialize());
    ASSERT_TRUE(server.CreateChunk(10));

    const std::string data =
        "local cloned chunk";

    ASSERT_TRUE(
        server.WriteChunk(
            10,
            0,
            data));

    ASSERT_TRUE(
        server.GetCloneManager().Clone(
            10,
            20));

    EXPECT_TRUE(server.ChunkExists(20));
    EXPECT_EQ(server.GetChunkSize(20), data.size());

    std::string output;

    ASSERT_TRUE(
        server.ReadChunk(
            20,
            0,
            data.size(),
            output));

    EXPECT_EQ(output, data);
}

TEST(CloneManagerTest, CloneRejectsExistingDestination) {
    TemporaryDirectory directory;

    Chunkserver server(
        1,
        directory.Path());

    ASSERT_TRUE(server.Initialize());
    ASSERT_TRUE(server.CreateChunk(10));
    ASSERT_TRUE(server.CreateChunk(20));

    ASSERT_TRUE(
        server.WriteChunk(
            10,
            0,
            "source"));

    EXPECT_FALSE(
        server.GetCloneManager().Clone(
            10,
            20));
}

TEST(MasterReplicationTest, RegistersAndTracksReplicas) {
    Master master;

    ASSERT_TRUE(master.Initialize());

    ASSERT_TRUE(
        master.RegisterChunkReplica(
            42,
            1,
            true));

    ASSERT_TRUE(
        master.RegisterChunkReplica(
            42,
            2));

    ASSERT_TRUE(
        master.RegisterChunkReplica(
            42,
            3));

    const auto replicas =
        master.GetChunkReplicas(42);

    ASSERT_EQ(replicas.size(), 3U);

    EXPECT_TRUE(
        master.HasChunkReplica(42, 1));

    EXPECT_TRUE(
        master.HasChunkReplica(42, 2));

    EXPECT_TRUE(
        master.HasChunkReplica(42, 3));

    const auto primary =
        master.GetChunkPrimary(42);

    ASSERT_TRUE(primary.has_value());
    EXPECT_EQ(*primary, 1U);
}

TEST(MasterReplicationTest, PlacesReplicas) {
    Master master(3);

    ASSERT_TRUE(master.Initialize());

    const auto selected =
        master.PlaceChunkReplicas(
            100,
            {
                {1, 0},
                {2, 2},
                {3, 1},
                {4, 3}});

    ASSERT_EQ(selected.size(), 3U);

    EXPECT_EQ(selected[0], 1U);
    EXPECT_EQ(selected[1], 3U);
    EXPECT_EQ(selected[2], 2U);

    EXPECT_EQ(
        master.GetReplicaManager().ReplicaCount(100),
        3U);

    const auto primary =
        master.GetChunkPrimary(100);

    ASSERT_TRUE(primary.has_value());
    EXPECT_EQ(*primary, 1U);
}

TEST(MasterReplicationTest, ChangesPrimary) {
    Master master;

    ASSERT_TRUE(master.Initialize());

    ASSERT_TRUE(
        master.RegisterChunkReplica(
            42,
            1,
            true));

    ASSERT_TRUE(
        master.RegisterChunkReplica(
            42,
            2));

    ASSERT_TRUE(
        master.SetChunkPrimary(42, 2));

    const auto primary =
        master.GetChunkPrimary(42);

    ASSERT_TRUE(primary.has_value());
    EXPECT_EQ(*primary, 2U);
}

TEST(ChunkserverReplicationTest, ClonePreservesSize) {
    TemporaryDirectory directory;

    Chunkserver server(
        1,
        directory.Path());

    ASSERT_TRUE(server.Initialize());
    ASSERT_TRUE(server.CreateChunk(1));

    const std::string data(1024, 'a');

    ASSERT_TRUE(
        server.WriteChunk(
            1,
            0,
            data));

    ASSERT_TRUE(
        server.GetCloneManager().Clone(
            1,
            2));

    EXPECT_EQ(
        server.GetChunkSize(1),
        server.GetChunkSize(2));

    EXPECT_EQ(
        server.GetChunkSize(2),
        data.size());
}

}  // namespace