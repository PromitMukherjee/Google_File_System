#include "gfs/chunkserver/chunkserver.hpp"
#include "gfs/chunkserver/mutation/mutation.hpp"
#include "gfs/common/utils.hpp"
#include "gfs/master/master.hpp"
#include "gfs/master/lease/lease.hpp"
#include "gfs/master/lease/lease_manager.hpp"

#include <gtest/gtest.h>

#include <cstdint>
#include <filesystem>
#include <string>

namespace {

using gfs::chunkserver::Chunkserver;
using gfs::chunkserver::mutation::Mutation;
using gfs::master::Master;
using gfs::master::lease::Lease;
using gfs::master::lease::LeaseManager;
using gfs::master::replication::ReplicaManager;

class TemporaryDirectory {
public:
    TemporaryDirectory() {
        path_ =
            std::filesystem::temp_directory_path() /
            ("gfs_phase7_" +
             std::to_string(
                 reinterpret_cast<std::uintptr_t>(
                     this)));

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

TEST(LeaseTest, ValidityDependsOnExpiration) {
    Lease lease;

    lease.chunk_handle = 42;
    lease.primary_server_id = 7;
    lease.version = 3;
    lease.expiration_time_ms = 2000;

    EXPECT_TRUE(
        lease.IsValid(1999));

    EXPECT_FALSE(
        lease.IsValid(2000));

    EXPECT_FALSE(
        lease.IsExpired(1999));

    EXPECT_TRUE(
        lease.IsExpired(2000));

    EXPECT_EQ(
        lease.RemainingTimeMs(1500),
        500U);

    EXPECT_EQ(
        lease.RemainingTimeMs(2000),
        0U);
}

TEST(LeaseManagerTest, GrantsLeaseOnlyToCurrentPrimary) {
    ReplicaManager replicas;

    ASSERT_TRUE(
        replicas.RegisterReplica(
            42,
            1,
            true));

    ASSERT_TRUE(
        replicas.RegisterReplica(
            42,
            2));

    LeaseManager manager(
        replicas,
        60000);

    const auto lease =
        manager.AcquireLease(
            42,
            1,
            3);

    ASSERT_TRUE(
        lease.has_value());

    EXPECT_EQ(
        lease->chunk_handle,
        42U);

    EXPECT_EQ(
        lease->primary_server_id,
        1U);

    EXPECT_EQ(
        lease->version,
        3U);

    EXPECT_TRUE(
        manager.IsLeaseValid(
            42,
            1));

    EXPECT_FALSE(
        manager.AcquireLease(
            42,
            2,
            3)
            .has_value());
}

TEST(LeaseManagerTest, RejectsLeaseWithoutPrimaryReplica) {
    ReplicaManager replicas;

    ASSERT_TRUE(
        replicas.RegisterReplica(
            42,
            1));

    LeaseManager manager(
        replicas,
        60000);

    EXPECT_FALSE(
        manager.AcquireLease(
            42,
            1,
            1)
            .has_value());
}

TEST(LeaseManagerTest, LeaseCanBeExtendedAndReleased) {
    ReplicaManager replicas;

    ASSERT_TRUE(
        replicas.RegisterReplica(
            42,
            1,
            true));

    LeaseManager manager(
        replicas,
        60000);

    ASSERT_TRUE(
        manager.AcquireLease(
            42,
            1,
            1)
            .has_value());

    const auto before =
        manager.GetLease(42);

    ASSERT_TRUE(
        before.has_value());

    ASSERT_TRUE(
        manager.ExtendLease(
            42,
            1,
            1000));

    const auto after =
        manager.GetLease(42);

    ASSERT_TRUE(
        after.has_value());

    EXPECT_GT(
        after->expiration_time_ms,
        before->expiration_time_ms);

    EXPECT_TRUE(
        manager.ReleaseLease(42));

    EXPECT_FALSE(
        manager.GetLease(42).has_value());
}

TEST(LeaseManagerTest, ExpiredLeasesCanBeRemoved) {
    ReplicaManager replicas;

    ASSERT_TRUE(
        replicas.RegisterReplica(
            42,
            1,
            true));

    LeaseManager manager(
        replicas,
        1);

    ASSERT_TRUE(
        manager.AcquireLease(
            42,
            1,
            1)
            .has_value());

    EXPECT_EQ(
        manager.LeaseCount(),
        1U);

    const auto lease =
        manager.GetLease(42);

    ASSERT_TRUE(
        lease.has_value());

    const std::uint64_t expiration_time_ms =
        lease->expiration_time_ms;

    while (gfs::UnixTimeMillis() <
           expiration_time_ms) {
    }

    EXPECT_EQ(
        manager.RemoveExpiredLeases(),
        1U);

    EXPECT_EQ(
        manager.LeaseCount(),
        0U);
}

TEST(MasterLeaseTest, MasterGrantsLeaseToPrimary) {
    Master master;

    ASSERT_TRUE(
        master.RegisterChunkReplica(
            42,
            1,
            true));

    ASSERT_TRUE(
        master.RegisterChunkReplica(
            42,
            2));

    EXPECT_FALSE(
        master.IsLeaseValid(42));

    const auto lease =
        master.AcquireLease(
            42,
            1);

    ASSERT_TRUE(
        lease.has_value());

    EXPECT_EQ(
        lease->primary_server_id,
        1U);

    EXPECT_TRUE(
        master.IsLeaseValid(
            42,
            1));

    EXPECT_FALSE(
        master.IsLeaseValid(
            42,
            2));
}

TEST(MasterLeaseTest, ChangingPrimaryInvalidatesOldLease) {
    Master master;

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
        master.AcquireLease(
            42,
            1)
            .has_value());

    ASSERT_TRUE(
        master.SetChunkPrimary(
            42,
            2));

    EXPECT_FALSE(
        master.GetLease(42).has_value());

    EXPECT_FALSE(
        master.IsLeaseValid(
            42,
            1));

    ASSERT_TRUE(
        master.AcquireLease(
            42,
            2)
            .has_value());

    EXPECT_TRUE(
        master.IsLeaseValid(
            42,
            2));
}

TEST(MutationTest, ValidatesMutationFields) {
    Mutation mutation;

    mutation.chunk_handle = 42;
    mutation.chunk_version = 1;
    mutation.mutation_id = 1;
    mutation.offset = 0;
    mutation.data = "hello";

    EXPECT_TRUE(
        mutation.IsValid());

    mutation.mutation_id = 0;

    EXPECT_FALSE(
        mutation.IsValid());
}

TEST(MutationManagerTest, PrimaryAssignsSequentialMutationIds) {
    TemporaryDirectory directory;

    Chunkserver server(
        1,
        directory.Path());

    ASSERT_TRUE(
        server.Initialize());

    const auto first =
        server.GetMutationManager()
            .CreatePrimaryMutation(
                42,
                1,
                0,
                "first");

    ASSERT_TRUE(
        first.has_value());

    const auto second =
        server.GetMutationManager()
            .CreatePrimaryMutation(
                42,
                1,
                5,
                "second");

    ASSERT_TRUE(
        second.has_value());

    EXPECT_EQ(
        first->mutation_id,
        1U);

    EXPECT_EQ(
        second->mutation_id,
        2U);
}

TEST(MutationManagerTest, RejectsOutOfOrderSecondaryMutation) {
    TemporaryDirectory directory;

    Chunkserver server(
        1,
        directory.Path());

    ASSERT_TRUE(
        server.Initialize());

    Mutation second;

    second.chunk_handle = 42;
    second.chunk_version = 1;
    second.mutation_id = 2;
    second.offset = 0;
    second.data = "second";

    EXPECT_FALSE(
        server.GetMutationManager()
            .ApplyMutation(second));

    Mutation first;

    first.chunk_handle = 42;
    first.chunk_version = 1;
    first.mutation_id = 1;
    first.offset = 0;
    first.data = "first";

    EXPECT_TRUE(
        server.GetMutationManager()
            .ApplyMutation(first));

    EXPECT_EQ(
        server.GetMutationManager()
            .LastAppliedMutationId(42),
        1U);

    EXPECT_TRUE(
        server.GetMutationManager()
            .ApplyMutation(second));

    EXPECT_EQ(
        server.GetMutationManager()
            .LastAppliedMutationId(42),
        2U);
}

TEST(MutationManagerTest, PrimaryAppliesBeforePropagation) {
    TemporaryDirectory primary_directory;
    TemporaryDirectory secondary_directory;

    Chunkserver primary(
        1,
        primary_directory.Path());

    Chunkserver secondary(
        2,
        secondary_directory.Path());

    ASSERT_TRUE(
        primary.Initialize());

    ASSERT_TRUE(
        secondary.Initialize());

    Mutation propagated;

    ASSERT_TRUE(
        primary.GetMutationManager()
            .ExecutePrimaryMutation(
                42,
                1,
                0,
                "ordered mutation",
                [&secondary, &propagated](
                    const Mutation& mutation) {
                    propagated = mutation;

                    return secondary
                        .GetMutationManager()
                        .ApplyMutation(mutation);
                }));

    EXPECT_EQ(
        propagated.mutation_id,
        1U);

    EXPECT_EQ(
        primary.GetMutationManager()
            .LastAppliedMutationId(42),
        1U);

    EXPECT_EQ(
        secondary.GetMutationManager()
            .LastAppliedMutationId(42),
        1U);

    std::string primary_data;
    std::string secondary_data;

    ASSERT_TRUE(
        primary.ReadChunk(
            42,
            0,
            std::string(
                "ordered mutation").size(),
            primary_data));

    ASSERT_TRUE(
        secondary.ReadChunk(
            42,
            0,
            std::string(
                "ordered mutation").size(),
            secondary_data));

    EXPECT_EQ(
        primary_data,
        "ordered mutation");

    EXPECT_EQ(
        secondary_data,
        "ordered mutation");
}

TEST(MutationManagerTest, MultipleMutationsPreserveOrderAcrossReplicas) {
    TemporaryDirectory primary_directory;
    TemporaryDirectory secondary_directory;

    Chunkserver primary(
        1,
        primary_directory.Path());

    Chunkserver secondary(
        2,
        secondary_directory.Path());

    ASSERT_TRUE(
        primary.Initialize());

    ASSERT_TRUE(
        secondary.Initialize());

    ASSERT_TRUE(
        primary.GetMutationManager()
            .ExecutePrimaryMutation(
                42,
                1,
                0,
                "ABC",
                [&secondary](
                    const Mutation& mutation) {
                    return secondary
                        .GetMutationManager()
                        .ApplyMutation(mutation);
                }));

    ASSERT_TRUE(
        primary.GetMutationManager()
            .ExecutePrimaryMutation(
                42,
                1,
                3,
                "DEF",
                [&secondary](
                    const Mutation& mutation) {
                    return secondary
                        .GetMutationManager()
                        .ApplyMutation(mutation);
                }));

    EXPECT_EQ(
        primary.GetMutationManager()
            .LastAppliedMutationId(42),
        2U);

    EXPECT_EQ(
        secondary.GetMutationManager()
            .LastAppliedMutationId(42),
        2U);

    std::string primary_data;
    std::string secondary_data;

    ASSERT_TRUE(
        primary.ReadChunk(
            42,
            0,
            6,
            primary_data));

    ASSERT_TRUE(
        secondary.ReadChunk(
            42,
            0,
            6,
            secondary_data));

    EXPECT_EQ(
        primary_data,
        "ABCDEF");

    EXPECT_EQ(
        secondary_data,
        "ABCDEF");
}

}  // namespace