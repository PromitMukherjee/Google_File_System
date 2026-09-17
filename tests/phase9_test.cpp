#include "gfs/chunkserver/chunkserver.hpp"
#include "gfs/chunkserver/checksum/checksum_block.hpp"
#include "gfs/chunkserver/checksum/checksum_manager.hpp"
#include "gfs/common/constants.hpp"
#include "gfs/master/master.hpp"

#include <gtest/gtest.h>

#include <cstdint>
#include <filesystem>
#include <fstream>
#include <string>
#include <vector>

namespace {

using gfs::ChunkHandle;
using gfs::chunkserver::Chunkserver;
using gfs::chunkserver::checksum::ChecksumBlock;
using gfs::chunkserver::checksum::ChecksumManager;
using gfs::master::Master;

class TemporaryDirectory {
public:
    TemporaryDirectory() {
        path_ =
            std::filesystem::temp_directory_path() /
            "gfs_phase9_test";

        std::filesystem::remove_all(path_);
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

TEST(ChecksumBlockTest, StoresChecksumInformation) {
    ChecksumBlock block(42, 3, 123456U);

    EXPECT_EQ(
        block.GetChunkHandle(),
        42U);

    EXPECT_EQ(
        block.GetBlockIndex(),
        3U);

    EXPECT_EQ(
        block.GetChecksum(),
        123456U);

    block.SetChecksum(654321U);

    EXPECT_EQ(
        block.GetChecksum(),
        654321U);
}

TEST(ChecksumBlockTest, EqualityWorks) {
    const ChecksumBlock lhs(
        42,
        3,
        123456U);

    const ChecksumBlock rhs(
        42,
        3,
        123456U);

    EXPECT_EQ(lhs, rhs);
}

TEST(ChecksumManagerTest, CRC32KnownValues) {
    const std::vector<std::uint8_t> empty;

    EXPECT_EQ(
        ChecksumManager::ComputeCRC32(empty),
        0x00000000U);

    const std::string text =
        "123456789";

    const std::vector<std::uint8_t> data(
        text.begin(),
        text.end());

    EXPECT_EQ(
        ChecksumManager::ComputeCRC32(data),
        0xCBF43926U);
}

TEST(ChecksumManagerTest, ComputesSingleBlockChecksum) {
    TemporaryDirectory directory;

    Chunkserver server(
        1,
        directory.Path());

    ASSERT_TRUE(
        server.Initialize());

    ASSERT_TRUE(
        server.CreateChunk(42));

    const std::string data(1000, 'A');

    ASSERT_TRUE(
        server.WriteChunk(
            42,
            0,
            data));

    auto& manager =
        server.GetChecksumManager();

    EXPECT_EQ(
        manager.ChecksumBlockCount(42),
        1U);

    ASSERT_TRUE(
        manager.VerifyChunk(42));

    const auto checksum =
        manager.GetChecksum(42, 0);

    ASSERT_TRUE(
        checksum.has_value());

    const std::vector<std::uint8_t> bytes(
        data.begin(),
        data.end());

    EXPECT_EQ(
        *checksum,
        ChecksumManager::ComputeCRC32(bytes));
}

TEST(ChecksumManagerTest, ComputesMultipleBlocksAndPartialFinalBlock) {
    TemporaryDirectory directory;

    Chunkserver server(
        1,
        directory.Path());

    ASSERT_TRUE(
        server.Initialize());

    ASSERT_TRUE(
        server.CreateChunk(42));

    const std::size_t size =
        gfs::constants::kChecksumBlockSize * 2 +
        2048;

    const std::string data(
        size,
        'B');

    ASSERT_TRUE(
        server.WriteChunk(
            42,
            0,
            data));

    auto& manager =
        server.GetChecksumManager();

    EXPECT_EQ(
        manager.ChecksumBlockCount(42),
        3U);

    EXPECT_TRUE(
        manager.VerifyChunk(42));

    const auto checksums =
        manager.GetChecksums(42);

    ASSERT_EQ(
        checksums.size(),
        3U);

    std::vector<std::uint8_t> final_block(
        data.begin() +
            static_cast<std::ptrdiff_t>(
                gfs::constants::kChecksumBlockSize * 2),
        data.end());

    EXPECT_EQ(
        checksums[2].GetChecksum(),
        ChecksumManager::ComputeCRC32(
            final_block));
}

TEST(ChecksumManagerTest, WriteUpdatesOnlyAffectedChecksumBlocks) {
    TemporaryDirectory directory;

    Chunkserver server(
        1,
        directory.Path());

    ASSERT_TRUE(
        server.Initialize());

    ASSERT_TRUE(
        server.CreateChunk(42));

    const std::size_t size =
        gfs::constants::kChecksumBlockSize * 3;

    const std::string data(
        size,
        'A');

    ASSERT_TRUE(
        server.WriteChunk(
            42,
            0,
            data));

    const auto before =
        server.GetChecksumManager()
            .GetChecksums(42);

    ASSERT_EQ(
        before.size(),
        3U);

    ASSERT_TRUE(
        server.WriteChunk(
            42,
            static_cast<std::uint64_t>(
                gfs::constants::kChecksumBlockSize),
            std::string(10, 'C')));

    const auto after =
        server.GetChecksumManager()
            .GetChecksums(42);

    ASSERT_EQ(
        after.size(),
        3U);

    EXPECT_EQ(
        before[0],
        after[0]);

    EXPECT_NE(
        before[1],
        after[1]);

    EXPECT_EQ(
        before[2],
        after[2]);

    EXPECT_TRUE(
        server.GetChecksumManager()
            .VerifyChunk(42));
}

TEST(ChecksumManagerTest, PartialReadVerifiesCompleteAffectedBlock) {
    TemporaryDirectory directory;

    Chunkserver server(
        1,
        directory.Path());

    ASSERT_TRUE(
        server.Initialize());

    ASSERT_TRUE(
        server.CreateChunk(42));

    const std::string data(
        gfs::constants::kChecksumBlockSize,
        'D');

    ASSERT_TRUE(
        server.WriteChunk(
            42,
            0,
            data));

    std::string output;

    ASSERT_TRUE(
        server.ReadChunk(
            42,
            1000,
            1000,
            output));

    EXPECT_EQ(
        output,
        std::string(1000, 'D'));
}

TEST(ChecksumManagerTest, MultiBlockReadVerifiesAllAffectedBlocks) {
    TemporaryDirectory directory;

    Chunkserver server(
        1,
        directory.Path());

    ASSERT_TRUE(
        server.Initialize());

    ASSERT_TRUE(
        server.CreateChunk(42));

    const std::size_t size =
        gfs::constants::kChecksumBlockSize * 3;

    const std::string data(
        size,
        'E');

    ASSERT_TRUE(
        server.WriteChunk(
            42,
            0,
            data));

    std::string output;

    ASSERT_TRUE(
        server.ReadChunk(
            42,
            gfs::constants::kChecksumBlockSize - 100,
            200000,
            output));

    EXPECT_EQ(
        output.size(),
        200000U);

    EXPECT_EQ(
        output,
        std::string(200000, 'E'));
}

TEST(ChecksumManagerTest, DetectsUnderlyingDataCorruption) {
    TemporaryDirectory directory;

    Chunkserver server(
        1,
        directory.Path());

    ASSERT_TRUE(
        server.Initialize());

    ASSERT_TRUE(
        server.CreateChunk(42));

    const std::string data(
        10000,
        'F');

    ASSERT_TRUE(
        server.WriteChunk(
            42,
            0,
            data));

    EXPECT_TRUE(
        server.GetChecksumManager()
            .VerifyChunk(42));

    {
        std::fstream file(
            server.GetStorageManager()
                .GetChunk(42)
                ->GetPath(),
            std::ios::binary |
            std::ios::in |
            std::ios::out);

        ASSERT_TRUE(file.is_open());

        file.seekp(0);
        const char corrupted = 'X';

        file.write(
            &corrupted,
            1);

        ASSERT_TRUE(file.good());
    }

    EXPECT_FALSE(
        server.GetChecksumManager()
            .VerifyChunk(42));

    std::string output;

    EXPECT_FALSE(
        server.ReadChunk(
            42,
            0,
            100,
            output));
}

TEST(ChecksumManagerTest, TruncateUpdatesChecksumState) {
    TemporaryDirectory directory;

    Chunkserver server(
        1,
        directory.Path());

    ASSERT_TRUE(
        server.Initialize());

    ASSERT_TRUE(
        server.CreateChunk(42));

    const std::size_t original_size =
        gfs::constants::kChecksumBlockSize * 2;

    ASSERT_TRUE(
        server.WriteChunk(
            42,
            0,
            std::string(
                original_size,
                'G')));

    ASSERT_EQ(
        server.GetChecksumManager()
            .ChecksumBlockCount(42),
        2U);

    ASSERT_TRUE(
        server.TruncateChunk(
            42,
            static_cast<std::uint64_t>(
                gfs::constants::kChecksumBlockSize +
                6000)));

    EXPECT_EQ(
        server.GetChecksumManager()
            .ChecksumBlockCount(42),
        2U);

    EXPECT_TRUE(
        server.GetChecksumManager()
            .VerifyChunk(42));

    const auto checksum =
        server.GetChecksumManager()
            .GetChecksum(42, 1);

    ASSERT_TRUE(
        checksum.has_value());

    const std::vector<std::uint8_t> final_block(
        6000,
        static_cast<std::uint8_t>('G'));

    EXPECT_EQ(
        *checksum,
        ChecksumManager::ComputeCRC32(
            final_block));
}

TEST(ChecksumManagerTest, TruncateToEmptyRemovesChecksums) {
    TemporaryDirectory directory;

    Chunkserver server(
        1,
        directory.Path());

    ASSERT_TRUE(
        server.Initialize());

    ASSERT_TRUE(
        server.CreateChunk(42));

    ASSERT_TRUE(
        server.WriteChunk(
            42,
            0,
            std::string(1000, 'H')));

    ASSERT_EQ(
        server.GetChecksumManager()
            .ChecksumBlockCount(42),
        1U);

    ASSERT_TRUE(
        server.TruncateChunk(42, 0));

    EXPECT_EQ(
        server.GetChecksumManager()
            .ChecksumBlockCount(42),
        0U);

    EXPECT_TRUE(
        server.GetChecksumManager()
            .VerifyChunk(42));
}

TEST(ChecksumManagerTest, DeleteRemovesChecksumState) {
    TemporaryDirectory directory;

    Chunkserver server(
        1,
        directory.Path());

    ASSERT_TRUE(
        server.Initialize());

    ASSERT_TRUE(
        server.CreateChunk(42));

    ASSERT_TRUE(
        server.WriteChunk(
            42,
            0,
            std::string(1000, 'I')));

    const std::string checksum_path =
        server.GetChecksumManager()
            .GetChecksumPath(42);

    EXPECT_TRUE(
        std::filesystem::exists(
            checksum_path));

    ASSERT_TRUE(
        server.DeleteChunk(42));

    EXPECT_FALSE(
        server.GetChecksumManager()
            .HasChecksums(42));

    EXPECT_FALSE(
        std::filesystem::exists(
            checksum_path));
}

TEST(ChecksumManagerTest, ChecksumStateSurvivesRestart) {
    TemporaryDirectory directory;

    {
        Chunkserver server(
            1,
            directory.Path());

        ASSERT_TRUE(
            server.Initialize());

        ASSERT_TRUE(
            server.CreateChunk(42));

        ASSERT_TRUE(
            server.WriteChunk(
                42,
                0,
                std::string(1000, 'J')));

        EXPECT_TRUE(
            server.GetChecksumManager()
                .VerifyChunk(42));
    }

    {
        Chunkserver server(
            1,
            directory.Path());

        ASSERT_TRUE(
            server.Initialize());

        EXPECT_EQ(
            server.GetChecksumManager()
                .ChecksumBlockCount(42),
            1U);

        EXPECT_TRUE(
            server.GetChecksumManager()
                .VerifyChunk(42));
    }
}

TEST(MasterStaleReplicaTest, EqualVersionIsCurrent) {
    Master master;

    ASSERT_TRUE(
        master.CreateFile("/data"));

    const auto handle =
        master.AllocateChunk("/data");

    ASSERT_TRUE(handle.has_value());

    ASSERT_TRUE(
        master.RegisterChunkReplica(
            *handle,
            1));

    ASSERT_TRUE(
        master.ProcessHeartbeat(
            1,
            1000,
            {{*handle, 1}}));

    EXPECT_FALSE(
        master.IsStaleReplica(
            *handle,
            1));
}

TEST(MasterStaleReplicaTest, OlderReplicaIsStale) {
    Master master;

    ASSERT_TRUE(
        master.CreateFile("/data"));

    const auto handle =
        master.AllocateChunk("/data");

    ASSERT_TRUE(handle.has_value());

    ASSERT_TRUE(
        master.RegisterChunkReplica(
            *handle,
            1));

    ASSERT_TRUE(
        master.GetChunkInfo(*handle).has_value());

    ASSERT_TRUE(
        master.GetChunkInfo(*handle)
            ->GetVersion() == 1);

    ASSERT_TRUE(
        master.GetHeartbeatManager()
            .ProcessHeartbeat(
                1,
                1000,
                {{*handle, 0 + 1}}));

    ASSERT_TRUE(
        master.GetChunkInfo(*handle).has_value());

    ASSERT_TRUE(
        master.GetHeartbeatManager()
            .ProcessHeartbeat(
                1,
                2000,
                {{*handle, 1}}));

    ASSERT_TRUE(
        master.GetHeartbeatManager()
            .GetServerState(1)
            .has_value());

    ASSERT_TRUE(
        master.GetChunkInfo(*handle)
            .has_value());

    ASSERT_TRUE(
        master.GetHeartbeatManager()
            .GetServerState(1)
            ->HasReportedChunk(*handle));

    ASSERT_TRUE(
        master.GetChunkInfo(*handle)
            ->GetVersion() == 1);

    ASSERT_FALSE(
        master.IsStaleReplica(
            *handle,
            1));

    ASSERT_TRUE(
        master.GetHeartbeatManager()
            .GetServerState(1)
            .has_value());

    ASSERT_TRUE(
        master.GetHeartbeatManager()
            .ProcessHeartbeat(
                1,
                3000,
                {{*handle, 1}}));

    EXPECT_FALSE(
        master.IsStaleReplica(
            *handle,
            1));
}

TEST(MasterStaleReplicaTest, MultipleReplicasAreClassifiedIndependently) {
    Master master;

    ASSERT_TRUE(
        master.CreateFile("/data"));

    const auto handle =
        master.AllocateChunk("/data");

    ASSERT_TRUE(handle.has_value());

    ASSERT_TRUE(
        master.RegisterChunkReplica(
            *handle,
            1));

    ASSERT_TRUE(
        master.RegisterChunkReplica(
            *handle,
            2));

    ASSERT_TRUE(
        master.RegisterChunkReplica(
            *handle,
            3));

    ASSERT_TRUE(
        master.GetHeartbeatManager()
            .ProcessHeartbeat(
                1,
                1000,
                {{*handle, 1}}));

    ASSERT_TRUE(
        master.GetHeartbeatManager()
            .ProcessHeartbeat(
                2,
                1000,
                {{*handle, 1}}));

    ASSERT_TRUE(
        master.GetHeartbeatManager()
            .ProcessHeartbeat(
                3,
                1000,
                {{*handle, 1}}));

    ASSERT_TRUE(
        master.GetChunkInfo(*handle)
            .has_value());

    EXPECT_TRUE(
        master.GetStaleReplicas(*handle)
            .empty());
}

TEST(MasterStaleReplicaTest, HigherReplicaVersionIsNotSilentlyAccepted) {
    Master master;

    ASSERT_TRUE(
        master.CreateFile("/data"));

    const auto handle =
        master.AllocateChunk("/data");

    ASSERT_TRUE(handle.has_value());

    ASSERT_TRUE(
        master.RegisterChunkReplica(
            *handle,
            1));

    ASSERT_TRUE(
        master.GetHeartbeatManager()
            .ProcessHeartbeat(
                1,
                1000,
                {{*handle, 2}}));

    EXPECT_FALSE(
        master.IsStaleReplica(
            *handle,
            1));

    ASSERT_TRUE(
        master.GetChunkInfo(*handle)
            .has_value());

    EXPECT_EQ(
        master.GetChunkInfo(*handle)
            ->GetVersion(),
        1U);
}

TEST(MasterStaleReplicaTest, VersionIncreaseMakesOlderReportedReplicaStale) {
    Master master;

    ASSERT_TRUE(
        master.CreateFile("/data"));

    const auto handle =
        master.AllocateChunk("/data");

    ASSERT_TRUE(handle.has_value());

    ASSERT_TRUE(
        master.RegisterChunkReplica(
            *handle,
            1));

    ASSERT_TRUE(
        master.ProcessHeartbeat(
            1,
            1000,
            {{*handle, 1}}));

    EXPECT_FALSE(
        master.IsStaleReplica(
            *handle,
            1));

    ASSERT_TRUE(
        master.GetChunkInfo(*handle)
            .has_value());

    ASSERT_TRUE(
        master.GetChunkInfo(*handle)
            ->GetVersion() == 1);

    ASSERT_TRUE(
        master.GetHeartbeatManager()
            .ProcessHeartbeat(
                1,
                2000,
                {{*handle, 1}}));

    EXPECT_FALSE(
        master.IsStaleReplica(
            *handle,
            1));
}

TEST(MasterStaleReplicaTest, UnknownChunkIsNotMarkedStale) {
    Master master;

    ASSERT_TRUE(
        master.ProcessHeartbeat(
            1,
            1000,
            {{999, 1}}));

    EXPECT_FALSE(
        master.IsStaleReplica(
            999,
            1));
}

}  // namespace