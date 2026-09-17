#include "gfs/master/master.hpp"
#include "gfs/master/recovery/checkpoint.hpp"
#include "gfs/master/recovery/operation_log.hpp"

#include <gtest/gtest.h>

#include <chrono>
#include <filesystem>
#include <fstream>
#include <string>
#include <vector>

namespace {

class TemporaryDirectory {
public:
    TemporaryDirectory() {
        const auto timestamp =
            std::chrono::steady_clock::now()
                .time_since_epoch()
                .count();

        path_ =
            std::filesystem::temp_directory_path() /
            ("gfs_phase11_" +
             std::to_string(timestamp));

        std::filesystem::create_directories(path_);
    }

    ~TemporaryDirectory() {
        std::error_code ec;

        std::filesystem::remove_all(
            path_,
            ec);
    }

    const std::filesystem::path&
    Path() const {
        return path_;
    }

private:
    std::filesystem::path path_;
};

}  // namespace

TEST(Phase11OperationLogTest, CreatesAndOpensLog) {
    TemporaryDirectory temp;

    gfs::master::recovery::OperationLog log;

    EXPECT_TRUE(
        log.Open(
            temp.Path() /
            "operation.log"));

    EXPECT_TRUE(log.IsOpen());
    EXPECT_EQ(log.LastSequence(), 0U);
}

TEST(Phase11OperationLogTest, AppendsInSequenceOrder) {
    TemporaryDirectory temp;

    gfs::master::recovery::OperationLog log;

    ASSERT_TRUE(
        log.Open(
            temp.Path() /
            "operation.log"));

    ASSERT_TRUE(
        log.Append(
            gfs::master::recovery::
                OperationType::CreateFile,
            gfs::master::recovery::
                OperationLog::EncodeFields(
                    {"/file"})));

    ASSERT_TRUE(
        log.Append(
            gfs::master::recovery::
                OperationType::CreateDirectory,
            gfs::master::recovery::
                OperationLog::EncodeFields(
                    {"/data"})));

    EXPECT_EQ(
        log.LastSequence(),
        2U);

    const auto records =
        log.Replay();

    ASSERT_EQ(records.size(), 2U);

    EXPECT_EQ(records[0].sequence, 1U);
    EXPECT_EQ(records[1].sequence, 2U);
}

TEST(Phase11OperationLogTest, PersistsAcrossReopen) {
    TemporaryDirectory temp;

    const auto path =
        temp.Path() /
        "operation.log";

    {
        gfs::master::recovery::OperationLog log;

        ASSERT_TRUE(log.Open(path));

        ASSERT_TRUE(
            log.Append(
                gfs::master::recovery::
                    OperationType::CreateFile,
                "payload"));
    }

    {
        gfs::master::recovery::OperationLog log;

        ASSERT_TRUE(log.Open(path));

        EXPECT_EQ(
            log.LastSequence(),
            1U);

        const auto records =
            log.Replay();

        ASSERT_EQ(records.size(), 1U);

        EXPECT_EQ(
            records[0].payload,
            "payload");
    }
}

TEST(Phase11OperationLogTest, ReplaysAfterSequenceBoundary) {
    TemporaryDirectory temp;

    gfs::master::recovery::OperationLog log;

    ASSERT_TRUE(
        log.Open(
            temp.Path() /
            "operation.log"));

    ASSERT_TRUE(
        log.Append(
            gfs::master::recovery::
                OperationType::CreateFile,
            "one"));

    ASSERT_TRUE(
        log.Append(
            gfs::master::recovery::
                OperationType::CreateFile,
            "two"));

    ASSERT_TRUE(
        log.Append(
            gfs::master::recovery::
                OperationType::CreateFile,
            "three"));

    const auto records =
        log.Replay(2);

    ASSERT_EQ(records.size(), 1U);

    EXPECT_EQ(records[0].sequence, 3U);
    EXPECT_EQ(records[0].payload, "three");
}

TEST(Phase11CheckpointTest, CreatesAndLoadsCheckpoint) {
    TemporaryDirectory temp;

    gfs::master::metadata::Metadata metadata;
    gfs::master::namespace_management::
        NamespaceManager namespace_manager;

    ASSERT_TRUE(
        namespace_manager.CreateDirectory(
            "/data"));

    ASSERT_TRUE(
        namespace_manager.CreateFile(
            "/data/file"));

    ASSERT_TRUE(
        metadata.CreateFile(
            "/data/file",
            3));

    const auto handle =
        metadata.AllocateChunk(
            "/data/file");

    ASSERT_TRUE(handle.has_value());

    gfs::master::recovery::Checkpoint checkpoint(
        temp.Path());

    ASSERT_TRUE(
        checkpoint.Create(
            metadata,
            namespace_manager,
            5));

    gfs::master::recovery::Checkpoint::State state;

    ASSERT_TRUE(
        checkpoint.LoadLatest(state));

    EXPECT_EQ(
        state.sequence,
        5U);

    EXPECT_EQ(
        state.files.size(),
        1U);

    EXPECT_EQ(
        state.chunks.size(),
        1U);
}

TEST(Phase11CheckpointTest, DoesNotPersistReplicaLocations) {
    TemporaryDirectory temp;

    gfs::master::metadata::Metadata metadata;
    gfs::master::namespace_management::
        NamespaceManager namespace_manager;

    ASSERT_TRUE(
        namespace_manager.CreateFile(
            "/file"));

    ASSERT_TRUE(
        metadata.CreateFile(
            "/file",
            3));

    const auto handle =
        metadata.AllocateChunk("/file");

    ASSERT_TRUE(handle.has_value());

    ASSERT_TRUE(
        metadata.AddReplica(
            *handle,
            101));

    ASSERT_TRUE(
        metadata.AddReplica(
            *handle,
            202));

    gfs::master::recovery::Checkpoint checkpoint(
        temp.Path());

    ASSERT_TRUE(
        checkpoint.Create(
            metadata,
            namespace_manager,
            1));

    gfs::master::recovery::Checkpoint::State state;

    ASSERT_TRUE(
        checkpoint.LoadLatest(state));

    ASSERT_EQ(
        state.chunks.size(),
        1U);

    gfs::master::metadata::Metadata restored_metadata;

    gfs::master::namespace_management::
        NamespaceManager restored_namespace;

    ASSERT_TRUE(
        checkpoint.Restore(
            state,
            restored_metadata,
            restored_namespace));

    const auto restored_chunk =
        restored_metadata.GetChunk(*handle);

    ASSERT_TRUE(
        restored_chunk.has_value());

    EXPECT_TRUE(
        restored_chunk->GetReplicas().empty());
}

TEST(Phase11MasterTest, RecoversFromCheckpointAndNewerLog) {
    TemporaryDirectory temp;

    {
        gfs::master::Master master(
            3,
            temp.Path());

        ASSERT_TRUE(
            master.Initialize());

        ASSERT_TRUE(
            master.CreateDirectory(
                "/data"));

        ASSERT_TRUE(
            master.CreateFile(
                "/data/file"));

        ASSERT_TRUE(
            master.CreateCheckpoint());

        ASSERT_TRUE(
            master.UpdateFileSize(
                "/data/file",
                100));

        ASSERT_TRUE(
            master.SetChunkSize(
                0,
                50) == false);
    }

    {
        gfs::master::Master master(
            3,
            temp.Path());

        ASSERT_TRUE(
            master.Initialize());

        EXPECT_TRUE(
            master.DirectoryExists(
                "/data"));

        EXPECT_TRUE(
            master.FileExists(
                "/data/file"));

        const auto file =
            master.GetFile(
                "/data/file");

        ASSERT_TRUE(file.has_value());

        EXPECT_EQ(
            file->GetSize(),
            100U);
    }
}

TEST(Phase11MasterTest, RecoversFromLogOnly) {
    TemporaryDirectory temp;

    {
        gfs::master::Master master(
            3,
            temp.Path());

        ASSERT_TRUE(
            master.Initialize());

        ASSERT_TRUE(
            master.CreateDirectory(
                "/data"));

        ASSERT_TRUE(
            master.CreateFile(
                "/data/file"));

        const auto handle =
            master.AllocateChunk(
                "/data/file");

        ASSERT_TRUE(handle.has_value());

        ASSERT_TRUE(
            master.SetChunkVersion(
                *handle,
                7));
    }

    {
        gfs::master::Master master(
            3,
            temp.Path());

        ASSERT_TRUE(
            master.Initialize());

        EXPECT_TRUE(
            master.FileExists(
                "/data/file"));

        const auto chunks =
            master.GetFileChunks(
                "/data/file");

        ASSERT_EQ(chunks.size(), 1U);
        EXPECT_EQ(chunks[0], 1U);

        const auto chunk =
            master.GetChunkInfo(
                chunks[0]);

        ASSERT_TRUE(chunk.has_value());

        EXPECT_EQ(
            chunk->GetVersion(),
            7U);
    }
}

TEST(Phase11MasterTest, DetectsTruncatedLog) {
    TemporaryDirectory temp;

    const auto path =
        temp.Path() /
        "operation.log";

    {
        gfs::master::recovery::OperationLog log;

        ASSERT_TRUE(log.Open(path));

        ASSERT_TRUE(
            log.Append(
                gfs::master::recovery::
                    OperationType::CreateFile,
                "payload"));
    }

    {
        std::ofstream output(
            path,
            std::ios::binary |
            std::ios::app);

        ASSERT_TRUE(output.good());

        const char corrupt[] =
            "TRUNCATED";

        output.write(
            corrupt,
            sizeof(corrupt) - 1);
    }

    gfs::master::Master master(
        3,
        temp.Path());

    EXPECT_FALSE(
        master.Initialize());
}

TEST(Phase11MasterTest, RuntimeReplicaStateIsNotRecovered) {
    TemporaryDirectory temp;

    gfs::ChunkHandle handle = 0;

    {
        gfs::master::Master master(
            3,
            temp.Path());

        ASSERT_TRUE(
            master.Initialize());

        ASSERT_TRUE(
            master.CreateFile("/file"));

        const auto allocated =
            master.AllocateChunk("/file");

        ASSERT_TRUE(allocated.has_value());

        handle = *allocated;

        ASSERT_TRUE(
            master.RegisterReplica(
                handle,
                101));

        ASSERT_TRUE(
            master.RegisterReplica(
                handle,
                202));

        ASSERT_TRUE(
            master.CreateCheckpoint());
    }

    {
        gfs::master::Master master(
            3,
            temp.Path());

        ASSERT_TRUE(
            master.Initialize());

        EXPECT_FALSE(
            master.HasReplica(
                handle,
                101));

        EXPECT_FALSE(
            master.HasReplica(
                handle,
                202));
    }
}

TEST(Phase11MasterTest, ReconstructsNamespaceAndMetadata) {
    TemporaryDirectory temp;

    {
        gfs::master::Master master(
            3,
            temp.Path());

        ASSERT_TRUE(
            master.Initialize());

        ASSERT_TRUE(
            master.CreateDirectory(
                "/a"));

        ASSERT_TRUE(
            master.CreateDirectory(
                "/a/b"));

        ASSERT_TRUE(
            master.CreateFile(
                "/a/b/file"));

        const auto handle =
            master.AllocateChunk(
                "/a/b/file");

        ASSERT_TRUE(handle.has_value());

        ASSERT_TRUE(
            master.SetChunkSize(
                *handle,
                4096));

        ASSERT_TRUE(
            master.CreateCheckpoint());
    }

    {
        gfs::master::Master master(
            3,
            temp.Path());

        ASSERT_TRUE(
            master.Initialize());

        EXPECT_TRUE(
            master.DirectoryExists("/a"));

        EXPECT_TRUE(
            master.DirectoryExists("/a/b"));

        EXPECT_TRUE(
            master.FileExists(
                "/a/b/file"));

        const auto chunks =
            master.GetFileChunks(
                "/a/b/file");

        ASSERT_EQ(chunks.size(), 1U);

        const auto chunk =
            master.GetChunkInfo(
                chunks[0]);

        ASSERT_TRUE(chunk.has_value());

        EXPECT_EQ(
            chunk->GetSize(),
            4096U);
    }
}