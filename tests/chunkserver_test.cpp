#include "gfs/chunkserver/chunkserver.hpp"
#include "gfs/chunkserver/storage/chunk_file.hpp"
#include "gfs/chunkserver/storage/chunk_storage.hpp"
#include "gfs/chunkserver/storage/storage_manager.hpp"
#include "gfs/common/constants.hpp"
#include "gfs/common/types.hpp"
#include "gfs/storage/file_handle.hpp"
#include "gfs/storage/local_filesystem.hpp"

#include <gtest/gtest.h>

#include <chrono>
#include <cstdint>
#include <filesystem>
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
            ("gfs_phase4_test_" +
             std::to_string(timestamp));

        std::filesystem::create_directories(path_);
    }

    ~TemporaryDirectory() {
        std::error_code error;
        std::filesystem::remove_all(path_, error);
    }

    const std::string& Path() const noexcept {
        path_string_ = path_.string();
        return path_string_;
    }

private:
    std::filesystem::path path_;
    mutable std::string path_string_;
};

TEST(LocalFilesystemTest, CreatesDirectories) {
    TemporaryDirectory temp;

    const std::string directory =
        gfs::storage::LocalFilesystem::JoinPath(
            temp.Path(),
            "chunks");

    EXPECT_TRUE(
        gfs::storage::LocalFilesystem::CreateDirectory(
            directory));

    EXPECT_TRUE(
        gfs::storage::LocalFilesystem::DirectoryExists(
            directory));
}

TEST(LocalFilesystemTest, WritesAndReadsFile) {
    TemporaryDirectory temp;

    const std::string path =
        gfs::storage::LocalFilesystem::JoinPath(
            temp.Path(),
            "test.dat");

    ASSERT_TRUE(
        gfs::storage::LocalFilesystem::CreateDirectory(
            temp.Path()));

    gfs::storage::FileHandle handle;

    ASSERT_TRUE(
        handle.Open(
            path,
            gfs::storage::FileHandle::OpenMode::ReadWrite,
            true,
            true));

    const std::vector<std::uint8_t> input{
        'G', 'F', 'S', '4'
    };

    const auto written =
        handle.WriteAt(
            0,
            input.data(),
            input.size());

    ASSERT_TRUE(written.has_value());
    EXPECT_EQ(*written, input.size());

    std::vector<std::uint8_t> output(input.size());

    const auto read =
        handle.ReadAt(
            0,
            output.data(),
            output.size());

    ASSERT_TRUE(read.has_value());
    EXPECT_EQ(*read, input.size());
    EXPECT_EQ(output, input);

    EXPECT_TRUE(handle.Close());
}

TEST(LocalFilesystemTest, ReportsFileSize) {
    TemporaryDirectory temp;

    const std::string path =
        gfs::storage::LocalFilesystem::JoinPath(
            temp.Path(),
            "size.dat");

    gfs::storage::FileHandle handle;

    ASSERT_TRUE(
        handle.Open(
            path,
            gfs::storage::FileHandle::OpenMode::ReadWrite,
            true,
            true));

    const std::vector<std::uint8_t> data(128, 7);

    ASSERT_TRUE(
        handle.WriteAt(
            0,
            data.data(),
            data.size())
        .has_value());

    const auto size = handle.Size();

    ASSERT_TRUE(size.has_value());
    EXPECT_EQ(*size, 128U);

    handle.Close();
}

TEST(LocalFilesystemTest, DeletesFile) {
    TemporaryDirectory temp;

    const std::string path =
        gfs::storage::LocalFilesystem::JoinPath(
            temp.Path(),
            "delete.dat");

    gfs::storage::FileHandle handle;

    ASSERT_TRUE(
        handle.Open(
            path,
            gfs::storage::FileHandle::OpenMode::ReadWrite,
            true,
            true));

    ASSERT_TRUE(handle.Close());
    ASSERT_TRUE(
        gfs::storage::LocalFilesystem::FileExists(path));

    EXPECT_TRUE(
        gfs::storage::LocalFilesystem::RemoveFile(path));

    EXPECT_FALSE(
        gfs::storage::LocalFilesystem::FileExists(path));
}

TEST(FileHandleTest, SupportsRAIIAndMove) {
    TemporaryDirectory temp;

    const std::string path =
        gfs::storage::LocalFilesystem::JoinPath(
            temp.Path(),
            "handle.dat");

    gfs::storage::FileHandle first;

    ASSERT_TRUE(
        first.Open(
            path,
            gfs::storage::FileHandle::OpenMode::ReadWrite,
            true,
            true));

    EXPECT_TRUE(first.IsOpen());

    gfs::storage::FileHandle second(
        std::move(first));

    EXPECT_FALSE(first.IsOpen());
    EXPECT_TRUE(second.IsOpen());

    EXPECT_TRUE(second.Close());
}

TEST(FileHandleTest, SupportsPositioning) {
    TemporaryDirectory temp;

    const std::string path =
        gfs::storage::LocalFilesystem::JoinPath(
            temp.Path(),
            "position.dat");

    gfs::storage::FileHandle handle;

    ASSERT_TRUE(
        handle.Open(
            path,
            gfs::storage::FileHandle::OpenMode::ReadWrite,
            true,
            true));

    const std::vector<std::uint8_t> data{
        1, 2, 3, 4, 5
    };

    ASSERT_TRUE(
        handle.Write(
            data.data(),
            data.size())
        .has_value());

    ASSERT_TRUE(handle.Seek(2));

    const auto position = handle.Tell();

    ASSERT_TRUE(position.has_value());
    EXPECT_EQ(*position, 2U);
}

TEST(ChunkFileTest, CreatesChunkFile) {
    TemporaryDirectory temp;

    const gfs::ChunkHandle handle = 42;
    const std::string path =
        gfs::storage::LocalFilesystem::JoinPath(
            temp.Path(),
            "chunk_42");

    gfs::chunkserver::storage::ChunkFile chunk(
        handle,
        path);

    EXPECT_TRUE(chunk.Create());
    EXPECT_TRUE(chunk.IsOpen());
    EXPECT_TRUE(chunk.Exists());
    EXPECT_EQ(chunk.GetHandle(), handle);
    EXPECT_EQ(chunk.GetPath(), path);
    EXPECT_EQ(chunk.GetSize(), 0U);
}

TEST(ChunkFileTest, WritesAndReadsChunk) {
    TemporaryDirectory temp;

    gfs::chunkserver::storage::ChunkFile chunk(
        10,
        gfs::storage::LocalFilesystem::JoinPath(
            temp.Path(),
            "chunk_10"));

    ASSERT_TRUE(chunk.Create());

    const std::vector<std::uint8_t> input{
        'h', 'e', 'l', 'l', 'o'
    };

    ASSERT_TRUE(chunk.Write(0, input));

    EXPECT_EQ(chunk.GetSize(), input.size());

    std::vector<std::uint8_t> output;

    ASSERT_TRUE(
        chunk.Read(
            0,
            input.size(),
            output));

    EXPECT_EQ(output, input);
}

TEST(ChunkFileTest, SupportsOffsetWrites) {
    TemporaryDirectory temp;

    gfs::chunkserver::storage::ChunkFile chunk(
        11,
        gfs::storage::LocalFilesystem::JoinPath(
            temp.Path(),
            "chunk_11"));

    ASSERT_TRUE(chunk.Create());

    const std::vector<std::uint8_t> first{
        'a', 'b', 'c'
    };

    const std::vector<std::uint8_t> second{
        'x', 'y'
    };

    ASSERT_TRUE(chunk.Write(0, first));
    ASSERT_TRUE(chunk.Write(1, second));

    std::vector<std::uint8_t> output;

    ASSERT_TRUE(chunk.Read(0, 3, output));

    ASSERT_EQ(output.size(), 3U);
    EXPECT_EQ(output[0], 'a');
    EXPECT_EQ(output[1], 'x');
    EXPECT_EQ(output[2], 'y');
}

TEST(ChunkFileTest, RejectsReadsBeyondCurrentSize) {
    TemporaryDirectory temp;

    gfs::chunkserver::storage::ChunkFile chunk(
        12,
        gfs::storage::LocalFilesystem::JoinPath(
            temp.Path(),
            "chunk_12"));

    ASSERT_TRUE(chunk.Create());

    const std::vector<std::uint8_t> data{
        1, 2, 3
    };

    ASSERT_TRUE(chunk.Write(0, data));

    std::vector<std::uint8_t> output;

    EXPECT_FALSE(
        chunk.Read(
            2,
            2,
            output));
}

TEST(ChunkFileTest, RejectsWritesBeyondGFSChunkSize) {
    TemporaryDirectory temp;

    gfs::chunkserver::storage::ChunkFile chunk(
        13,
        gfs::storage::LocalFilesystem::JoinPath(
            temp.Path(),
            "chunk_13"));

    ASSERT_TRUE(chunk.Create());

    const std::size_t invalid_size =
        gfs::constants::kChunkSize + 1;

    std::vector<std::uint8_t> data(
        invalid_size,
        0);

    EXPECT_FALSE(chunk.Write(0, data));
    EXPECT_EQ(chunk.GetSize(), 0U);
}

TEST(ChunkFileTest, DeletesChunkFile) {
    TemporaryDirectory temp;

    const std::string path =
        gfs::storage::LocalFilesystem::JoinPath(
            temp.Path(),
            "chunk_14");

    gfs::chunkserver::storage::ChunkFile chunk(
        14,
        path);

    ASSERT_TRUE(chunk.Create());
    ASSERT_TRUE(chunk.Exists());

    EXPECT_TRUE(chunk.Delete());
    EXPECT_FALSE(chunk.Exists());
    EXPECT_FALSE(chunk.IsOpen());
}

TEST(ChunkFileTest, TruncatesChunk) {
    TemporaryDirectory temp;

    gfs::chunkserver::storage::ChunkFile chunk(
        15,
        gfs::storage::LocalFilesystem::JoinPath(
            temp.Path(),
            "chunk_15"));

    ASSERT_TRUE(chunk.Create());

    const std::vector<std::uint8_t> data(
        100,
        1);

    ASSERT_TRUE(chunk.Write(0, data));
    EXPECT_EQ(chunk.GetSize(), 100U);

    ASSERT_TRUE(chunk.Truncate(25));
    EXPECT_EQ(chunk.GetSize(), 25U);
}

TEST(ChunkStorageTest, InitializesStorageDirectory) {
    TemporaryDirectory temp;

    const std::string storage_directory =
        gfs::storage::LocalFilesystem::JoinPath(
            temp.Path(),
            "chunks");

    gfs::chunkserver::storage::ChunkStorage storage(
        storage_directory);

    EXPECT_TRUE(storage.Initialize());

    EXPECT_TRUE(
        gfs::storage::LocalFilesystem::DirectoryExists(
            storage_directory));
}

TEST(ChunkStorageTest, CreatesAndFindsChunks) {
    TemporaryDirectory temp;

    gfs::chunkserver::storage::ChunkStorage storage(
        temp.Path());

    ASSERT_TRUE(storage.Initialize());

    EXPECT_TRUE(storage.CreateChunk(100));
    EXPECT_TRUE(storage.ChunkExists(100));
    EXPECT_EQ(storage.ChunkCount(), 1U);

    auto chunk = storage.GetChunk(100);

    ASSERT_NE(chunk, nullptr);
    EXPECT_EQ(chunk->GetHandle(), 100U);
}

TEST(ChunkStorageTest, RejectsDuplicateChunk) {
    TemporaryDirectory temp;

    gfs::chunkserver::storage::ChunkStorage storage(
        temp.Path());

    ASSERT_TRUE(storage.Initialize());

    EXPECT_TRUE(storage.CreateChunk(101));
    EXPECT_FALSE(storage.CreateChunk(101));
}

TEST(ChunkStorageTest, ReadsAndWritesChunks) {
    TemporaryDirectory temp;

    gfs::chunkserver::storage::ChunkStorage storage(
        temp.Path());

    ASSERT_TRUE(storage.Initialize());
    ASSERT_TRUE(storage.CreateChunk(102));

    const std::vector<std::uint8_t> input{
        10, 20, 30, 40
    };

    ASSERT_TRUE(
        storage.WriteChunk(
            102,
            0,
            input));

    std::vector<std::uint8_t> output;

    ASSERT_TRUE(
        storage.ReadChunk(
            102,
            0,
            input.size(),
            output));

    EXPECT_EQ(output, input);
    EXPECT_EQ(storage.GetChunkSize(102), 4U);
}

TEST(ChunkStorageTest, DeletesChunks) {
    TemporaryDirectory temp;

    gfs::chunkserver::storage::ChunkStorage storage(
        temp.Path());

    ASSERT_TRUE(storage.Initialize());
    ASSERT_TRUE(storage.CreateChunk(103));

    EXPECT_TRUE(storage.DeleteChunk(103));
    EXPECT_FALSE(storage.ChunkExists(103));
    EXPECT_EQ(storage.ChunkCount(), 0U);
}

TEST(ChunkStorageTest, ListsChunks) {
    TemporaryDirectory temp;

    gfs::chunkserver::storage::ChunkStorage storage(
        temp.Path());

    ASSERT_TRUE(storage.Initialize());

    ASSERT_TRUE(storage.CreateChunk(3));
    ASSERT_TRUE(storage.CreateChunk(1));
    ASSERT_TRUE(storage.CreateChunk(2));

    const auto chunks = storage.ListChunks();

    ASSERT_EQ(chunks.size(), 3U);
    EXPECT_EQ(chunks[0], 1U);
    EXPECT_EQ(chunks[1], 2U);
    EXPECT_EQ(chunks[2], 3U);
}

TEST(ChunkStorageTest, ReopensExistingChunks) {
    TemporaryDirectory temp;

    {
        gfs::chunkserver::storage::ChunkStorage storage(
            temp.Path());

        ASSERT_TRUE(storage.Initialize());
        ASSERT_TRUE(storage.CreateChunk(200));

        const std::vector<std::uint8_t> data{
            1, 2, 3, 4
        };

        ASSERT_TRUE(
            storage.WriteChunk(
                200,
                0,
                data));
    }

    {
        gfs::chunkserver::storage::ChunkStorage storage(
            temp.Path());

        ASSERT_TRUE(storage.Initialize());

        EXPECT_TRUE(storage.ChunkExists(200));
        EXPECT_EQ(storage.ChunkCount(), 1U);
        EXPECT_TRUE(storage.GetChunk(200) != nullptr);

        std::vector<std::uint8_t> data;

        ASSERT_TRUE(
            storage.ReadChunk(
                200,
                0,
                4,
                data));

        EXPECT_EQ(
            data,
            std::vector<std::uint8_t>({
                1, 2, 3, 4
            }));
    }
}

TEST(StorageManagerTest, ManagesLocalChunks) {
    TemporaryDirectory temp;

    gfs::chunkserver::storage::StorageManager manager(
        temp.Path());

    ASSERT_TRUE(manager.Initialize());

    EXPECT_TRUE(manager.CreateChunk(300));
    EXPECT_TRUE(manager.ChunkExists(300));

    const std::vector<std::uint8_t> data{
        5, 6, 7
    };

    ASSERT_TRUE(
        manager.WriteChunk(
            300,
            0,
            data));

    EXPECT_EQ(manager.GetChunkSize(300), 3U);

    std::vector<std::uint8_t> output;

    ASSERT_TRUE(
        manager.ReadChunk(
            300,
            0,
            3,
            output));

    EXPECT_EQ(output, data);

    EXPECT_TRUE(manager.DeleteChunk(300));
    EXPECT_FALSE(manager.ChunkExists(300));
}

TEST(StorageManagerTest, ListsManagedChunks) {
    TemporaryDirectory temp;

    gfs::chunkserver::storage::StorageManager manager(
        temp.Path());

    ASSERT_TRUE(manager.Initialize());

    ASSERT_TRUE(manager.CreateChunk(401));
    ASSERT_TRUE(manager.CreateChunk(402));

    const auto chunks = manager.ListChunks();

    ASSERT_EQ(chunks.size(), 2U);
    EXPECT_EQ(chunks[0], 401U);
    EXPECT_EQ(chunks[1], 402U);
}

TEST(ChunkserverTest, InitializesAndIdentifiesServer) {
    TemporaryDirectory temp;

    gfs::chunkserver::Chunkserver server(
        77,
        temp.Path());

    EXPECT_EQ(server.GetServerId(), 77U);
    EXPECT_TRUE(server.Initialize());
    EXPECT_EQ(
        server.GetStorageDirectory(),
        temp.Path());
}

TEST(ChunkserverTest, PerformsLocalChunkOperations) {
    TemporaryDirectory temp;

    gfs::chunkserver::Chunkserver server(
        78,
        temp.Path());

    ASSERT_TRUE(server.Initialize());
    ASSERT_TRUE(server.CreateChunk(500));

    const std::vector<std::uint8_t> input{
        8, 9, 10, 11
    };

    ASSERT_TRUE(
        server.WriteChunk(
            500,
            0,
            input));

    std::vector<std::uint8_t> output;

    ASSERT_TRUE(
        server.ReadChunk(
            500,
            0,
            input.size(),
            output));

    EXPECT_EQ(output, input);
    EXPECT_EQ(server.GetChunkSize(500), 4U);
    EXPECT_TRUE(server.ChunkExists(500));

    EXPECT_TRUE(server.DeleteChunk(500));
    EXPECT_FALSE(server.ChunkExists(500));
}

TEST(ChunkserverTest, ExposesStorageManager) {
    TemporaryDirectory temp;

    gfs::chunkserver::Chunkserver server(
        79,
        temp.Path());

    ASSERT_TRUE(server.Initialize());
    ASSERT_TRUE(server.CreateChunk(600));

    auto& storage = server.GetStorageManager();

    EXPECT_EQ(storage.ChunkCount(), 1U);
    EXPECT_TRUE(storage.ChunkExists(600));
}

TEST(ChunkserverTest, PreservesChunkSizeLimit) {
    TemporaryDirectory temp;

    gfs::chunkserver::Chunkserver server(
        80,
        temp.Path());

    ASSERT_TRUE(server.Initialize());
    ASSERT_TRUE(server.CreateChunk(700));

    const std::vector<std::uint8_t> data(
        gfs::constants::kChunkSize + 1,
        0);

    EXPECT_FALSE(
        server.WriteChunk(
            700,
            0,
            data));
}

}  // namespace