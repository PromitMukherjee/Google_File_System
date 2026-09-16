#include "gfs/client/gfs_client.hpp"
#include "gfs/client/io/reader.hpp"
#include "gfs/client/io/writer.hpp"
#include "gfs/client/metadata/chunk_location_cache.hpp"
#include "gfs/client/metadata/master_client.hpp"
#include "gfs/common/constants.hpp"

#include <gtest/gtest.h>

#include <algorithm>
#include <cstdint>
#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

namespace {

using gfs::ChunkHandle;
using gfs::ChunkIndex;
using gfs::FilePath;
using gfs::ServerId;
using gfs::client::GFSClient;
using gfs::client::io::Reader;
using gfs::client::io::Writer;
using gfs::client::metadata::ChunkLocation;
using gfs::client::metadata::ChunkLocationCache;
using gfs::client::metadata::ChunkLocationCacheEntry;
using gfs::client::metadata::ChunkMetadata;
using gfs::client::metadata::FileMetadata;
using gfs::client::metadata::MasterClient;

class FakeCluster {
public:
    struct Chunk {
        ChunkMetadata metadata;
        std::string data;
    };

    std::unordered_map<FilePath, FileMetadata> files;
    std::unordered_map<ChunkHandle, Chunk> chunks;

    ChunkHandle next_handle = 1;

    bool CreateFile(const FilePath& path) {
        if (path.empty() || files.contains(path)) {
            return false;
        }

        files.emplace(
            path,
            FileMetadata{path, 0, 0});

        return true;
    }

    std::optional<FileMetadata> LookupFile(
        const FilePath& path) const {
        const auto it = files.find(path);

        if (it == files.end()) {
            return std::nullopt;
        }

        return it->second;
    }

    std::optional<ChunkMetadata> LookupChunk(
        const FilePath& path,
        ChunkIndex index) const {
        const auto file_it = files.find(path);

        if (file_it == files.end() ||
            index >= file_it->second.chunk_count) {
            return std::nullopt;
        }

        const ChunkHandle handle =
            static_cast<ChunkHandle>(index + 1);

        const auto chunk_it = chunks.find(handle);

        if (chunk_it == chunks.end()) {
            return std::nullopt;
        }

        return chunk_it->second.metadata;
    }

    std::optional<ChunkMetadata> AllocateChunk(
        const FilePath& path,
        ChunkIndex index) {
        auto file_it = files.find(path);

        if (file_it == files.end()) {
            return std::nullopt;
        }

        if (index > file_it->second.chunk_count) {
            return std::nullopt;
        }

        if (index < file_it->second.chunk_count) {
            return LookupChunk(path, index);
        }

        const ChunkHandle handle = next_handle++;

        ChunkMetadata metadata;
        metadata.handle = handle;
        metadata.index = index;
        metadata.locations.push_back(
            ChunkLocation{1, "chunkserver-1"});

        chunks.emplace(
            handle,
            Chunk{metadata, {}});

        ++file_it->second.chunk_count;

        return metadata;
    }

    bool UpdateFileSize(
        const FilePath& path,
        std::uint64_t size) {
        const auto it = files.find(path);

        if (it == files.end()) {
            return false;
        }

        it->second.size =
            std::max(it->second.size, size);

        return true;
    }

    bool Read(
        const ChunkLocation& location,
        ChunkHandle handle,
        std::uint64_t offset,
        std::size_t length,
        std::string& output) {
        if (location.server_id != 1) {
            return false;
        }

        const auto it = chunks.find(handle);

        if (it == chunks.end()) {
            return false;
        }

        if (offset > it->second.data.size()) {
            return false;
        }

        const std::size_t available =
            it->second.data.size() -
            static_cast<std::size_t>(offset);

        const std::size_t actual =
            std::min(length, available);

        output = it->second.data.substr(
            static_cast<std::size_t>(offset),
            actual);

        return true;
    }

    bool Write(
        const ChunkLocation& location,
        ChunkHandle handle,
        std::uint64_t offset,
        const std::string& data) {
        if (location.server_id != 1) {
            return false;
        }

        auto it = chunks.find(handle);

        if (it == chunks.end()) {
            return false;
        }

        if (offset + data.size() >
            gfs::constants::kChunkSize) {
            return false;
        }

        const std::size_t required_size =
            static_cast<std::size_t>(
                offset + data.size());

        if (it->second.data.size() < required_size) {
            it->second.data.resize(required_size, '\0');
        }

        std::copy(
            data.begin(),
            data.end(),
            it->second.data.begin() +
                static_cast<std::ptrdiff_t>(offset));

        return true;
    }

    std::unique_ptr<MasterClient> CreateMasterClient() {
        auto master = std::make_unique<MasterClient>("master");

        master->SetFileLookup(
            [this](const FilePath& path) {
                return LookupFile(path);
            });

        master->SetChunkLookup(
            [this](const FilePath& path, ChunkIndex index) {
                return LookupChunk(path, index);
            });

        master->SetChunkAllocator(
            [this](const FilePath& path, ChunkIndex index) {
                return AllocateChunk(path, index);
            });

        return master;
    }
};

TEST(MasterClientTest, FileLookup) {
    FakeCluster cluster;
    ASSERT_TRUE(cluster.CreateFile("/file"));

    auto master = cluster.CreateMasterClient();

    const auto file =
        master->LookupFile("/file");

    ASSERT_TRUE(file.has_value());
    EXPECT_EQ(file->path, "/file");
    EXPECT_EQ(file->size, 0U);
    EXPECT_EQ(file->chunk_count, 0U);

    EXPECT_FALSE(
        master->LookupFile("/missing").has_value());
}

TEST(MasterClientTest, ChunkAllocationAndLookup) {
    FakeCluster cluster;
    ASSERT_TRUE(cluster.CreateFile("/file"));

    auto master = cluster.CreateMasterClient();

    const auto allocated =
        master->AllocateChunk("/file", 0);

    ASSERT_TRUE(allocated.has_value());
    EXPECT_NE(allocated->handle, 0U);
    ASSERT_EQ(allocated->locations.size(), 1U);
    EXPECT_EQ(allocated->locations.front().server_id, 1U);

    const auto handle =
        master->LookupChunkHandle("/file", 0);

    ASSERT_TRUE(handle.has_value());
    EXPECT_EQ(*handle, allocated->handle);

    const auto locations =
        master->LookupChunkLocations("/file", 0);

    ASSERT_EQ(locations.size(), 1U);
    EXPECT_EQ(locations.front().server_id, 1U);
}

TEST(ChunkLocationCacheTest, InsertLookupInvalidate) {
    ChunkLocationCache cache;

    std::vector<ChunkLocation> locations{
        {1, "chunkserver-1"},
        {2, "chunkserver-2"}};

    ASSERT_TRUE(
        cache.Insert(
            "/file",
            0,
            42,
            locations));

    const auto entry =
        cache.Lookup("/file", 0);

    ASSERT_TRUE(entry.has_value());
    EXPECT_EQ(entry->handle, 42U);
    EXPECT_EQ(entry->chunk_index, 0U);
    EXPECT_EQ(entry->locations, locations);

    EXPECT_EQ(
        cache.LookupHandle("/file", 0),
        std::optional<ChunkHandle>(42));

    EXPECT_EQ(
        cache.LookupLocations("/file", 0),
        locations);

    EXPECT_TRUE(cache.Invalidate("/file", 0));
    EXPECT_FALSE(cache.Lookup("/file", 0).has_value());
    EXPECT_TRUE(cache.Empty());
}

TEST(ChunkLocationCacheTest, FileInvalidation) {
    ChunkLocationCache cache;

    ASSERT_TRUE(
        cache.Insert("/file", 0, 10, {}));
    ASSERT_TRUE(
        cache.Insert("/file", 1, 11, {}));
    ASSERT_TRUE(
        cache.Insert("/other", 0, 12, {}));

    cache.InvalidateFile("/file");

    EXPECT_FALSE(
        cache.Lookup("/file", 0).has_value());
    EXPECT_FALSE(
        cache.Lookup("/file", 1).has_value());

    EXPECT_TRUE(
        cache.Lookup("/other", 0).has_value());

    EXPECT_EQ(cache.Size(), 1U);
}

TEST(ReaderTest, ReadsThroughChunkserver) {
    FakeCluster cluster;
    ASSERT_TRUE(cluster.CreateFile("/file"));

    auto master = cluster.CreateMasterClient();
    ChunkLocationCache cache;

    Reader reader(
        *master,
        cache,
        [&cluster](
            const ChunkLocation& location,
            ChunkHandle handle,
            std::uint64_t offset,
            std::size_t length,
            std::string& output) {
            return cluster.Read(
                location,
                handle,
                offset,
                length,
                output);
        });

    const auto chunk =
        cluster.AllocateChunk("/file", 0);

    ASSERT_TRUE(chunk.has_value());

    ASSERT_TRUE(
        cluster.Write(
            chunk->locations.front(),
            chunk->handle,
            0,
            "hello world"));

    ASSERT_TRUE(
        cluster.UpdateFileSize(
            "/file",
            11));

    std::string output;

    EXPECT_TRUE(
        reader.Read(
            "/file",
            0,
            5,
            output));

    EXPECT_EQ(output, "hello");

    EXPECT_TRUE(
        reader.Read(
            "/file",
            6,
            5,
            output));

    EXPECT_EQ(output, "world");
}

TEST(ReaderTest, RejectsInvalidOffsets) {
    FakeCluster cluster;
    ASSERT_TRUE(cluster.CreateFile("/file"));

    auto master = cluster.CreateMasterClient();
    ChunkLocationCache cache;

    Reader reader(
        *master,
        cache,
        [&cluster](
            const ChunkLocation& location,
            ChunkHandle handle,
            std::uint64_t offset,
            std::size_t length,
            std::string& output) {
            return cluster.Read(
                location,
                handle,
                offset,
                length,
                output);
        });

    std::string output;

    EXPECT_FALSE(
        reader.Read(
            "/file",
            1,
            5,
            output));
}

TEST(WriterTest, WritesAndAllocatesChunks) {
    FakeCluster cluster;
    ASSERT_TRUE(cluster.CreateFile("/file"));

    auto master = cluster.CreateMasterClient();
    ChunkLocationCache cache;

    Writer writer(
        *master,
        cache,
        [&cluster](
            const ChunkLocation& location,
            ChunkHandle handle,
            std::uint64_t offset,
            const std::string& data) {
            return cluster.Write(
                location,
                handle,
                offset,
                data);
        },
        [&cluster](
            const FilePath& path,
            std::uint64_t size) {
            return cluster.UpdateFileSize(
                path,
                size);
        });

    EXPECT_TRUE(
        writer.Write(
            "/file",
            0,
            "hello world"));

    const auto file =
        cluster.LookupFile("/file");

    ASSERT_TRUE(file.has_value());
    EXPECT_EQ(file->size, 11U);
    EXPECT_EQ(file->chunk_count, 1U);

    const auto chunk =
        cluster.LookupChunk("/file", 0);

    ASSERT_TRUE(chunk.has_value());

    EXPECT_EQ(
        cluster.chunks.at(chunk->handle).data,
        "hello world");
}

TEST(WriterTest, HandlesChunkBoundary) {
    FakeCluster cluster;
    ASSERT_TRUE(cluster.CreateFile("/file"));

    auto master = cluster.CreateMasterClient();
    ChunkLocationCache cache;

    Writer writer(
        *master,
        cache,
        [&cluster](
            const ChunkLocation& location,
            ChunkHandle handle,
            std::uint64_t offset,
            const std::string& data) {
            return cluster.Write(
                location,
                handle,
                offset,
                data);
        },
        [&cluster](
            const FilePath& path,
            std::uint64_t size) {
            return cluster.UpdateFileSize(
                path,
                size);
        });

    const std::string first =
        "A" +
        std::string(
            gfs::constants::kChunkSize - 1,
            'B');

    ASSERT_TRUE(
        writer.Write(
            "/file",
            0,
            first));

    ASSERT_TRUE(
        writer.Write(
            "/file",
            gfs::constants::kChunkSize,
            "XYZ"));

    const auto file =
        cluster.LookupFile("/file");

    ASSERT_TRUE(file.has_value());
    EXPECT_EQ(
        file->size,
        gfs::constants::kChunkSize + 3);

    ASSERT_TRUE(
        cluster.LookupChunk("/file", 0).has_value());
    ASSERT_TRUE(
        cluster.LookupChunk("/file", 1).has_value());
}

TEST(GFSClientTest, CreateLookupReadWrite) {
    FakeCluster cluster;
    ASSERT_TRUE(cluster.CreateFile("/file"));

    auto master = cluster.CreateMasterClient();

    auto cache =
        std::make_unique<ChunkLocationCache>();

    GFSClient client(
        std::move(master),
        std::move(cache),
        [&cluster](
            const ChunkLocation& location,
            ChunkHandle handle,
            std::uint64_t offset,
            std::size_t length,
            std::string& output) {
            return cluster.Read(
                location,
                handle,
                offset,
                length,
                output);
        },
        [&cluster](
            const ChunkLocation& location,
            ChunkHandle handle,
            std::uint64_t offset,
            const std::string& data) {
            return cluster.Write(
                location,
                handle,
                offset,
                data);
        },
        [&cluster](
            const FilePath& path,
            std::uint64_t size) {
            return cluster.UpdateFileSize(
                path,
                size);
        });

    client.SetFileCreateFunction(
        [&cluster](const FilePath& path) {
            return cluster.CreateFile(path);
        });

    ASSERT_TRUE(client.FileExists("/file"));

    EXPECT_FALSE(client.FileExists("/missing"));

    ASSERT_TRUE(
        client.Write(
            "/file",
            0,
            "distributed data"));

    std::string output;

    ASSERT_TRUE(
        client.Read(
            "/file",
            0,
            17,
            output));

    EXPECT_EQ(output, "distributed data");

    const auto file =
        client.LookupFile("/file");

    ASSERT_TRUE(file.has_value());
    EXPECT_EQ(file->size, 16U);
    EXPECT_EQ(file->chunk_count, 1U);
}

TEST(GFSClientTest, FileCreation) {
    FakeCluster cluster;

    auto master = cluster.CreateMasterClient();

    auto cache =
        std::make_unique<ChunkLocationCache>();

    GFSClient client(
        std::move(master),
        std::move(cache),
        [](const ChunkLocation&,
           ChunkHandle,
           std::uint64_t,
           std::size_t,
           std::string&) {
            return false;
        },
        [](const ChunkLocation&,
           ChunkHandle,
           std::uint64_t,
           const std::string&) {
            return false;
        });

    client.SetFileCreateFunction(
        [&cluster](const FilePath& path) {
            return cluster.CreateFile(path);
        });

    EXPECT_TRUE(client.CreateFile("/new"));
    EXPECT_TRUE(client.FileExists("/new"));
    EXPECT_FALSE(client.CreateFile("/new"));
}

TEST(GFSClientTest, MultiChunkReadWrite) {
    FakeCluster cluster;
    ASSERT_TRUE(cluster.CreateFile("/large"));

    auto master = cluster.CreateMasterClient();

    auto cache =
        std::make_unique<ChunkLocationCache>();

    GFSClient client(
        std::move(master),
        std::move(cache),
        [&cluster](
            const ChunkLocation& location,
            ChunkHandle handle,
            std::uint64_t offset,
            std::size_t length,
            std::string& output) {
            return cluster.Read(
                location,
                handle,
                offset,
                length,
                output);
        },
        [&cluster](
            const ChunkLocation& location,
            ChunkHandle handle,
            std::uint64_t offset,
            const std::string& data) {
            return cluster.Write(
                location,
                handle,
                offset,
                data);
        },
        [&cluster](
            const FilePath& path,
            std::uint64_t size) {
            return cluster.UpdateFileSize(
                path,
                size);
        });

    const std::size_t prefix =
        gfs::constants::kChunkSize - 4;

    const std::string data =
        std::string(prefix, 'A') +
        "BBBB";

    ASSERT_TRUE(
        client.Write(
            "/large",
            0,
            data));

    std::string output;

    ASSERT_TRUE(
        client.Read(
            "/large",
            prefix - 2,
            6,
            output));

    EXPECT_EQ(
        output,
        "AABBBB");

    const auto file =
        client.LookupFile("/large");

    ASSERT_TRUE(file.has_value());
    EXPECT_EQ(
        file->size,
        gfs::constants::kChunkSize);
    EXPECT_EQ(file->chunk_count, 1U);
}

}  // namespace