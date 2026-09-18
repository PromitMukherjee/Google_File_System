#include "gfs/chunkserver/chunkserver.hpp"
#include "gfs/client/gfs_client.hpp"
#include "gfs/client/io/reader.hpp"
#include "gfs/client/metadata/master_client.hpp"
#include "gfs/common/constants.hpp"
#include "gfs/master/master.hpp"

#include <gtest/gtest.h>

#include <algorithm>
#include <cstdint>
#include <filesystem>
#include <memory>
#include <string>
#include <vector>

namespace {

using gfs::ChunkHandle;
using gfs::ChunkIndex;
using gfs::ChunkVersion;
using gfs::FilePath;
using gfs::ServerId;

using gfs::chunkserver::Chunkserver;
using gfs::chunkserver::mutation::Mutation;
using gfs::client::GFSClient;
using gfs::client::metadata::ChunkLocation;
using gfs::client::metadata::ChunkLocationCache;
using gfs::client::metadata::ChunkMetadata;
using gfs::client::metadata::FileMetadata;
using gfs::client::metadata::MasterClient;
using gfs::master::Master;

class TempDir {
public:
    TempDir() {
        path_ =
            std::filesystem::temp_directory_path() /
            ("gfs_phase13_" +
             std::to_string(
                 reinterpret_cast<std::uintptr_t>(
                     this)));

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

struct Fixture {
    TempDir primary_dir;
    TempDir secondary_dir;

    Chunkserver primary;
    Chunkserver secondary;
    Master master;

    Fixture()
        : primary(
              1,
              primary_dir.Path()),
          secondary(
              2,
              secondary_dir.Path()),
          master(2) {
        static_cast<void>(
            primary.Initialize());

        static_cast<void>(
            secondary.Initialize());
    }

    std::unique_ptr<MasterClient>
    CreateMasterClient() {
        auto client =
            std::make_unique<
                MasterClient>("master");

        client->SetFileLookup(
            [this](const FilePath& path) {
                const auto file =
                    master.GetFile(path);

                if (!file.has_value()) {
                    return std::optional<
                        FileMetadata>{};
                }

                return std::optional<
                    FileMetadata>(
                    FileMetadata{
                        file->GetPath(),
                        file->GetSize(),
                        file->ChunkCount()});
            });

        client->SetChunkLookup(
            [this](
                const FilePath& path,
                ChunkIndex index) {
                return LookupChunk(
                    path,
                    index);
            });

        client->SetChunkAllocator(
            [this](
                const FilePath& path,
                ChunkIndex index) {
                const auto count =
                    master.GetChunkCount(path);

                if (!count.has_value() ||
                    index != *count) {
                    return std::optional<
                        ChunkMetadata>{};
                }

                const auto handle =
                    master.AllocateChunk(path);

                if (!handle.has_value()) {
                    return std::optional<
                        ChunkMetadata>{};
                }

                if (!master.RegisterReplica(
                        *handle,
                        1,
                        true) ||
                    !master.RegisterReplica(
                        *handle,
                        2,
                        false)) {
                    return std::optional<
                        ChunkMetadata>{};
                }

                if (!primary.CreateChunk(
                        *handle) ||
                    !secondary.CreateChunk(
                        *handle)) {
                    return std::optional<
                        ChunkMetadata>{};
                }

                return LookupChunk(
                    path,
                    index);
            });

        return client;
    }

    std::optional<ChunkMetadata>
    LookupChunk(
        const FilePath& path,
        ChunkIndex index) {
        const auto chunks =
            master.GetFileChunks(path);

        if (index >= chunks.size()) {
            return std::nullopt;
        }

        const ChunkHandle handle =
            chunks[
                static_cast<std::size_t>(
                    index)];

        const auto chunk =
            master.GetChunkInfo(handle);

        if (!chunk.has_value()) {
            return std::nullopt;
        }

        ChunkMetadata result;

        result.handle = handle;
        result.index = index;
        result.version =
            chunk->GetVersion();

        for (const ServerId server_id :
             master.GetReplicaServers(handle)) {
            result.locations.push_back(
                ChunkLocation{
                    server_id,
                    "chunkserver-" +
                        std::to_string(
                            server_id)});
        }

        return result;
    }

    bool Write(
        const ChunkLocation& location,
        ChunkHandle handle,
        std::uint64_t offset,
        const std::string& data) {
        Chunkserver* server =
            location.server_id == 1
                ? &primary
                : location.server_id == 2
                    ? &secondary
                    : nullptr;

        if (server == nullptr) {
            return false;
        }

        const auto chunk =
            master.GetChunkInfo(handle);

        if (!chunk.has_value()) {
            return false;
        }

        return server
            ->GetMutationManager()
            .ExecutePrimaryMutation(
                handle,
                chunk->GetVersion(),
                offset,
                data,
                [&](const Mutation& mutation) {
                    if (location.server_id != 1) {
                        return false;
                    }

                    return secondary
                        .GetMutationManager()
                        .ApplyMutation(
                            mutation);
                });
    }

    bool Read(
        const ChunkLocation& location,
        ChunkHandle handle,
        std::uint64_t offset,
        std::size_t length,
        std::string& output) {
        if (location.server_id == 1) {
            return primary.ReadChunk(
                handle,
                offset,
                length,
                output);
        }

        if (location.server_id == 2) {
            return secondary.ReadChunk(
                handle,
                offset,
                length,
                output);
        }

        return false;
    }

    bool CloneReplicas(
        ChunkHandle source,
        ChunkHandle destination,
        const std::vector<ServerId>& replicas) {
        if (replicas.empty()) {
            return false;
        }

        if (!primary.GetCloneManager().Clone(
                source,
                destination)) {
            return false;
        }

        const std::uint64_t source_size =
            primary.GetChunkSize(source);

        std::string source_data;

        if (!primary.ReadChunk(
                source,
                0,
                static_cast<std::size_t>(
                    source_size),
                source_data)) {
            return false;
        }

        for (const ServerId server_id :
             replicas) {
            if (server_id == 1) {
                continue;
            }

            if (server_id == 2) {
                if (!secondary.CreateChunk(
                        destination)) {
                    return false;
                }

                if (!source_data.empty() &&
                    !secondary.WriteChunk(
                        destination,
                        0,
                        source_data)) {
                    return false;
                }
            }
        }

        return true;
    }

    void PrepareInitialChunk(
        const FilePath& path,
        ChunkIndex index,
        const std::string& data) {
        const auto chunk =
            master.AllocateChunk(path);

        ASSERT_TRUE(chunk.has_value());

        ASSERT_EQ(index, 0U);

        ASSERT_TRUE(
            master.RegisterReplica(
                *chunk,
                1,
                true));

        ASSERT_TRUE(
            master.RegisterReplica(
                *chunk,
                2,
                false));

        ASSERT_TRUE(
            primary.CreateChunk(*chunk));

        ASSERT_TRUE(
            secondary.CreateChunk(*chunk));

        ASSERT_TRUE(
            primary.GetMutationManager()
                .ExecutePrimaryMutation(
                    *chunk,
                    1,
                    0,
                    data,
                    [&](const Mutation& mutation) {
                        return secondary
                            .GetMutationManager()
                            .ApplyMutation(
                                mutation);
                    }));

        ASSERT_TRUE(
            master.SetChunkSize(
                *chunk,
                data.size()));

        ASSERT_TRUE(
            master.UpdateFileSize(
                path,
                data.size()));
    }
};

TEST(
    SnapshotTest,
    SnapshotCreationAndIndependentMetadata) {
    Fixture fixture;

    ASSERT_TRUE(
        fixture.master.CreateFile(
            "/source",
            2));

    fixture.PrepareInitialChunk(
        "/source",
        0,
        "original-data");

    ASSERT_TRUE(
        fixture.master.CreateSnapshot(
            "/source",
            "/snapshot"));

    ASSERT_TRUE(
        fixture.master.FileExists(
            "/snapshot"));

    const auto source =
        fixture.master.GetFile(
            "/source");

    const auto snapshot =
        fixture.master.GetFile(
            "/snapshot");

    ASSERT_TRUE(source.has_value());
    ASSERT_TRUE(snapshot.has_value());

    EXPECT_EQ(
        source->GetPath(),
        "/source");

    EXPECT_EQ(
        snapshot->GetPath(),
        "/snapshot");

    ASSERT_EQ(
        source->GetChunkHandles().size(),
        1U);

    ASSERT_EQ(
        snapshot->GetChunkHandles().size(),
        1U);

    EXPECT_EQ(
        source->GetChunkHandles()[0],
        snapshot->GetChunkHandles()[0]);

    EXPECT_EQ(
        fixture.master.GetChunkReferenceCount(
            source->GetChunkHandles()[0]),
        2U);
}

TEST(
    SnapshotTest,
    SnapshotDoesNotCopyPhysicalChunkData) {
    Fixture fixture;

    ASSERT_TRUE(
        fixture.master.CreateFile(
            "/source",
            2));

    fixture.PrepareInitialChunk(
        "/source",
        0,
        "original-data");

    const auto primary_count =
        fixture.primary.ChunkCount();

    const auto secondary_count =
        fixture.secondary.ChunkCount();

    ASSERT_TRUE(
        fixture.master.CreateSnapshot(
            "/source",
            "/snapshot"));

    EXPECT_EQ(
        fixture.primary.ChunkCount(),
        primary_count);

    EXPECT_EQ(
        fixture.secondary.ChunkCount(),
        secondary_count);
}

TEST(
    SnapshotTest,
    SnapshotReadsOriginalDataAndSourceUsesCopyOnWrite) {
    Fixture fixture;

    ASSERT_TRUE(
        fixture.master.CreateFile(
            "/source",
            2));

    fixture.PrepareInitialChunk(
        "/source",
        0,
        "original-data");

    ASSERT_TRUE(
        fixture.master.CreateSnapshot(
            "/source",
            "/snapshot"));

    auto source_client =
        fixture.CreateMasterClient();

    auto cache =
        std::make_unique<
            ChunkLocationCache>();

    GFSClient client(
        std::move(source_client),
        std::move(cache),
        [&](const ChunkLocation& location,
            ChunkHandle handle,
            std::uint64_t offset,
            std::size_t length,
            std::string& output) {
            return fixture.Read(
                location,
                handle,
                offset,
                length,
                output);
        },
        [&](const ChunkLocation& location,
            ChunkHandle handle,
            std::uint64_t offset,
            const std::string& data) {
            return fixture.Write(
                location,
                handle,
                offset,
                data);
        });

    client.SetChunkSizeUpdater(
        [&](ChunkHandle handle,
            std::uint64_t size) {
            return fixture.master.SetChunkSize(
                handle,
                size);
        });

    client.SetCopyOnWriteFunction(
        [&](const FilePath& path,
            ChunkIndex index,
            ChunkHandle old_handle) {
            const auto result =
                fixture.master.PrepareCopyOnWrite(
                    path,
                    index,
                    [&](ChunkHandle source,
                        ChunkHandle destination,
                        const std::vector<
                            ServerId>& replicas) {
                        return fixture.CloneReplicas(
                            source,
                            destination,
                            replicas);
                    });

            return result.has_value() &&
                   (*result == old_handle ||
                    *result != 0);
        });

    std::string snapshot_data;

    {
        auto snapshot_client =
            fixture.CreateMasterClient();

        ChunkLocationCache snapshot_cache;

        gfs::client::io::Reader reader(
            *snapshot_client,
            snapshot_cache,
            [&](const ChunkLocation& location,
                ChunkHandle handle,
                std::uint64_t offset,
                std::size_t length,
                std::string& output) {
                return fixture.Read(
                    location,
                    handle,
                    offset,
                    length,
                    output);
            });

        ASSERT_TRUE(
            reader.Read(
                "/snapshot",
                0,
                13,
                snapshot_data));
    }

    EXPECT_EQ(
        snapshot_data,
        "original-data");

    const auto old_handle =
        fixture.master.GetFileChunks(
            "/source")[0];

    ASSERT_TRUE(
        client.Write(
            "/source",
            0,
            "MODIFIED"));

    const auto new_handle =
        fixture.master.GetFileChunks(
            "/source")[0];

    EXPECT_NE(
        old_handle,
        new_handle);

    EXPECT_EQ(
        fixture.master.GetFileChunks(
            "/snapshot")[0],
        old_handle);

    EXPECT_EQ(
        fixture.master.GetChunkReferenceCount(
            old_handle),
        1U);

    EXPECT_EQ(
        fixture.master.GetChunkReferenceCount(
            new_handle),
        1U);

    EXPECT_EQ(
        fixture.master.GetReplicaServers(
            new_handle),
        fixture.master.GetReplicaServers(
            old_handle));

    std::string source_data;

    ASSERT_TRUE(
        client.Read(
            "/source",
            0,
            13,
            source_data));

    EXPECT_EQ(
        source_data,
        "MODIFIED-data");

    std::string snapshot_after;

    {
        auto snapshot_client =
            fixture.CreateMasterClient();

        ChunkLocationCache snapshot_cache;

        gfs::client::io::Reader reader(
            *snapshot_client,
            snapshot_cache,
            [&](const ChunkLocation& location,
                ChunkHandle handle,
                std::uint64_t offset,
                std::size_t length,
                std::string& output) {
                return fixture.Read(
                    location,
                    handle,
                    offset,
                    length,
                    output);
            });

        ASSERT_TRUE(
            reader.Read(
                "/snapshot",
                0,
                13,
                snapshot_after));
    }

    EXPECT_EQ(
        snapshot_after,
        "original-data");

    EXPECT_EQ(
        fixture.primary.GetChunkSize(
            new_handle),
        13U);

    EXPECT_EQ(
        fixture.secondary.GetChunkSize(
            new_handle),
        13U);

    EXPECT_EQ(
        fixture.primary.GetMutationManager()
            .LastAppliedMutationId(
                new_handle),
        1U);

    EXPECT_EQ(
        fixture.secondary.GetMutationManager()
            .LastAppliedMutationId(
                new_handle),
        1U);
}

TEST(
    SnapshotTest,
    DirectorySnapshotDuplicatesNamespaceAndSharesFileChunks) {
    Fixture fixture;

    ASSERT_TRUE(
        fixture.master.CreateDirectory(
            "/dir"));

    ASSERT_TRUE(
        fixture.master.CreateFile(
            "/dir/a",
            2));

    ASSERT_TRUE(
        fixture.master.CreateFile(
            "/dir/b",
            2));

    fixture.PrepareInitialChunk(
        "/dir/a",
        0,
        "a-data");

    const auto b_chunk =
        fixture.master.AllocateChunk(
            "/dir/b");

    ASSERT_TRUE(b_chunk.has_value());

    ASSERT_TRUE(
        fixture.master.RegisterReplica(
            *b_chunk,
            1,
            true));

    ASSERT_TRUE(
        fixture.master.RegisterReplica(
            *b_chunk,
            2,
            false));

    ASSERT_TRUE(
        fixture.primary.CreateChunk(*b_chunk));

    ASSERT_TRUE(
        fixture.secondary.CreateChunk(*b_chunk));

    ASSERT_TRUE(
        fixture.primary.GetMutationManager()
            .ExecutePrimaryMutation(
                *b_chunk,
                1,
                0,
                "b-data",
                [&](const Mutation& mutation) {
                    return fixture.secondary
                        .GetMutationManager()
                        .ApplyMutation(
                            mutation);
                }));

    ASSERT_TRUE(
        fixture.master.SetChunkSize(
            *b_chunk,
            6));

    ASSERT_TRUE(
        fixture.master.UpdateFileSize(
            "/dir/b",
            6));

    ASSERT_TRUE(
        fixture.master.CreateSnapshot(
            "/dir",
            "/snapshot-dir"));

    EXPECT_TRUE(
        fixture.master.DirectoryExists(
            "/snapshot-dir"));

    EXPECT_TRUE(
        fixture.master.FileExists(
            "/snapshot-dir/a"));

    EXPECT_TRUE(
        fixture.master.FileExists(
            "/snapshot-dir/b"));

    const auto a_source =
        fixture.master.GetFileChunks(
            "/dir/a");

    const auto a_snapshot =
        fixture.master.GetFileChunks(
            "/snapshot-dir/a");

    const auto b_source =
        fixture.master.GetFileChunks(
            "/dir/b");

    const auto b_snapshot =
        fixture.master.GetFileChunks(
            "/snapshot-dir/b");

    ASSERT_EQ(a_source.size(), 1U);
    ASSERT_EQ(a_snapshot.size(), 1U);
    ASSERT_EQ(b_source.size(), 1U);
    ASSERT_EQ(b_snapshot.size(), 1U);

    EXPECT_EQ(
        a_source[0],
        a_snapshot[0]);

    EXPECT_EQ(
        b_source[0],
        b_snapshot[0]);

    EXPECT_EQ(
        fixture.primary.ChunkCount(),
        2U);
}

TEST(
    SnapshotTest,
    LeaseIsReleasedWhenSnapshotIsCreated) {
    Fixture fixture;

    ASSERT_TRUE(
        fixture.master.CreateFile(
            "/source",
            2));

    fixture.PrepareInitialChunk(
        "/source",
        0,
        "lease-data");

    const auto handle =
        fixture.master.GetFileChunks(
            "/source")[0];

    ASSERT_TRUE(
        fixture.master.AcquireLease(
            handle,
            1)
            .has_value());

    ASSERT_TRUE(
        fixture.master.GetLease(
            handle)
            .has_value());

    ASSERT_TRUE(
        fixture.master.CreateSnapshot(
            "/source",
            "/snapshot"));

    EXPECT_FALSE(
        fixture.master.GetLease(
            handle)
            .has_value());
}

TEST(
    SnapshotTest,
    SnapshotMetadataSurvivesPhase11Recovery) {
    TempDir persistence;

    {
        Master master(
            2,
            persistence.Path());

        ASSERT_TRUE(
            master.Initialize());

        ASSERT_TRUE(
            master.CreateFile(
                "/source",
                2));

        const auto handle =
            master.AllocateChunk(
                "/source");

        ASSERT_TRUE(
            handle.has_value());

        ASSERT_TRUE(
            master.CreateSnapshot(
                "/source",
                "/snapshot"));

        ASSERT_TRUE(
            master.CreateCheckpoint());
    }

    {
        Master recovered(
            2,
            persistence.Path());

        ASSERT_TRUE(
            recovered.Initialize());

        ASSERT_TRUE(
            recovered.FileExists(
                "/source"));

        ASSERT_TRUE(
            recovered.FileExists(
                "/snapshot"));

        const auto source =
            recovered.GetFile(
                "/source");

        const auto snapshot =
            recovered.GetFile(
                "/snapshot");

        ASSERT_TRUE(source.has_value());
        ASSERT_TRUE(snapshot.has_value());

        ASSERT_EQ(
            source->GetChunkHandles(),
            snapshot->GetChunkHandles());

        ASSERT_EQ(
            source->GetChunkHandles().size(),
            1U);

        EXPECT_EQ(
            recovered.GetChunkReferenceCount(
                source->GetChunkHandles()[0]),
            2U);
    }
}

TEST(
    SnapshotTest,
    MultipleSharedChunksCopyOnWriteIndependently) {
    Fixture fixture;

    ASSERT_TRUE(
        fixture.master.CreateFile(
            "/source",
            2));

    fixture.PrepareInitialChunk(
        "/source",
        0,
        "chunk-zero");

    const auto second =
        fixture.master.AllocateChunk(
            "/source");

    ASSERT_TRUE(second.has_value());

    ASSERT_TRUE(
        fixture.master.RegisterReplica(
            *second,
            1,
            true));

    ASSERT_TRUE(
        fixture.master.RegisterReplica(
            *second,
            2,
            false));

    ASSERT_TRUE(
        fixture.primary.CreateChunk(*second));

    ASSERT_TRUE(
        fixture.secondary.CreateChunk(*second));

    ASSERT_TRUE(
        fixture.primary.GetMutationManager()
            .ExecutePrimaryMutation(
                *second,
                1,
                0,
                "chunk-one",
                [&](const Mutation& mutation) {
                    return fixture.secondary
                        .GetMutationManager()
                        .ApplyMutation(
                            mutation);
                }));

    ASSERT_TRUE(
        fixture.master.SetChunkSize(
            *second,
            9));

    ASSERT_TRUE(
        fixture.master.UpdateFileSize(
            "/source",
            gfs::constants::kChunkSize + 9));

    ASSERT_TRUE(
        fixture.master.CreateSnapshot(
            "/source",
            "/snapshot"));

    const auto old_first =
        fixture.master.GetFileChunks(
            "/source")[0];

    const auto old_second =
        fixture.master.GetFileChunks(
            "/source")[1];

    auto master_client =
        fixture.CreateMasterClient();

    ChunkLocationCache cache;

    gfs::client::io::Writer writer(
        *master_client,
        cache,
        [&](const ChunkLocation& location,
            ChunkHandle handle,
            std::uint64_t offset,
            const std::string& data) {
            return fixture.Write(
                location,
                handle,
                offset,
                data);
        });

    writer.SetChunkSizeUpdater(
        [&](ChunkHandle handle,
            std::uint64_t size) {
            return fixture.master.SetChunkSize(
                handle,
                size);
        });

    writer.SetCopyOnWriteFunction(
        [&](const FilePath& path,
            ChunkIndex index,
            ChunkHandle) {
            return fixture.master
                .PrepareCopyOnWrite(
                    path,
                    index,
                    [&](ChunkHandle source,
                        ChunkHandle destination,
                        const std::vector<
                            ServerId>& replicas) {
                        return fixture.CloneReplicas(
                            source,
                            destination,
                            replicas);
                    })
                .has_value();
        });

    ASSERT_TRUE(
        writer.Write(
            "/source",
            0,
            "changed"));

    const auto new_first =
        fixture.master.GetFileChunks(
            "/source")[0];

    EXPECT_NE(
        old_first,
        new_first);

    EXPECT_EQ(
        fixture.master.GetFileChunks(
            "/source")[1],
        old_second);

    EXPECT_EQ(
        fixture.master.GetFileChunks(
            "/snapshot")[1],
        old_second);
}

}  // namespace