#include "gfs/chunkserver/chunkserver.hpp"
#include "gfs/client/gfs_client.hpp"
#include "gfs/client/io/record_appender.hpp"
#include "gfs/client/metadata/chunk_location_cache.hpp"
#include "gfs/client/metadata/master_client.hpp"
#include "gfs/client/retry/retry_policy.hpp"
#include "gfs/common/constants.hpp"

#include <gtest/gtest.h>

#include <algorithm>
#include <cstdint>
#include <filesystem>
#include <memory>
#include <optional>
#include <string>
#include <unordered_map>
#include <utility>
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
using gfs::client::io::RecordAppender;
using gfs::client::metadata::ChunkLocation;
using gfs::client::metadata::ChunkLocationCache;
using gfs::client::metadata::ChunkMetadata;
using gfs::client::metadata::FileMetadata;
using gfs::client::metadata::MasterClient;

class TemporaryDirectory {
public:
    TemporaryDirectory() {
        path_ =
            std::filesystem::temp_directory_path() /
            ("gfs_phase12_" +
             std::to_string(
                 reinterpret_cast<std::uintptr_t>(
                     this)));

        std::filesystem::create_directories(
            path_);
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

class FakeAppendCluster {
public:
    struct Chunk {
        ChunkMetadata metadata;
        std::string data;
    };

    bool CreateFile(
        const FilePath& path) {
        if (path.empty() ||
            files.contains(path)) {
            return false;
        }

        files.emplace(
            path,
            FileMetadata{
                path,
                0,
                0});

        return true;
    }

    std::optional<FileMetadata>
    LookupFile(
        const FilePath& path) const {
        const auto it =
            files.find(path);

        if (it == files.end()) {
            return std::nullopt;
        }

        return it->second;
    }

    std::optional<ChunkMetadata>
    LookupChunk(
        const FilePath& path,
        ChunkIndex index) const {
        const auto file_it =
            files.find(path);

        if (file_it == files.end() ||
            index >=
                file_it->second.chunk_count) {
            return std::nullopt;
        }

        const auto chunk_it =
            chunks.find(
                file_it->second.chunk_count == 0
                    ? 0
                    : static_cast<ChunkHandle>(
                          index + 1));

        if (chunk_it == chunks.end()) {
            return std::nullopt;
        }

        return chunk_it->second.metadata;
    }

    std::optional<ChunkMetadata>
    AllocateChunk(
        const FilePath& path,
        ChunkIndex index) {
        auto file_it =
            files.find(path);

        if (file_it == files.end() ||
            index > file_it->second.chunk_count) {
            return std::nullopt;
        }

        if (index <
            file_it->second.chunk_count) {
            return LookupChunk(
                path,
                index);
        }

        const ChunkHandle handle =
            next_handle++;

        ChunkMetadata metadata;
        metadata.handle = handle;
        metadata.index = index;
        metadata.version = 1;
        metadata.locations = {
            ChunkLocation{
                1,
                "primary"},
            ChunkLocation{
                2,
                "secondary"}};

        chunks.emplace(
            handle,
            Chunk{
                metadata,
                {}});

        ++file_it->second.chunk_count;

        return metadata;
    }

    bool UpdateFileSize(
        const FilePath& path,
        std::uint64_t size) {
        auto it =
            files.find(path);

        if (it == files.end()) {
            return false;
        }

        it->second.size =
            std::max(
                it->second.size,
                size);

        return true;
    }

    bool UpdateChunkSize(
        ChunkHandle handle,
        std::uint64_t size) {
        const auto it =
            chunks.find(handle);

        if (it == chunks.end()) {
            return false;
        }

        it->second.metadata.version = 1;

        return size <=
               gfs::constants::kChunkSize;
    }

    bool Read(
        const ChunkLocation& location,
        ChunkHandle handle,
        std::uint64_t offset,
        std::size_t length,
        std::string& output) {
        if (location.server_id != 1 &&
            location.server_id != 2) {
            return false;
        }

        const auto it =
            chunks.find(handle);

        if (it == chunks.end() ||
            offset > it->second.data.size()) {
            return false;
        }

        const std::size_t available =
            it->second.data.size() -
            static_cast<std::size_t>(
                offset);

        const std::size_t actual =
            std::min(
                length,
                available);

        output =
            it->second.data.substr(
                static_cast<std::size_t>(
                    offset),
                actual);

        return true;
    }

    std::unordered_map<
        FilePath,
        FileMetadata>
        files;

    std::unordered_map<
        ChunkHandle,
        Chunk>
        chunks;

    ChunkHandle next_handle = 1;

    std::unique_ptr<MasterClient>
    CreateMasterClient() {
        auto master =
            std::make_unique<
                MasterClient>("master");

        master->SetFileLookup(
            [this](const FilePath& path) {
                return LookupFile(path);
            });

        master->SetChunkLookup(
            [this](
                const FilePath& path,
                ChunkIndex index) {
                return LookupChunk(
                    path,
                    index);
            });

        master->SetChunkAllocator(
            [this](
                const FilePath& path,
                ChunkIndex index) {
                return AllocateChunk(
                    path,
                    index);
            });

        return master;
    }
};

TEST(
    RecordAppendTest,
    RejectsRecordLargerThanOneFourthChunk) {
    FakeAppendCluster cluster;

    ASSERT_TRUE(
        cluster.CreateFile("/log"));

    auto master =
        std::make_unique<MasterClient>(
            "master");

    master->SetFileLookup(
        [&](const FilePath& path) {
            return cluster.LookupFile(path);
        });

    master->SetChunkLookup(
        [&](const FilePath& path,
            ChunkIndex index) {
            return cluster.LookupChunk(
                path,
                index);
        });

    master->SetChunkAllocator(
        [&](const FilePath& path,
            ChunkIndex index) {
            return cluster.AllocateChunk(
                path,
                index);
        });

    ChunkLocationCache cache;

    RecordAppender appender(
        *master,
        cache);

    appender.SetAppendFunction(
        [](
            const FilePath&,
            const ChunkLocation&,
            ChunkHandle,
            ChunkVersion,
            const std::vector<ChunkLocation>&,
            const std::string&) {
            return RecordAppender::AppendResult{};
        });

    std::uint64_t offset = 0;

    const std::string oversized(
        gfs::constants::kChunkSize / 4U + 1U,
        'x');

    EXPECT_FALSE(
        appender.Append(
            "/log",
            oversized,
            offset));
}

TEST(
    RecordAppendTest,
    PrimarySelectsOffsetAndPropagatesMutation) {
    TemporaryDirectory primary_dir;
    TemporaryDirectory secondary_dir;

    Chunkserver primary(
        1,
        primary_dir.Path());

    Chunkserver secondary(
        2,
        secondary_dir.Path());

    ASSERT_TRUE(
        primary.Initialize());

    ASSERT_TRUE(
        secondary.Initialize());

    ASSERT_TRUE(
        primary.CreateChunk(1));

    ASSERT_TRUE(
        secondary.CreateChunk(1));

    FakeAppendCluster cluster;

    ASSERT_TRUE(
        cluster.CreateFile("/log"));

    ASSERT_TRUE(
        cluster.AllocateChunk(
            "/log",
            0)
            .has_value());

    auto master =
        std::make_unique<MasterClient>(
            "master");

    master->SetFileLookup(
        [&](const FilePath& path) {
            return cluster.LookupFile(path);
        });

    master->SetChunkLookup(
        [&](const FilePath& path,
            ChunkIndex index) {
            return cluster.LookupChunk(
                path,
                index);
        });

    master->SetChunkAllocator(
        [&](const FilePath& path,
            ChunkIndex index) {
            return cluster.AllocateChunk(
                path,
                index);
        });

    ChunkLocationCache cache;

    RecordAppender appender(
        *master,
        cache);

    appender.SetLeaseValidator(
        [](ChunkHandle,
           ServerId server_id) {
            return server_id == 1;
        });

    appender.SetAppendFunction(
        [&](
            const FilePath&,
            const ChunkLocation& primary_location,
            ChunkHandle handle,
            ChunkVersion version,
            const std::vector<
                ChunkLocation>& replicas,
            const std::string& data) {
            if (primary_location.server_id != 1) {
                return RecordAppender::AppendResult{};
            }

            const auto result =
                primary.GetMutationManager()
                    .ExecutePrimaryRecordAppend(
                        handle,
                        version,
                        data,
                        [&](const Mutation& mutation) {
                            for (const auto& replica :
                                 replicas) {
                                if (replica.server_id == 1) {
                                    continue;
                                }

                                if (replica.server_id == 2 &&
                                    !secondary
                                         .GetMutationManager()
                                         .ApplyMutation(
                                             mutation)) {
                                    return false;
                                }
                            }

                            return true;
                        });

            RecordAppender::AppendResult converted;

            converted.offset =
                result.offset;

            converted.chunk_size =
                result.chunk_size;

            converted.bytes_appended =
                result.bytes_appended;

            if (result.status ==
                gfs::chunkserver::mutation::
                    MutationManager::
                        RecordAppendStatus::Success) {
                converted.status =
                    RecordAppender::
                        AppendStatus::Success;
            } else if (
                result.status ==
                gfs::chunkserver::mutation::
                    MutationManager::
                        RecordAppendStatus::
                            RetryNextChunk) {
                converted.status =
                    RecordAppender::
                        AppendStatus::
                            RetryNextChunk;
            }

            return converted;
        });

    std::uint64_t first_offset = 0;

    ASSERT_TRUE(
        appender.Append(
            "/log",
            "first",
            first_offset));

    EXPECT_EQ(
        first_offset,
        0U);

    std::uint64_t second_offset = 0;

    ASSERT_TRUE(
        appender.Append(
            "/log",
            "second",
            second_offset));

    EXPECT_EQ(
        second_offset,
        5U);

    std::string primary_data;
    std::string secondary_data;

    ASSERT_TRUE(
        primary.ReadChunk(
            1,
            0,
            11,
            primary_data));

    ASSERT_TRUE(
        secondary.ReadChunk(
            1,
            0,
            11,
            secondary_data));

    EXPECT_EQ(
        primary_data,
        "firstsecond");

    EXPECT_EQ(
        secondary_data,
        "firstsecond");

    EXPECT_EQ(
        primary.GetMutationManager()
            .LastAppliedMutationId(1),
        2U);

    EXPECT_EQ(
        secondary.GetMutationManager()
            .LastAppliedMutationId(1),
        2U);
}

TEST(
    RecordAppendTest,
    AppendedDataIsReadableThroughGFSClient) {
    TemporaryDirectory primary_dir;
    TemporaryDirectory secondary_dir;

    Chunkserver primary(
        1,
        primary_dir.Path());

    Chunkserver secondary(
        2,
        secondary_dir.Path());

    ASSERT_TRUE(
        primary.Initialize());

    ASSERT_TRUE(
        secondary.Initialize());

    FakeAppendCluster cluster;

    ASSERT_TRUE(
        cluster.CreateFile("/log"));

    auto master =
        cluster.CreateMasterClient();

    auto cache =
        std::make_unique<
            ChunkLocationCache>();

    GFSClient client(
        std::move(master),
        std::move(cache),
        [&](const ChunkLocation& location,
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
        },
        [](
            const ChunkLocation&,
            ChunkHandle,
            std::uint64_t,
            const std::string&) {
            return true;
        });

    client.SetRecordAppendLeaseValidator(
        [](ChunkHandle,
           ServerId server_id) {
            return server_id == 1;
        });

    client.SetRecordAppendFunction(
        [&](
            const FilePath&,
            const ChunkLocation&,
            ChunkHandle handle,
            ChunkVersion version,
            const std::vector<
                ChunkLocation>& replicas,
            const std::string& data) {
            const auto result =
                primary.GetMutationManager()
                    .ExecutePrimaryRecordAppend(
                        handle,
                        version,
                        data,
                        [&](const Mutation& mutation) {
                            for (const auto& replica :
                                 replicas) {
                                if (replica.server_id == 1) {
                                    continue;
                                }

                                if (replica.server_id == 2 &&
                                    !secondary
                                         .GetMutationManager()
                                         .ApplyMutation(
                                             mutation)) {
                                    return false;
                                }
                            }

                            return true;
                        });

            RecordAppender::AppendResult converted;

            converted.offset =
                result.offset;

            converted.chunk_size =
                result.chunk_size;

            converted.bytes_appended =
                result.bytes_appended;

            using Status =
                gfs::chunkserver::mutation::
                    MutationManager::
                        RecordAppendStatus;

            converted.status =
                result.status == Status::Success
                    ? RecordAppender::
                          AppendStatus::Success
                    : result.status ==
                              Status::
                                  RetryNextChunk
                        ? RecordAppender::
                              AppendStatus::
                                  RetryNextChunk
                        : RecordAppender::
                              AppendStatus::Failed;

            return converted;
        });

    client.SetRecordAppendFileSizeUpdater(
        [&](const FilePath& path,
            std::uint64_t size) {
            return cluster.UpdateFileSize(
                path,
                size);
        });

    std::uint64_t offset = 0;

    ASSERT_TRUE(
        client.Append(
            "/log",
            "record-one",
            offset));

    EXPECT_EQ(
        offset,
        0U);

    std::string output;

    ASSERT_TRUE(
        client.Read(
            "/log",
            0,
            10,
            output));

    EXPECT_EQ(
        output,
        "record-one");

    EXPECT_EQ(
        client.LookupFile("/log")->size,
        10U);
}

TEST(
    RecordAppendTest,
    PadsFullBoundaryAndRetriesOnNextChunk) {
    TemporaryDirectory primary_dir;
    TemporaryDirectory secondary_dir;

    Chunkserver primary(
        1,
        primary_dir.Path());

    Chunkserver secondary(
        2,
        secondary_dir.Path());

    ASSERT_TRUE(
        primary.Initialize());

    ASSERT_TRUE(
        secondary.Initialize());

    ASSERT_TRUE(
        primary.CreateChunk(1));

    ASSERT_TRUE(
        secondary.CreateChunk(1));

    const std::size_t near_full =
        gfs::constants::kChunkSize - 3U;

    const std::string prefix(
        near_full,
        'p');

    ASSERT_TRUE(
        primary.WriteChunk(
            1,
            0,
            prefix));

    ASSERT_TRUE(
        secondary.WriteChunk(
            1,
            0,
            prefix));

    FakeAppendCluster cluster;

    ASSERT_TRUE(
        cluster.CreateFile("/log"));

    auto first =
        cluster.AllocateChunk(
            "/log",
            0);

    ASSERT_TRUE(
        first.has_value());

    auto master =
        std::make_unique<MasterClient>(
            "master");

    master->SetFileLookup(
        [&](const FilePath& path) {
            return cluster.LookupFile(path);
        });

    master->SetChunkLookup(
        [&](const FilePath& path,
            ChunkIndex index) {
            return cluster.LookupChunk(
                path,
                index);
        });

    master->SetChunkAllocator(
        [&](const FilePath& path,
            ChunkIndex index) {
            return cluster.AllocateChunk(
                path,
                index);
        });

    ChunkLocationCache cache;

    RecordAppender appender(
        *master,
        cache);

    appender.SetLeaseValidator(
        [](ChunkHandle,
           ServerId server_id) {
            return server_id == 1;
        });

    appender.SetAppendFunction(
        [&](
            const FilePath&,
            const ChunkLocation& primary_location,
            ChunkHandle handle,
            ChunkVersion version,
            const std::vector<
                ChunkLocation>& replicas,
            const std::string& data) {
            if (primary_location.server_id != 1) {
                return RecordAppender::AppendResult{};
            }

            const auto result =
                primary.GetMutationManager()
                    .ExecutePrimaryRecordAppend(
                        handle,
                        version,
                        data,
                        [&](const Mutation& mutation) {
                            for (const auto& replica :
                                 replicas) {
                                if (replica.server_id == 1) {
                                    continue;
                                }

                                if (replica.server_id == 2 &&
                                    !secondary
                                         .GetMutationManager()
                                         .ApplyMutation(
                                             mutation)) {
                                    return false;
                                }
                            }

                            return true;
                        });

            RecordAppender::AppendResult converted;

            converted.offset =
                result.offset;

            converted.chunk_size =
                result.chunk_size;

            converted.bytes_appended =
                result.bytes_appended;

            using Status =
                gfs::chunkserver::mutation::
                    MutationManager::
                        RecordAppendStatus;

            converted.status =
                result.status == Status::Success
                    ? RecordAppender::
                          AppendStatus::Success
                    : result.status ==
                              Status::
                                  RetryNextChunk
                        ? RecordAppender::
                              AppendStatus::
                                  RetryNextChunk
                        : RecordAppender::
                              AppendStatus::Failed;

            return converted;
        });

    std::uint64_t offset = 0;

    ASSERT_TRUE(
        appender.Append(
            "/log",
            "record",
            offset));

    EXPECT_EQ(
        offset,
        static_cast<std::uint64_t>(
            gfs::constants::kChunkSize));

    ASSERT_EQ(
        cluster.files.at("/log").chunk_count,
        2U);

    EXPECT_EQ(
        primary.GetChunkSize(1),
        static_cast<std::uint64_t>(
            gfs::constants::kChunkSize));

    EXPECT_EQ(
        secondary.GetChunkSize(1),
        static_cast<std::uint64_t>(
            gfs::constants::kChunkSize));
}

TEST(
    RecordAppendTest,
    FailedPropagationCausesAtLeastOnceRetry) {
    TemporaryDirectory primary_dir;
    TemporaryDirectory secondary_dir;

    Chunkserver primary(
        1,
        primary_dir.Path());

    Chunkserver secondary(
        2,
        secondary_dir.Path());

    ASSERT_TRUE(
        primary.Initialize());

    ASSERT_TRUE(
        secondary.Initialize());

    ASSERT_TRUE(
        primary.CreateChunk(1));

    ASSERT_TRUE(
        secondary.CreateChunk(1));

    FakeAppendCluster cluster;

    ASSERT_TRUE(
        cluster.CreateFile("/log"));

    ASSERT_TRUE(
        cluster.AllocateChunk(
            "/log",
            0)
            .has_value());

    auto master =
        std::make_unique<MasterClient>(
            "master");

    master->SetFileLookup(
        [&](const FilePath& path) {
            return cluster.LookupFile(path);
        });

    master->SetChunkLookup(
        [&](const FilePath& path,
            ChunkIndex index) {
            return cluster.LookupChunk(
                path,
                index);
        });

    master->SetChunkAllocator(
        [&](const FilePath& path,
            ChunkIndex index) {
            return cluster.AllocateChunk(
                path,
                index);
        });

    ChunkLocationCache cache;

    RecordAppender appender(
        *master,
        cache,
        gfs::client::retry::RetryPolicy(4));

    bool fail_first_propagation = true;

    appender.SetLeaseValidator(
        [](ChunkHandle,
           ServerId) {
            return true;
        });

    appender.SetAppendFunction(
        [&](
            const FilePath&,
            const ChunkLocation&,
            ChunkHandle handle,
            ChunkVersion version,
            const std::vector<
                ChunkLocation>& replicas,
            const std::string& data) {
            const auto result =
                primary.GetMutationManager()
                    .ExecutePrimaryRecordAppend(
                        handle,
                        version,
                        data,
                        [&](const Mutation& mutation) {
                            if (fail_first_propagation) {
                                fail_first_propagation =
                                    false;
                                return false;
                            }

                            for (const auto& replica :
                                 replicas) {
                                if (replica.server_id == 2 &&
                                    !secondary
                                         .GetMutationManager()
                                         .ApplyMutation(
                                             mutation)) {
                                    return false;
                                }
                            }

                            return true;
                        });

            RecordAppender::AppendResult converted;

            converted.offset =
                result.offset;

            converted.chunk_size =
                result.chunk_size;

            converted.bytes_appended =
                result.bytes_appended;

            using Status =
                gfs::chunkserver::mutation::
                    MutationManager::
                        RecordAppendStatus;

            converted.status =
                result.status == Status::Success
                    ? RecordAppender::
                          AppendStatus::Success
                    : result.status ==
                              Status::
                                  RetryNextChunk
                        ? RecordAppender::
                              AppendStatus::
                                  RetryNextChunk
                        : RecordAppender::
                              AppendStatus::Failed;

            return converted;
        });

    std::uint64_t offset = 0;

    ASSERT_TRUE(
        appender.Append(
            "/log",
            "duplicate-safe",
            offset));

    EXPECT_EQ(
        offset,
        static_cast<std::uint64_t>(
            std::string(
                "duplicate-safe").size()));

    EXPECT_EQ(
        primary.GetMutationManager()
            .LastAppliedMutationId(1),
        2U);

    EXPECT_EQ(
        secondary.GetMutationManager()
            .LastAppliedMutationId(1),
        2U);
}

}  // namespace