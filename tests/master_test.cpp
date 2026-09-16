#include "gfs/common/constants.hpp"
#include "gfs/master/master.hpp"
#include "gfs/master/metadata/chunk_metadata.hpp"
#include "gfs/master/metadata/file_metadata.hpp"
#include "gfs/master/metadata/metadata.hpp"
#include "gfs/master/namespace/namespace_lock.hpp"
#include "gfs/master/namespace/namespace_manager.hpp"

#include <gtest/gtest.h>

#include <cstdint>
#include <string>
#include <vector>

namespace {

using gfs::ChunkHandle;
using gfs::ServerId;
using gfs::master::Master;
using gfs::master::metadata::ChunkMetadata;
using gfs::master::metadata::FileMetadata;
using gfs::master::metadata::Metadata;
using gfs::master::namespace_management::NamespaceLock;
using gfs::master::namespace_management::NamespaceManager;

TEST(FileMetadataTest, StoresBasicMetadata) {
    FileMetadata file("/data/file", 3);

    EXPECT_EQ(file.GetPath(), "/data/file");
    EXPECT_EQ(file.GetSize(), 0U);
    EXPECT_EQ(file.GetReplicationFactor(), 3U);
    EXPECT_EQ(file.ChunkCount(), 0U);
}

TEST(FileMetadataTest, ManagesChunkHandles) {
    FileMetadata file("/data/file", 3);

    EXPECT_TRUE(file.AddChunk(10));
    EXPECT_FALSE(file.AddChunk(10));
    EXPECT_TRUE(file.HasChunk(10));

    EXPECT_TRUE(file.AddChunk(20));
    EXPECT_EQ(file.ChunkCount(), 2U);

    EXPECT_TRUE(file.RemoveChunk(10));
    EXPECT_FALSE(file.HasChunk(10));
    EXPECT_EQ(file.ChunkCount(), 1U);

    EXPECT_FALSE(file.RemoveChunk(10));
}

TEST(FileMetadataTest, UpdatesSizeAndPath) {
    FileMetadata file("/old/path", 2);

    file.SetPath("/new/path");
    file.SetSize(12345);

    EXPECT_EQ(file.GetPath(), "/new/path");
    EXPECT_EQ(file.GetSize(), 12345U);
    EXPECT_EQ(file.GetReplicationFactor(), 2U);
}

TEST(ChunkMetadataTest, StoresHandleVersionAndSize) {
    ChunkMetadata chunk(42, 1);

    EXPECT_EQ(chunk.GetHandle(), 42U);
    EXPECT_EQ(chunk.GetVersion(), 1U);
    EXPECT_EQ(chunk.GetSize(), 0U);

    chunk.SetVersion(7);
    chunk.SetSize(4096);

    EXPECT_EQ(chunk.GetVersion(), 7U);
    EXPECT_EQ(chunk.GetSize(), 4096U);
}

TEST(ChunkMetadataTest, TracksReplicas) {
    ChunkMetadata chunk(42);

    EXPECT_TRUE(chunk.AddReplica(1));
    EXPECT_FALSE(chunk.AddReplica(1));
    EXPECT_TRUE(chunk.AddReplica(2));

    EXPECT_TRUE(chunk.HasReplica(1));
    EXPECT_TRUE(chunk.HasReplica(2));
    EXPECT_EQ(chunk.ReplicaCount(), 2U);

    const auto replicas = chunk.GetReplicaServerIds();

    ASSERT_EQ(replicas.size(), 2U);
    EXPECT_EQ(replicas[0], 1U);
    EXPECT_EQ(replicas[1], 2U);

    EXPECT_TRUE(chunk.RemoveReplica(1));
    EXPECT_FALSE(chunk.HasReplica(1));
    EXPECT_EQ(chunk.ReplicaCount(), 1U);
}

TEST(MetadataTest, CreatesAndLooksUpFiles) {
    Metadata metadata;

    EXPECT_TRUE(metadata.CreateFile("/data/file", 3));
    EXPECT_FALSE(metadata.CreateFile("/data/file", 3));

    EXPECT_TRUE(metadata.FileExists("/data/file"));
    EXPECT_EQ(metadata.FileCount(), 1U);

    const auto file = metadata.GetFile("/data/file");

    ASSERT_TRUE(file.has_value());
    EXPECT_EQ(file->GetPath(), "/data/file");
    EXPECT_EQ(file->GetReplicationFactor(), 3U);
    EXPECT_EQ(file->GetSize(), 0U);
}

TEST(MetadataTest, RejectsInvalidReplicationFactor) {
    Metadata metadata;

    EXPECT_FALSE(metadata.CreateFile("/data/file", 0));
    EXPECT_FALSE(metadata.FileExists("/data/file"));
}

TEST(MetadataTest, AllocatesChunksForFiles) {
    Metadata metadata;

    ASSERT_TRUE(metadata.CreateFile("/data/file", 3));

    const auto first = metadata.AllocateChunk("/data/file");
    const auto second = metadata.AllocateChunk("/data/file");

    ASSERT_TRUE(first.has_value());
    ASSERT_TRUE(second.has_value());

    EXPECT_NE(*first, *second);
    EXPECT_TRUE(metadata.ChunkExists(*first));
    EXPECT_TRUE(metadata.ChunkExists(*second));

    const auto file = metadata.GetFile("/data/file");

    ASSERT_TRUE(file.has_value());
    ASSERT_EQ(file->ChunkCount(), 2U);
    EXPECT_EQ(file->GetChunkHandles()[0], *first);
    EXPECT_EQ(file->GetChunkHandles()[1], *second);

    const auto chunk = metadata.GetChunk(*first);

    ASSERT_TRUE(chunk.has_value());
    EXPECT_EQ(chunk->GetHandle(), *first);
    EXPECT_EQ(chunk->GetVersion(), 1U);
}

TEST(MetadataTest, TracksChunkReplicas) {
    Metadata metadata;

    ASSERT_TRUE(metadata.CreateFile("/data/file", 3));

    const auto handle = metadata.AllocateChunk("/data/file");
    ASSERT_TRUE(handle.has_value());

    EXPECT_TRUE(metadata.AddReplica(*handle, 10));
    EXPECT_TRUE(metadata.AddReplica(*handle, 20));
    EXPECT_FALSE(metadata.AddReplica(*handle, 10));

    const auto replicas = metadata.GetReplicas(*handle);

    ASSERT_EQ(replicas.size(), 2U);
    EXPECT_EQ(replicas[0], 10U);
    EXPECT_EQ(replicas[1], 20U);

    EXPECT_TRUE(metadata.RemoveReplica(*handle, 10));
    EXPECT_EQ(metadata.GetReplicas(*handle).size(), 1U);
}

TEST(MetadataTest, RenamesFiles) {
    Metadata metadata;

    ASSERT_TRUE(metadata.CreateFile("/old", 3));
    ASSERT_TRUE(metadata.AllocateChunk("/old").has_value());

    EXPECT_TRUE(metadata.RenameFile("/old", "/new"));

    EXPECT_FALSE(metadata.FileExists("/old"));
    EXPECT_TRUE(metadata.FileExists("/new"));

    const auto file = metadata.GetFile("/new");

    ASSERT_TRUE(file.has_value());
    EXPECT_EQ(file->GetPath(), "/new");
    EXPECT_EQ(file->ChunkCount(), 1U);
}

TEST(MetadataTest, DeletesFileAndItsChunks) {
    Metadata metadata;

    ASSERT_TRUE(metadata.CreateFile("/data/file", 3));

    const auto chunk = metadata.AllocateChunk("/data/file");
    ASSERT_TRUE(chunk.has_value());

    EXPECT_EQ(metadata.FileCount(), 1U);
    EXPECT_EQ(metadata.ChunkCount(), 1U);

    EXPECT_TRUE(metadata.DeleteFile("/data/file"));

    EXPECT_FALSE(metadata.FileExists("/data/file"));
    EXPECT_EQ(metadata.FileCount(), 0U);
    EXPECT_EQ(metadata.ChunkCount(), 0U);
}

TEST(MetadataTest, UpdatesChunkMetadata) {
    Metadata metadata;

    ASSERT_TRUE(metadata.CreateFile("/data/file", 3));

    const auto handle = metadata.AllocateChunk("/data/file");
    ASSERT_TRUE(handle.has_value());

    EXPECT_TRUE(metadata.SetChunkVersion(*handle, 5));
    EXPECT_TRUE(metadata.SetChunkSize(*handle, 65536));

    EXPECT_EQ(metadata.GetChunkVersion(*handle), 5U);

    const auto chunk = metadata.GetChunk(*handle);
    ASSERT_TRUE(chunk.has_value());

    EXPECT_EQ(chunk->GetVersion(), 5U);
    EXPECT_EQ(chunk->GetSize(), 65536U);
}

TEST(NamespaceManagerTest, RootDirectoryExists) {
    NamespaceManager manager;

    EXPECT_TRUE(manager.Exists("/"));
    EXPECT_TRUE(manager.IsDirectory("/"));
    EXPECT_FALSE(manager.IsFile("/"));
}

TEST(NamespaceManagerTest, ValidatesPaths) {
    EXPECT_TRUE(NamespaceManager::IsValidPath("/"));
    EXPECT_TRUE(NamespaceManager::IsValidPath("/data"));
    EXPECT_TRUE(NamespaceManager::IsValidPath("/data/file"));

    EXPECT_FALSE(NamespaceManager::IsValidPath(""));
    EXPECT_FALSE(NamespaceManager::IsValidPath("data"));
    EXPECT_FALSE(NamespaceManager::IsValidPath("/data/"));
    EXPECT_FALSE(NamespaceManager::IsValidPath("/data//file"));
    EXPECT_FALSE(NamespaceManager::IsValidPath("/data/../file"));
    EXPECT_FALSE(NamespaceManager::IsValidPath("/data/./file"));
}

TEST(NamespaceManagerTest, CreatesHierarchicalDirectories) {
    NamespaceManager manager;

    EXPECT_TRUE(manager.CreateDirectory("/data"));
    EXPECT_TRUE(manager.CreateDirectory("/data/files"));

    EXPECT_TRUE(manager.Exists("/data"));
    EXPECT_TRUE(manager.Exists("/data/files"));
    EXPECT_TRUE(manager.IsDirectory("/data"));
    EXPECT_TRUE(manager.IsDirectory("/data/files"));

    EXPECT_FALSE(manager.CreateDirectory("/data"));
    EXPECT_FALSE(manager.CreateDirectory("/missing/child"));
}

TEST(NamespaceManagerTest, CreatesFilesInsideDirectories) {
    NamespaceManager manager;

    ASSERT_TRUE(manager.CreateDirectory("/data"));
    EXPECT_TRUE(manager.CreateFile("/data/file"));

    EXPECT_TRUE(manager.IsFile("/data/file"));
    EXPECT_FALSE(manager.IsDirectory("/data/file"));
    EXPECT_FALSE(manager.CreateFile("/data/file"));
}

TEST(NamespaceManagerTest, ListsDirectoryChildren) {
    NamespaceManager manager;

    ASSERT_TRUE(manager.CreateDirectory("/data"));
    ASSERT_TRUE(manager.CreateFile("/data/a"));
    ASSERT_TRUE(manager.CreateDirectory("/data/b"));

    const auto children = manager.ListDirectory("/data");

    ASSERT_EQ(children.size(), 2U);
    EXPECT_TRUE(
        children[0].path == "/data/a" ||
        children[1].path == "/data/a");
    EXPECT_TRUE(
        children[0].path == "/data/b" ||
        children[1].path == "/data/b");
}

TEST(NamespaceManagerTest, DeletesFiles) {
    NamespaceManager manager;

    ASSERT_TRUE(manager.CreateDirectory("/data"));
    ASSERT_TRUE(manager.CreateFile("/data/file"));

    EXPECT_TRUE(manager.DeleteFile("/data/file"));
    EXPECT_FALSE(manager.Exists("/data/file"));
    EXPECT_FALSE(manager.DeleteFile("/data/file"));
}

TEST(NamespaceManagerTest, DeletesOnlyEmptyDirectories) {
    NamespaceManager manager;

    ASSERT_TRUE(manager.CreateDirectory("/data"));
    ASSERT_TRUE(manager.CreateFile("/data/file"));

    EXPECT_FALSE(manager.DeleteDirectory("/data"));

    EXPECT_TRUE(manager.DeleteFile("/data/file"));
    EXPECT_TRUE(manager.DeleteDirectory("/data"));
    EXPECT_FALSE(manager.Exists("/data"));
}

TEST(NamespaceManagerTest, RenamesFiles) {
    NamespaceManager manager;

    ASSERT_TRUE(manager.CreateDirectory("/data"));
    ASSERT_TRUE(manager.CreateFile("/data/old"));

    EXPECT_TRUE(manager.Rename("/data/old", "/data/new"));

    EXPECT_FALSE(manager.Exists("/data/old"));
    EXPECT_TRUE(manager.IsFile("/data/new"));
}

TEST(NamespaceManagerTest, PreventsDirectoryCycles) {
    NamespaceManager manager;

    ASSERT_TRUE(manager.CreateDirectory("/a"));
    ASSERT_TRUE(manager.CreateDirectory("/a/b"));

    EXPECT_FALSE(manager.Rename("/a", "/a/b/c"));
}

TEST(NamespaceManagerTest, ParentAndBaseNameWork) {
    NamespaceManager manager;

    EXPECT_EQ(manager.ParentPath("/"), "");
    EXPECT_EQ(manager.ParentPath("/data"), "/");
    EXPECT_EQ(manager.ParentPath("/data/file"), "/data");

    EXPECT_EQ(manager.BaseName("/"), "");
    EXPECT_EQ(manager.BaseName("/data"), "data");
    EXPECT_EQ(manager.BaseName("/data/file"), "file");
}

TEST(NamespaceLockTest, AcquiresReadLock) {
    NamespaceLock lock;

    auto guard = lock.AcquireRead("/data/file");

    EXPECT_TRUE(guard.OwnsLock());
}

TEST(NamespaceLockTest, AcquiresWriteLock) {
    NamespaceLock lock;

    auto guard = lock.AcquireWrite("/data/file");

    EXPECT_TRUE(guard.OwnsLock());
}

TEST(NamespaceLockTest, AcquiresMultiplePathLocks) {
    NamespaceLock lock;

    auto guard = lock.AcquireWritePath(
        {"/data/a", "/data/b"});

    EXPECT_TRUE(guard.OwnsLock());
}

TEST(NamespaceLockTest, GuardsAreMovable) {
    NamespaceLock lock;

    auto first = lock.AcquireRead("/data/file");
    ASSERT_TRUE(first.OwnsLock());

    auto second = std::move(first);

    EXPECT_FALSE(first.OwnsLock());
    EXPECT_TRUE(second.OwnsLock());
}

TEST(MasterTest, CreatesFileWithMetadataAndNamespace) {
    Master master;

    EXPECT_TRUE(master.CreateFile("/data", 3));
    EXPECT_TRUE(master.FileExists("/data"));
    EXPECT_FALSE(master.DirectoryExists("/data"));

    const auto info = master.GetFileInfo("/data");

    ASSERT_TRUE(info.has_value());
    EXPECT_EQ(info->path, "/data");
    EXPECT_EQ(info->size, 0U);
    EXPECT_EQ(info->replication_factor, 3U);
    EXPECT_TRUE(info->chunk_handles.empty());
}

TEST(MasterTest, CreatesDirectories) {
    Master master;

    EXPECT_TRUE(master.CreateDirectory("/data"));
    EXPECT_TRUE(master.CreateDirectory("/data/files"));

    EXPECT_TRUE(master.DirectoryExists("/data"));
    EXPECT_TRUE(master.DirectoryExists("/data/files"));
}

TEST(MasterTest, AllocatesChunkThroughMaster) {
    Master master;

    ASSERT_TRUE(master.CreateFile("/data", 3));

    const auto handle = master.AllocateChunk("/data");

    ASSERT_TRUE(handle.has_value());

    const auto file = master.GetFileInfo("/data");
    ASSERT_TRUE(file.has_value());

    ASSERT_EQ(file->chunk_handles.size(), 1U);
    EXPECT_EQ(file->chunk_handles.front(), *handle);

    const auto chunk = master.GetChunkInfo(*handle);

    ASSERT_TRUE(chunk.has_value());
    EXPECT_EQ(chunk->handle, *handle);
    EXPECT_EQ(chunk->version, 1U);
}

TEST(MasterTest, TracksReplicas) {
    Master master;

    ASSERT_TRUE(master.CreateFile("/data", 3));

    const auto handle = master.AllocateChunk("/data");
    ASSERT_TRUE(handle.has_value());

    EXPECT_TRUE(master.AddReplica(*handle, 1));
    EXPECT_TRUE(master.AddReplica(*handle, 2));
    EXPECT_TRUE(master.AddReplica(*handle, 3));

    const auto replicas =
        master.GetChunkReplicas(*handle);

    EXPECT_EQ(replicas.size(), 3U);

    const auto info = master.GetChunkInfo(*handle);

    ASSERT_TRUE(info.has_value());
    EXPECT_EQ(info->replicas.size(), 3U);
}

TEST(MasterTest, RenamesFileAndPreservesChunkMetadata) {
    Master master;

    ASSERT_TRUE(master.CreateFile("/old", 3));

    const auto handle = master.AllocateChunk("/old");
    ASSERT_TRUE(handle.has_value());

    EXPECT_TRUE(master.RenameFile("/old", "/new"));

    EXPECT_FALSE(master.FileExists("/old"));
    EXPECT_TRUE(master.FileExists("/new"));

    const auto info = master.GetFileInfo("/new");

    ASSERT_TRUE(info.has_value());
    ASSERT_EQ(info->chunk_handles.size(), 1U);
    EXPECT_EQ(info->chunk_handles.front(), *handle);
}

TEST(MasterTest, DeletesFileAndMetadata) {
    Master master;

    ASSERT_TRUE(master.CreateFile("/data", 3));

    const auto handle = master.AllocateChunk("/data");
    ASSERT_TRUE(handle.has_value());

    EXPECT_TRUE(master.DeleteFile("/data"));

    EXPECT_FALSE(master.FileExists("/data"));
    EXPECT_FALSE(master.GetFileInfo("/data").has_value());
    EXPECT_FALSE(master.GetChunkInfo(*handle).has_value());
}

TEST(MasterTest, RejectsDuplicateFiles) {
    Master master;

    EXPECT_TRUE(master.CreateFile("/data", 3));
    EXPECT_FALSE(master.CreateFile("/data", 3));

    EXPECT_EQ(master.FileCount(), 1U);
}

TEST(MasterTest, DoesNotAllocateChunkForMissingFile) {
    Master master;

    const auto handle =
        master.AllocateChunk("/missing");

    EXPECT_FALSE(handle.has_value());
    EXPECT_EQ(master.ChunkCount(), 0U);
}

TEST(MasterTest, NamespaceAndMetadataRemainIntegrated) {
    Master master;

    ASSERT_TRUE(master.CreateDirectory("/data"));
    ASSERT_TRUE(master.CreateFile("/data/file", 3));

    const auto handle =
        master.AllocateChunk("/data/file");

    ASSERT_TRUE(handle.has_value());

    EXPECT_EQ(master.FileCount(), 1U);
    EXPECT_EQ(master.ChunkCount(), 1U);
    EXPECT_EQ(master.NamespaceNodeCount(), 3U);

    EXPECT_TRUE(master.DirectoryExists("/data"));
    EXPECT_TRUE(master.FileExists("/data/file"));
}

}  // namespace