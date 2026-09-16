#pragma once

#include "gfs/common/types.hpp"
#include "gfs/master/metadata/metadata.hpp"
#include "gfs/master/namespace/namespace_lock.hpp"
#include "gfs/master/namespace/namespace_manager.hpp"

#include <cstddef>
#include <cstdint>
#include <optional>
#include <string>
#include <vector>

namespace gfs::master {

class Master {
public:
    struct FileInfo {
        std::string path;
        std::uint64_t size = 0;
        std::uint32_t replication_factor = 3;
        std::vector<ChunkHandle> chunk_handles;
    };

    struct ChunkInfo {
        ChunkHandle handle = 0;
        ChunkVersion version = 0;
        std::uint64_t size = 0;
        std::vector<ServerId> replicas;
    };

    Master();
    ~Master();

    Master(const Master&) = delete;
    Master& operator=(const Master&) = delete;
    Master(Master&&) = delete;
    Master& operator=(Master&&) = delete;

    bool CreateFile(
        const std::string& path,
        std::uint32_t replication_factor = 3);

    bool DeleteFile(const std::string& path);

    bool RenameFile(
        const std::string& source_path,
        const std::string& destination_path);

    bool CreateDirectory(const std::string& path);
    bool DeleteDirectory(const std::string& path);

    bool FileExists(const std::string& path) const;
    bool DirectoryExists(const std::string& path) const;

    std::optional<FileInfo> GetFileInfo(
        const std::string& path) const;

    std::optional<ChunkInfo> GetChunkInfo(
        ChunkHandle chunk_handle) const;

    std::optional<ChunkHandle> AllocateChunk(
        const std::string& path);

    bool AddReplica(
        ChunkHandle chunk_handle,
        ServerId server_id);

    bool RemoveReplica(
        ChunkHandle chunk_handle,
        ServerId server_id);

    std::vector<ServerId> GetChunkReplicas(
        ChunkHandle chunk_handle) const;

    std::size_t FileCount() const;
    std::size_t ChunkCount() const;
    std::size_t NamespaceNodeCount() const;

    namespace_management::NamespaceManager&
    GetNamespaceManager() noexcept;

    const namespace_management::NamespaceManager&
    GetNamespaceManager() const noexcept;

    metadata::Metadata& GetMetadata() noexcept;
    const metadata::Metadata& GetMetadata() const noexcept;

    namespace_management::NamespaceLock&
    GetNamespaceLock() noexcept;

private:
    metadata::Metadata metadata_;
    namespace_management::NamespaceManager namespace_manager_;
    namespace_management::NamespaceLock namespace_lock_;
};

}  // namespace gfs::master