#pragma once

#include "gfs/common/types.hpp"

#include <cstddef>
#include <cstdint>
#include <functional>
#include <optional>
#include <string>
#include <vector>

namespace gfs::client::metadata {

struct DirectoryEntry {
    std::string path;
    bool directory = false;

    friend bool operator==(
        const DirectoryEntry& lhs,
        const DirectoryEntry& rhs) = default;
};

struct ChunkLocation {
    ServerId server_id = 0;
    std::string address;

    friend bool operator==(
        const ChunkLocation& lhs,
        const ChunkLocation& rhs) = default;
};

struct ChunkMetadata {
    ChunkHandle handle = 0;
    ChunkIndex index = 0;
    std::vector<ChunkLocation> locations;
    ChunkVersion version = 1;
};

struct FileMetadata {
    FilePath path;
    std::uint64_t size = 0;
    std::size_t chunk_count = 0;
    std::uint32_t replication_factor = 3;
    std::vector<ChunkHandle> chunk_handles;
};

class MasterClient {
public:
    using FileLookup =
        std::function<std::optional<FileMetadata>(
            const FilePath&)>;

    using ChunkLookup =
        std::function<std::optional<ChunkMetadata>(
            const FilePath&,
            ChunkIndex)>;

    using ChunkAllocation =
        std::function<std::optional<ChunkMetadata>(
            const FilePath&,
            ChunkIndex)>;

    MasterClient() = default;

    explicit MasterClient(
        std::string master_address);

    MasterClient(
        std::string master_address,
        FileLookup file_lookup,
        ChunkLookup chunk_lookup,
        ChunkAllocation chunk_allocator);

    MasterClient(
        const MasterClient&) = delete;

    MasterClient& operator=(
        const MasterClient&) = delete;

    MasterClient(
        MasterClient&&) noexcept = default;

    MasterClient& operator=(
        MasterClient&&) noexcept = default;

    ~MasterClient() = default;

    void SetMasterAddress(
        std::string master_address);

    [[nodiscard]] const std::string&
    GetMasterAddress() const noexcept;

    void SetFileLookup(
        FileLookup lookup);

    void SetChunkLookup(
        ChunkLookup lookup);

    void SetChunkAllocator(
        ChunkAllocation allocator);

    [[nodiscard]] bool
    IsConfigured() const noexcept;

    [[nodiscard]] std::optional<FileMetadata>
    LookupFile(
        const FilePath& path) const;

    [[nodiscard]] std::optional<ChunkHandle>
    LookupChunkHandle(
        const FilePath& path,
        ChunkIndex chunk_index) const;

    [[nodiscard]] std::optional<ChunkMetadata>
    LookupChunk(
        const FilePath& path,
        ChunkIndex chunk_index) const;

    [[nodiscard]] std::vector<ChunkLocation>
    LookupChunkLocations(
        const FilePath& path,
        ChunkIndex chunk_index) const;

    [[nodiscard]] std::optional<ChunkMetadata>
    AllocateChunk(
        const FilePath& path,
        ChunkIndex chunk_index) const;

    [[nodiscard]] bool
    CreateFile(
        const FilePath& path,
        std::uint32_t replication_factor = 0) const;

    [[nodiscard]] bool
    CreateDirectory(
        const FilePath& path) const;

    [[nodiscard]] std::vector<DirectoryEntry>
    ListDirectory(
        const FilePath& path) const;

    [[nodiscard]] bool
    DeleteFile(
        const FilePath& path) const;

    [[nodiscard]] bool
    RenameFile(
        const FilePath& source_path,
        const FilePath& destination_path) const;

    [[nodiscard]] bool
    CreateSnapshot(
        const FilePath& source_path,
        const FilePath& snapshot_path) const;

    [[nodiscard]] bool
    UpdateFileSize(
        const FilePath& path,
        std::uint64_t size) const;

private:
    std::string master_address_;

    FileLookup file_lookup_;
    ChunkLookup chunk_lookup_;
    ChunkAllocation chunk_allocator_;
};

}  // namespace gfs::client::metadata