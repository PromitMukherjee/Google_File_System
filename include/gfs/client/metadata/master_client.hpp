#pragma once

#include "gfs/common/types.hpp"

#include <cstddef>
#include <functional>
#include <optional>
#include <string>
#include <vector>

namespace gfs::client::metadata {

struct ChunkLocation {
    ServerId server_id = 0;
    std::string address;

    friend bool operator==(const ChunkLocation& lhs,
                           const ChunkLocation& rhs) = default;
};

struct ChunkMetadata {
    ChunkHandle handle = 0;
    ChunkIndex index = 0;
    std::vector<ChunkLocation> locations;
};

struct FileMetadata {
    FilePath path;
    std::uint64_t size = 0;
    std::size_t chunk_count = 0;
};

class MasterClient {
public:
    using FileLookup = std::function<std::optional<FileMetadata>(
        const FilePath&)>;

    using ChunkLookup = std::function<std::optional<ChunkMetadata>(
        const FilePath&, ChunkIndex)>;

    using ChunkAllocation = std::function<std::optional<ChunkMetadata>(
        const FilePath&, ChunkIndex)>;

    MasterClient() = default;

    explicit MasterClient(std::string master_address);

    MasterClient(std::string master_address,
                 FileLookup file_lookup,
                 ChunkLookup chunk_lookup,
                 ChunkAllocation chunk_allocator);

    MasterClient(const MasterClient&) = delete;
    MasterClient& operator=(const MasterClient&) = delete;

    MasterClient(MasterClient&&) noexcept = default;
    MasterClient& operator=(MasterClient&&) noexcept = default;

    ~MasterClient() = default;

    void SetMasterAddress(std::string master_address);

    [[nodiscard]] const std::string& GetMasterAddress() const noexcept;

    void SetFileLookup(FileLookup lookup);
    void SetChunkLookup(ChunkLookup lookup);
    void SetChunkAllocator(ChunkAllocation allocator);

    [[nodiscard]] bool IsConfigured() const noexcept;

    [[nodiscard]] std::optional<FileMetadata> LookupFile(
        const FilePath& path) const;

    [[nodiscard]] std::optional<ChunkHandle> LookupChunkHandle(
        const FilePath& path,
        ChunkIndex chunk_index) const;

    [[nodiscard]] std::optional<ChunkMetadata> LookupChunk(
        const FilePath& path,
        ChunkIndex chunk_index) const;

    [[nodiscard]] std::vector<ChunkLocation> LookupChunkLocations(
        const FilePath& path,
        ChunkIndex chunk_index) const;

    [[nodiscard]] std::optional<ChunkMetadata> AllocateChunk(
        const FilePath& path,
        ChunkIndex chunk_index) const;

private:
    std::string master_address_;
    FileLookup file_lookup_;
    ChunkLookup chunk_lookup_;
    ChunkAllocation chunk_allocator_;
};

}  // namespace gfs::client::metadata