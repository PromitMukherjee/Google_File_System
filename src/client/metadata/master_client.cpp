#include "gfs/client/metadata/master_client.hpp"

#include <utility>

namespace gfs::client::metadata {

MasterClient::MasterClient(std::string master_address)
    : master_address_(std::move(master_address)) {}

MasterClient::MasterClient(
    std::string master_address,
    FileLookup file_lookup,
    ChunkLookup chunk_lookup,
    ChunkAllocation chunk_allocator)
    : master_address_(std::move(master_address)),
      file_lookup_(std::move(file_lookup)),
      chunk_lookup_(std::move(chunk_lookup)),
      chunk_allocator_(std::move(chunk_allocator)) {}

void MasterClient::SetMasterAddress(std::string master_address) {
    master_address_ = std::move(master_address);
}

const std::string& MasterClient::GetMasterAddress() const noexcept {
    return master_address_;
}

void MasterClient::SetFileLookup(FileLookup lookup) {
    file_lookup_ = std::move(lookup);
}

void MasterClient::SetChunkLookup(ChunkLookup lookup) {
    chunk_lookup_ = std::move(lookup);
}

void MasterClient::SetChunkAllocator(ChunkAllocation allocator) {
    chunk_allocator_ = std::move(allocator);
}

bool MasterClient::IsConfigured() const noexcept {
    return static_cast<bool>(file_lookup_) &&
           static_cast<bool>(chunk_lookup_);
}

std::optional<FileMetadata> MasterClient::LookupFile(
    const FilePath& path) const {
    if (path.empty() || !file_lookup_) {
        return std::nullopt;
    }

    return file_lookup_(path);
}

std::optional<ChunkHandle> MasterClient::LookupChunkHandle(
    const FilePath& path,
    ChunkIndex chunk_index) const {
    const auto metadata = LookupChunk(path, chunk_index);

    if (!metadata.has_value()) {
        return std::nullopt;
    }

    return metadata->handle;
}

std::optional<ChunkMetadata> MasterClient::LookupChunk(
    const FilePath& path,
    ChunkIndex chunk_index) const {
    if (path.empty() || !chunk_lookup_) {
        return std::nullopt;
    }

    return chunk_lookup_(path, chunk_index);
}

std::vector<ChunkLocation> MasterClient::LookupChunkLocations(
    const FilePath& path,
    ChunkIndex chunk_index) const {
    const auto metadata = LookupChunk(path, chunk_index);

    if (!metadata.has_value()) {
        return {};
    }

    return metadata->locations;
}

std::optional<ChunkMetadata> MasterClient::AllocateChunk(
    const FilePath& path,
    ChunkIndex chunk_index) const {
    if (path.empty() || !chunk_allocator_) {
        return std::nullopt;
    }

    return chunk_allocator_(path, chunk_index);
}

}  // namespace gfs::client::metadata