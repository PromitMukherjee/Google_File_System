#include "gfs/client/io/reader.hpp"

#include "gfs/common/constants.hpp"

#include <algorithm>
#include <limits>

namespace gfs::client::io {

Reader::Reader(
    metadata::MasterClient& master_client,
    metadata::ChunkLocationCache& location_cache,
    ReadFunction read_function)
    : master_client_(master_client),
      location_cache_(location_cache),
      read_function_(std::move(read_function)) {}

bool Reader::Read(
    const FilePath& path,
    std::uint64_t offset,
    std::size_t length,
    std::string& data) {
    data.clear();

    if (path.empty()) {
        return false;
    }

    if (length == 0) {
        return true;
    }

    const auto file_metadata = master_client_.LookupFile(path);

    if (!file_metadata.has_value()) {
        return false;
    }

    if (offset > file_metadata->size) {
        return false;
    }

    if (offset == file_metadata->size) {
        return true;
    }

    const std::uint64_t available =
        file_metadata->size - offset;

    const std::size_t actual_length =
        static_cast<std::size_t>(
            std::min<std::uint64_t>(
                available,
                static_cast<std::uint64_t>(length)));

    return ReadSequential(
        path,
        offset,
        actual_length,
        data);
}

bool Reader::ReadChunk(
    const FilePath& path,
    ChunkIndex chunk_index,
    std::uint64_t chunk_offset,
    std::size_t length,
    std::string& data) {
    data.clear();

    if (path.empty() ||
        length == 0 ||
        chunk_offset > constants::kChunkSize) {
        return false;
    }

    if (chunk_offset +
            static_cast<std::uint64_t>(length) >
        constants::kChunkSize) {
        return false;
    }

    metadata::ChunkLocationCacheEntry entry;

    if (!ResolveChunk(path, chunk_index, entry)) {
        return false;
    }

    return ReadFromChunkserver(
        entry,
        chunk_offset,
        length,
        data);
}

bool Reader::ReadSequential(
    const FilePath& path,
    std::uint64_t offset,
    std::size_t length,
    std::string& data) {
    data.clear();

    if (path.empty()) {
        return false;
    }

    if (length == 0) {
        return true;
    }

    std::size_t remaining = length;
    std::uint64_t current_offset = offset;

    while (remaining > 0) {
        const ChunkIndex chunk_index =
            current_offset / constants::kChunkSize;

        const std::uint64_t chunk_offset =
            current_offset % constants::kChunkSize;

        const std::uint64_t bytes_until_boundary =
            constants::kChunkSize - chunk_offset;

        const std::size_t request_length =
            static_cast<std::size_t>(
                std::min<std::uint64_t>(
                    bytes_until_boundary,
                    static_cast<std::uint64_t>(remaining)));

        metadata::ChunkLocationCacheEntry entry;

        if (!ResolveChunk(path, chunk_index, entry)) {
            data.clear();
            return false;
        }

        std::string chunk_data;

        if (!ReadFromChunkserver(
                entry,
                chunk_offset,
                request_length,
                chunk_data)) {
            data.clear();
            return false;
        }

        data.append(chunk_data);

        current_offset += request_length;
        remaining -= request_length;
    }

    return data.size() == length;
}

bool Reader::ResolveChunk(
    const FilePath& path,
    ChunkIndex chunk_index,
    metadata::ChunkLocationCacheEntry& entry) {
    const auto cached =
        location_cache_.Lookup(path, chunk_index);

    if (cached.has_value()) {
        entry = *cached;
        return true;
    }

    const auto chunk =
        master_client_.LookupChunk(path, chunk_index);

    if (!chunk.has_value() ||
        chunk->handle == 0 ||
        chunk->locations.empty()) {
        return false;
    }

    entry.handle = chunk->handle;
    entry.chunk_index = chunk_index;
    entry.locations = chunk->locations;
    entry.inserted_at = std::chrono::steady_clock::now();

    location_cache_.Insert(
        path,
        chunk_index,
        entry);

    return true;
}

bool Reader::ReadFromChunkserver(
    const metadata::ChunkLocationCacheEntry& entry,
    std::uint64_t offset,
    std::size_t length,
    std::string& data) {
    data.clear();

    if (!read_function_ ||
        entry.handle == 0 ||
        entry.locations.empty()) {
        return false;
    }

    for (const auto& location : entry.locations) {
        if (location.server_id == 0) {
            continue;
        }

        std::string candidate;

        if (read_function_(
                location,
                entry.handle,
                offset,
                length,
                candidate)) {
            data = std::move(candidate);
            return data.size() == length;
        }
    }

    return false;
}

}  // namespace gfs::client::io