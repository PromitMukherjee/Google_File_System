#include "gfs/client/io/writer.hpp"

#include "gfs/common/constants.hpp"

#include <algorithm>
#include <chrono>
#include <utility>

namespace gfs::client::io {

Writer::Writer(
    metadata::MasterClient& master_client,
    metadata::ChunkLocationCache& location_cache,
    WriteFunction write_function,
    FileSizeUpdater file_size_updater)
    : master_client_(master_client),
      location_cache_(location_cache),
      write_function_(std::move(write_function)),
      file_size_updater_(std::move(file_size_updater)) {}

bool Writer::Write(
    const FilePath& path,
    std::uint64_t offset,
    const std::string& data) {
    if (path.empty()) {
        return false;
    }

    if (data.empty()) {
        return true;
    }

    return WriteSequential(path, offset, data);
}

bool Writer::WriteChunk(
    const FilePath& path,
    ChunkIndex chunk_index,
    std::uint64_t chunk_offset,
    const std::string& data) {
    if (path.empty() ||
        data.empty() ||
        chunk_offset > constants::kChunkSize) {
        return false;
    }

    if (chunk_offset +
            static_cast<std::uint64_t>(data.size()) >
        constants::kChunkSize) {
        return false;
    }

    metadata::ChunkLocationCacheEntry entry;

    if (!ResolveOrAllocateChunk(
            path,
            chunk_index,
            entry)) {
        return false;
    }

    if (!WriteToChunkserver(
            entry,
            chunk_offset,
            data)) {
        return false;
    }

    return true;
}

bool Writer::WriteSequential(
    const FilePath& path,
    std::uint64_t offset,
    const std::string& data) {
    if (path.empty() || data.empty()) {
        return false;
    }

    std::size_t remaining = data.size();
    std::size_t data_position = 0;
    std::uint64_t current_offset = offset;

    while (remaining > 0) {
        const ChunkIndex chunk_index =
            current_offset / constants::kChunkSize;

        const std::uint64_t chunk_offset =
            current_offset % constants::kChunkSize;

        const std::uint64_t bytes_until_boundary =
            constants::kChunkSize - chunk_offset;

        const std::size_t write_length =
            static_cast<std::size_t>(
                std::min<std::uint64_t>(
                    bytes_until_boundary,
                    static_cast<std::uint64_t>(remaining)));

        const std::string chunk_data =
            data.substr(data_position, write_length);

        metadata::ChunkLocationCacheEntry entry;

        if (!ResolveOrAllocateChunk(
                path,
                chunk_index,
                entry)) {
            return false;
        }

        if (!WriteToChunkserver(
                entry,
                chunk_offset,
                chunk_data)) {
            return false;
        }

        current_offset += write_length;
        data_position += write_length;
        remaining -= write_length;
    }

    if (file_size_updater_) {
        const std::uint64_t end_offset =
            offset + static_cast<std::uint64_t>(data.size());

        if (!file_size_updater_(path, end_offset)) {
            return false;
        }
    }

    return true;
}

bool Writer::ResolveOrAllocateChunk(
    const FilePath& path,
    ChunkIndex chunk_index,
    metadata::ChunkLocationCacheEntry& entry) {
    const auto cached =
        location_cache_.Lookup(path, chunk_index);

    if (cached.has_value()) {
        entry = *cached;
        return true;
    }

    auto chunk =
        master_client_.LookupChunk(path, chunk_index);

    if (!chunk.has_value()) {
        chunk =
            master_client_.AllocateChunk(path, chunk_index);
    }

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

bool Writer::WriteToChunkserver(
    const metadata::ChunkLocationCacheEntry& entry,
    std::uint64_t offset,
    const std::string& data) {
    if (!write_function_ ||
        entry.handle == 0 ||
        entry.locations.empty() ||
        data.empty()) {
        return false;
    }

    for (const auto& location : entry.locations) {
        if (location.server_id == 0) {
            continue;
        }

        if (write_function_(
                location,
                entry.handle,
                offset,
                data)) {
            return true;
        }
    }

    return false;
}

}  // namespace gfs::client::io