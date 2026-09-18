#pragma once

#include "gfs/client/metadata/chunk_location_cache.hpp"
#include "gfs/client/metadata/master_client.hpp"
#include "gfs/common/types.hpp"

#include <cstddef>
#include <cstdint>
#include <functional>
#include <string>
#include <vector>

namespace gfs::client::io {

class Writer {
public:
    using WriteFunction = std::function<bool(
        const metadata::ChunkLocation&,
        ChunkHandle,
        std::uint64_t,
        const std::string&)>;

    using FileSizeUpdater = std::function<bool(
        const FilePath&,
        std::uint64_t)>;

    using ChunkSizeUpdater = std::function<bool(
        ChunkHandle,
        std::uint64_t)>;

    using CopyOnWriteFunction = std::function<bool(
        const FilePath&,
        ChunkIndex,
        ChunkHandle)>;

    Writer(metadata::MasterClient& master_client,
           metadata::ChunkLocationCache& location_cache,
           WriteFunction write_function,
           FileSizeUpdater file_size_updater = {});

    [[nodiscard]] bool Write(
        const FilePath& path,
        std::uint64_t offset,
        const std::string& data);

    [[nodiscard]] bool WriteChunk(
        const FilePath& path,
        ChunkIndex chunk_index,
        std::uint64_t chunk_offset,
        const std::string& data);

    [[nodiscard]] bool WriteSequential(
        const FilePath& path,
        std::uint64_t offset,
        const std::string& data);

    void SetChunkSizeUpdater(
        ChunkSizeUpdater updater);

    void SetCopyOnWriteFunction(
        CopyOnWriteFunction copy_on_write_function);

private:
    [[nodiscard]] bool ResolveOrAllocateChunk(
        const FilePath& path,
        ChunkIndex chunk_index,
        metadata::ChunkLocationCacheEntry& entry);

    [[nodiscard]] bool WriteToChunkserver(
        const metadata::ChunkLocationCacheEntry& entry,
        std::uint64_t offset,
        const std::string& data);

    metadata::MasterClient& master_client_;
    metadata::ChunkLocationCache& location_cache_;
    WriteFunction write_function_;
    FileSizeUpdater file_size_updater_;
    ChunkSizeUpdater chunk_size_updater_;
    CopyOnWriteFunction copy_on_write_function_;
};

}  // namespace gfs::client::io