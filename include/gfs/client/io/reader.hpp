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

class Reader {
public:
    using ReadFunction = std::function<bool(
        const metadata::ChunkLocation&,
        ChunkHandle,
        std::uint64_t,
        std::size_t,
        std::string&)>;

    Reader(metadata::MasterClient& master_client,
           metadata::ChunkLocationCache& location_cache,
           ReadFunction read_function);

    [[nodiscard]] bool Read(
        const FilePath& path,
        std::uint64_t offset,
        std::size_t length,
        std::string& data);

    [[nodiscard]] bool ReadChunk(
        const FilePath& path,
        ChunkIndex chunk_index,
        std::uint64_t chunk_offset,
        std::size_t length,
        std::string& data);

    [[nodiscard]] bool ReadSequential(
        const FilePath& path,
        std::uint64_t offset,
        std::size_t length,
        std::string& data);

private:
    [[nodiscard]] bool ResolveChunk(
        const FilePath& path,
        ChunkIndex chunk_index,
        metadata::ChunkLocationCacheEntry& entry);

    [[nodiscard]] bool ReadFromChunkserver(
        const metadata::ChunkLocationCacheEntry& entry,
        std::uint64_t offset,
        std::size_t length,
        std::string& data);

    metadata::MasterClient& master_client_;
    metadata::ChunkLocationCache& location_cache_;
    ReadFunction read_function_;
};

}  // namespace gfs::client::io