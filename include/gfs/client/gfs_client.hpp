#pragma once

#include "gfs/client/io/reader.hpp"
#include "gfs/client/io/writer.hpp"
#include "gfs/client/metadata/chunk_location_cache.hpp"
#include "gfs/client/metadata/master_client.hpp"
#include "gfs/common/types.hpp"

#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <vector>

namespace gfs::client {

class GFSClient {
public:
    using FileCreateFunction = std::function<bool(const FilePath&)>;

    GFSClient(
        std::unique_ptr<metadata::MasterClient> master_client,
        std::unique_ptr<metadata::ChunkLocationCache> location_cache,
        io::Reader::ReadFunction read_function,
        io::Writer::WriteFunction write_function,
        io::Writer::FileSizeUpdater file_size_updater = {});

    GFSClient(const GFSClient&) = delete;
    GFSClient& operator=(const GFSClient&) = delete;

    GFSClient(GFSClient&&) noexcept = delete;
    GFSClient& operator=(GFSClient&&) noexcept = delete;

    ~GFSClient();

    void SetFileCreateFunction(FileCreateFunction create_function);

    [[nodiscard]] bool CreateFile(const FilePath& path);

    [[nodiscard]] bool FileExists(const FilePath& path) const;

    [[nodiscard]] std::optional<metadata::FileMetadata> LookupFile(
        const FilePath& path) const;

    [[nodiscard]] std::optional<metadata::ChunkMetadata> LookupChunk(
        const FilePath& path,
        ChunkIndex chunk_index) const;

    [[nodiscard]] std::vector<metadata::ChunkLocation> LookupChunkLocations(
        const FilePath& path,
        ChunkIndex chunk_index) const;

    [[nodiscard]] bool Read(
        const FilePath& path,
        std::uint64_t offset,
        std::size_t length,
        std::string& data);

    [[nodiscard]] bool Write(
        const FilePath& path,
        std::uint64_t offset,
        const std::string& data);

    [[nodiscard]] metadata::MasterClient& GetMasterClient() noexcept;

    [[nodiscard]] const metadata::MasterClient& GetMasterClient() const noexcept;

    [[nodiscard]] metadata::ChunkLocationCache& GetLocationCache() noexcept;

    [[nodiscard]] const metadata::ChunkLocationCache&
    GetLocationCache() const noexcept;

private:
    std::unique_ptr<metadata::MasterClient> master_client_;
    std::unique_ptr<metadata::ChunkLocationCache> location_cache_;

    io::Reader reader_;
    io::Writer writer_;

    FileCreateFunction file_create_function_;
};

}  // namespace gfs::client