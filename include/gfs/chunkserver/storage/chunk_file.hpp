#pragma once

#include "gfs/common/constants.hpp"
#include "gfs/common/types.hpp"
#include "gfs/storage/file_handle.hpp"

#include <cstddef>
#include <cstdint>
#include <memory>
#include <shared_mutex>
#include <string>
#include <vector>

namespace gfs::chunkserver::storage {

class ChunkFile {
public:
    ChunkFile(
        ChunkHandle handle,
        std::string path);

    ~ChunkFile();

    ChunkFile(const ChunkFile&) = delete;
    ChunkFile& operator=(const ChunkFile&) = delete;
    ChunkFile(ChunkFile&&) = delete;
    ChunkFile& operator=(ChunkFile&&) = delete;

    bool Create();
    bool Open();
    bool Close();
    bool Delete();

    bool IsOpen() const noexcept;
    bool Exists() const;

    ChunkHandle GetHandle() const noexcept;
    const std::string& GetPath() const noexcept;

    std::uint64_t GetSize() const;

    bool Read(
        std::uint64_t offset,
        std::size_t length,
        std::vector<std::uint8_t>& data) const;

    bool Write(
        std::uint64_t offset,
        const std::vector<std::uint8_t>& data);

    bool Truncate(std::uint64_t size);

private:
    bool IsRangeValid(
        std::uint64_t offset,
        std::size_t length) const;

    ChunkHandle handle_;
    std::string path_;

    mutable std::shared_mutex mutex_;

    std::unique_ptr<gfs::storage::FileHandle> file_handle_;
    std::uint64_t size_ = 0;
};

}  // namespace gfs::chunkserver::storage