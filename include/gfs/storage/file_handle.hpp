#pragma once

#include <cstddef>
#include <cstdint>
#include <optional>
#include <string>

namespace gfs::storage {

class FileHandle {
public:
    enum class OpenMode {
        ReadOnly,
        WriteOnly,
        ReadWrite
    };

    FileHandle() noexcept;
    ~FileHandle();

    FileHandle(const FileHandle&) = delete;
    FileHandle& operator=(const FileHandle&) = delete;

    FileHandle(FileHandle&& other) noexcept;
    FileHandle& operator=(FileHandle&& other) noexcept;

    bool Open(
        const std::string& path,
        OpenMode mode,
        bool create = false,
        bool truncate = false);

    bool Close() noexcept;

    bool IsOpen() const noexcept;

    int GetFileDescriptor() const noexcept;

    const std::string& GetPath() const noexcept;

    std::optional<std::size_t> Read(
        void* buffer,
        std::size_t length);

    std::optional<std::size_t> ReadAt(
        std::uint64_t offset,
        void* buffer,
        std::size_t length) const;

    std::optional<std::size_t> Write(
        const void* buffer,
        std::size_t length);

    std::optional<std::size_t> WriteAt(
        std::uint64_t offset,
        const void* buffer,
        std::size_t length);

    bool Seek(std::uint64_t offset);

    std::optional<std::uint64_t> Tell() const;

    std::optional<std::uint64_t> Size() const;

    bool Truncate(std::uint64_t size);

private:
    int fd_;
    std::string path_;
};

}  // namespace gfs::storage