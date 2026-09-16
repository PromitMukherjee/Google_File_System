#include "gfs/storage/file_handle.hpp"

#include <cerrno>
#include <fcntl.h>
#include <limits>
#include <sys/stat.h>
#include <unistd.h>
#include <utility>

namespace gfs::storage {

FileHandle::FileHandle() noexcept
    : fd_(-1) {
}

FileHandle::~FileHandle() {
    Close();
}

FileHandle::FileHandle(
    FileHandle&& other) noexcept
    : fd_(other.fd_),
      path_(std::move(other.path_)) {

    other.fd_ = -1;
    other.path_.clear();
}

FileHandle& FileHandle::operator=(
    FileHandle&& other) noexcept {

    if (this == &other) {
        return *this;
    }

    Close();

    fd_ = other.fd_;
    path_ = std::move(other.path_);

    other.fd_ = -1;
    other.path_.clear();

    return *this;
}

bool FileHandle::Open(
    const std::string& path,
    OpenMode mode,
    bool create,
    bool truncate) {

    if (path.empty() || IsOpen()) {
        return false;
    }

    int flags = 0;

    switch (mode) {
        case OpenMode::ReadOnly:
            flags = O_RDONLY;
            break;

        case OpenMode::WriteOnly:
            flags = O_WRONLY;
            break;

        case OpenMode::ReadWrite:
            flags = O_RDWR;
            break;
    }

    if (create) {
        flags |= O_CREAT;
    }

    if (truncate) {
        flags |= O_TRUNC;
    }

    const int fd =
        ::open(
            path.c_str(),
            flags,
            0644);

    if (fd < 0) {
        return false;
    }

    fd_ = fd;
    path_ = path;

    return true;
}

bool FileHandle::Close() noexcept {
    if (fd_ < 0) {
        return true;
    }

    const int result = ::close(fd_);

    fd_ = -1;
    path_.clear();

    return result == 0;
}

bool FileHandle::IsOpen() const noexcept {
    return fd_ >= 0;
}

int FileHandle::GetFileDescriptor() const noexcept {
    return fd_;
}

const std::string&
FileHandle::GetPath() const noexcept {
    return path_;
}

std::optional<std::size_t>
FileHandle::Read(
    void* buffer,
    std::size_t length) {

    if (!IsOpen() ||
        buffer == nullptr && length != 0) {
        return std::nullopt;
    }

    std::size_t total = 0;

    while (total < length) {
        const ssize_t result =
            ::read(
                fd_,
                static_cast<std::uint8_t*>(buffer) + total,
                length - total);

        if (result < 0) {
            if (errno == EINTR) {
                continue;
            }

            return std::nullopt;
        }

        if (result == 0) {
            break;
        }

        total += static_cast<std::size_t>(result);
    }

    return total;
}

std::optional<std::size_t>
FileHandle::ReadAt(
    std::uint64_t offset,
    void* buffer,
    std::size_t length) const {

    if (!IsOpen() ||
        buffer == nullptr && length != 0 ||
        offset >
            static_cast<std::uint64_t>(
                std::numeric_limits<off_t>::max())) {
        return std::nullopt;
    }

    std::size_t total = 0;

    while (total < length) {
        const std::uint64_t current_offset =
            offset + total;

        if (current_offset >
            static_cast<std::uint64_t>(
                std::numeric_limits<off_t>::max())) {
            return std::nullopt;
        }

        const ssize_t result =
            ::pread(
                fd_,
                static_cast<std::uint8_t*>(buffer) + total,
                length - total,
                static_cast<off_t>(current_offset));

        if (result < 0) {
            if (errno == EINTR) {
                continue;
            }

            return std::nullopt;
        }

        if (result == 0) {
            break;
        }

        total += static_cast<std::size_t>(result);
    }

    return total;
}

std::optional<std::size_t>
FileHandle::Write(
    const void* buffer,
    std::size_t length) {

    if (!IsOpen() ||
        buffer == nullptr && length != 0) {
        return std::nullopt;
    }

    std::size_t total = 0;

    while (total < length) {
        const ssize_t result =
            ::write(
                fd_,
                static_cast<const std::uint8_t*>(buffer) + total,
                length - total);

        if (result < 0) {
            if (errno == EINTR) {
                continue;
            }

            return std::nullopt;
        }

        if (result == 0) {
            break;
        }

        total += static_cast<std::size_t>(result);
    }

    return total;
}

std::optional<std::size_t>
FileHandle::WriteAt(
    std::uint64_t offset,
    const void* buffer,
    std::size_t length) {

    if (!IsOpen() ||
        buffer == nullptr && length != 0 ||
        offset >
            static_cast<std::uint64_t>(
                std::numeric_limits<off_t>::max())) {
        return std::nullopt;
    }

    std::size_t total = 0;

    while (total < length) {
        const std::uint64_t current_offset =
            offset + total;

        if (current_offset >
            static_cast<std::uint64_t>(
                std::numeric_limits<off_t>::max())) {
            return std::nullopt;
        }

        const ssize_t result =
            ::pwrite(
                fd_,
                static_cast<const std::uint8_t*>(buffer) + total,
                length - total,
                static_cast<off_t>(current_offset));

        if (result < 0) {
            if (errno == EINTR) {
                continue;
            }

            return std::nullopt;
        }

        if (result == 0) {
            break;
        }

        total += static_cast<std::size_t>(result);
    }

    return total;
}

bool FileHandle::Seek(std::uint64_t offset) {
    if (!IsOpen() ||
        offset >
            static_cast<std::uint64_t>(
                std::numeric_limits<off_t>::max())) {
        return false;
    }

    return ::lseek(
               fd_,
               static_cast<off_t>(offset),
               SEEK_SET) != static_cast<off_t>(-1);
}

std::optional<std::uint64_t>
FileHandle::Tell() const {

    if (!IsOpen()) {
        return std::nullopt;
    }

    const off_t position =
        ::lseek(fd_, 0, SEEK_CUR);

    if (position == static_cast<off_t>(-1)) {
        return std::nullopt;
    }

    return static_cast<std::uint64_t>(position);
}

std::optional<std::uint64_t>
FileHandle::Size() const {

    if (!IsOpen()) {
        return std::nullopt;
    }

    struct stat status {};

    if (::fstat(fd_, &status) != 0) {
        return std::nullopt;
    }

    if (status.st_size < 0) {
        return std::nullopt;
    }

    return static_cast<std::uint64_t>(status.st_size);
}

bool FileHandle::Truncate(std::uint64_t size) {
    if (!IsOpen() ||
        size >
            static_cast<std::uint64_t>(
                std::numeric_limits<off_t>::max())) {
        return false;
    }

    return ::ftruncate(
               fd_,
               static_cast<off_t>(size)) == 0;
}

}  // namespace gfs::storage