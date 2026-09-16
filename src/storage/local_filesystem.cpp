#include "gfs/storage/local_filesystem.hpp"

#include <cerrno>
#include <fcntl.h>
#include <filesystem>
#include <limits>
#include <sys/stat.h>
#include <unistd.h>

namespace gfs::storage {

bool LocalFilesystem::CreateDirectory(
    const std::string& path) {

    if (path.empty()) {
        return false;
    }

    std::error_code error;

    std::filesystem::create_directories(path, error);

    return !error &&
           std::filesystem::is_directory(path, error) &&
           !error;
}

bool LocalFilesystem::DirectoryExists(
    const std::string& path) {

    std::error_code error;

    const bool exists =
        std::filesystem::is_directory(path, error);

    return exists && !error;
}

bool LocalFilesystem::FileExists(
    const std::string& path) {

    std::error_code error;

    const bool exists =
        std::filesystem::is_regular_file(path, error);

    return exists && !error;
}

bool LocalFilesystem::RemoveFile(
    const std::string& path) {

    std::error_code error;

    const bool removed =
        std::filesystem::remove(path, error);

    return removed && !error;
}

bool LocalFilesystem::RemoveDirectory(
    const std::string& path) {

    std::error_code error;

    const bool removed =
        std::filesystem::remove(path, error);

    return removed && !error;
}

std::uint64_t LocalFilesystem::FileSize(
    const std::string& path) {

    std::error_code error;

    const auto size =
        std::filesystem::file_size(path, error);

    if (error) {
        return 0;
    }

    return size;
}

bool LocalFilesystem::ReadFile(
    const std::string& path,
    std::uint64_t offset,
    std::size_t length,
    std::vector<std::uint8_t>& data) {

    const int fd =
        ::open(path.c_str(), O_RDONLY);

    if (fd < 0) {
        return false;
    }

    data.resize(length);

    std::size_t total = 0;

    while (total < length) {
        const std::size_t remaining =
            length - total;

        const ssize_t result =
            ::pread(
                fd,
                data.data() + total,
                remaining,
                static_cast<off_t>(
                    offset + total));

        if (result < 0) {
            if (errno == EINTR) {
                continue;
            }

            ::close(fd);
            data.clear();
            return false;
        }

        if (result == 0) {
            break;
        }

        total += static_cast<std::size_t>(result);
    }

    ::close(fd);

    data.resize(total);
    return true;
}

bool LocalFilesystem::WriteFile(
    const std::string& path,
    std::uint64_t offset,
    const std::vector<std::uint8_t>& data) {

    const int fd =
        ::open(path.c_str(), O_WRONLY);

    if (fd < 0) {
        return false;
    }

    std::size_t total = 0;

    while (total < data.size()) {
        const std::size_t remaining =
            data.size() - total;

        const ssize_t result =
            ::pwrite(
                fd,
                data.data() + total,
                remaining,
                static_cast<off_t>(
                    offset + total));

        if (result < 0) {
            if (errno == EINTR) {
                continue;
            }

            ::close(fd);
            return false;
        }

        if (result == 0) {
            ::close(fd);
            return false;
        }

        total += static_cast<std::size_t>(result);
    }

    return ::close(fd) == 0;
}

bool LocalFilesystem::TruncateFile(
    const std::string& path,
    std::uint64_t size) {

    if (size >
        static_cast<std::uint64_t>(
            std::numeric_limits<off_t>::max())) {
        return false;
    }

    return ::truncate(
               path.c_str(),
               static_cast<off_t>(size)) == 0;
}

std::string LocalFilesystem::JoinPath(
    const std::string& directory,
    const std::string& filename) {

    return (
        std::filesystem::path(directory) /
        filename
    ).string();
}

}  // namespace gfs::storage