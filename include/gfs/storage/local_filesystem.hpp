#pragma once

#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

namespace gfs::storage {

class LocalFilesystem {
public:
    LocalFilesystem() = delete;

    static bool CreateDirectory(
        const std::string& path);

    static bool DirectoryExists(
        const std::string& path);

    static bool FileExists(
        const std::string& path);

    static bool RemoveFile(
        const std::string& path);

    static bool RemoveDirectory(
        const std::string& path);

    static std::uint64_t FileSize(
        const std::string& path);

    static bool ReadFile(
        const std::string& path,
        std::uint64_t offset,
        std::size_t length,
        std::vector<std::uint8_t>& data);

    static bool WriteFile(
        const std::string& path,
        std::uint64_t offset,
        const std::vector<std::uint8_t>& data);

    static bool TruncateFile(
        const std::string& path,
        std::uint64_t size);

    static std::string JoinPath(
        const std::string& directory,
        const std::string& filename);
};

}  // namespace gfs::storage