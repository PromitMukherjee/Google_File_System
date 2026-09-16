#include "gfs/common/utils.hpp"

#include <chrono>

namespace gfs {

std::string JoinPath(
    const FilePath& base,
    const FilePath& child
) {
    if (base.empty()) {
        return child;
    }

    if (child.empty()) {
        return base;
    }

    if (base.back() == '/') {
        if (child.front() == '/') {
            return base + child.substr(1);
        }

        return base + child;
    }

    if (child.front() == '/') {
        return base + child;
    }

    return base + "/" + child;
}

bool IsValidFilePath(const FilePath& path) {
    if (path.empty() || path.front() != '/') {
        return false;
    }

    if (path.find('\0') != std::string::npos) {
        return false;
    }

    return true;
}

std::uint64_t UnixTimeMillis() {
    const auto now = std::chrono::system_clock::now();

    const auto duration =
        std::chrono::duration_cast<std::chrono::milliseconds>(
            now.time_since_epoch()
        );

    return static_cast<std::uint64_t>(duration.count());
}

}  // namespace gfs