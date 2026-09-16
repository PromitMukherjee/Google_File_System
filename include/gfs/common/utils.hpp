#pragma once

#include <cstdint>
#include <string>

#include "gfs/common/types.hpp"

namespace gfs {

std::string JoinPath(
    const FilePath& base,
    const FilePath& child
);

bool IsValidFilePath(
    const FilePath& path
);

std::uint64_t UnixTimeMillis();

}  // namespace gfs