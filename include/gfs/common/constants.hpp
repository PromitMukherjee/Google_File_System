#pragma once

#include <cstddef>

namespace gfs::constants {

inline constexpr std::size_t kChunkSize = 64ULL * 1024ULL * 1024ULL;
inline constexpr std::size_t kDefaultReplicationFactor = 3;
inline constexpr std::size_t kChecksumBlockSize = 64ULL * 1024ULL;

}  // namespace gfs::constants