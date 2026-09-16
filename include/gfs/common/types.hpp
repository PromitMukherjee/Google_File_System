#pragma once

#include <cstdint>
#include <string>

namespace gfs {

using ChunkHandle = std::uint64_t;
using ChunkIndex = std::uint64_t;
using ChunkVersion = std::uint64_t;
using ServerId = std::uint64_t;
using FilePath = std::string;

}  // namespace gfs