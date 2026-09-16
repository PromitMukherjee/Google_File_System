#include "gfs/common/types.hpp"

namespace gfs {

static_assert(sizeof(ChunkHandle) == sizeof(std::uint64_t));
static_assert(sizeof(ChunkIndex) == sizeof(std::uint64_t));
static_assert(sizeof(ChunkVersion) == sizeof(std::uint64_t));
static_assert(sizeof(ServerId) == sizeof(std::uint64_t));

}  // namespace gfs