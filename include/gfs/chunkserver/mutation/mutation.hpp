#pragma once

#include "gfs/common/constants.hpp"
#include "gfs/common/types.hpp"

#include <cstdint>
#include <string>

namespace gfs::chunkserver::mutation {

struct Mutation {
    ChunkHandle chunk_handle = 0;
    ChunkVersion chunk_version = 1;
    std::uint64_t mutation_id = 0;
    std::uint64_t offset = 0;
    std::string data;

    [[nodiscard]] bool IsValid() const noexcept;

    [[nodiscard]] bool FitsChunk() const noexcept;

    friend bool operator==(const Mutation& lhs,
                           const Mutation& rhs) = default;
};

}  // namespace gfs::chunkserver::mutation