#pragma once

#include "gfs/common/types.hpp"

#include <cstdint>

namespace gfs::master::lease {

struct Lease {
    ChunkHandle chunk_handle = 0;
    ServerId primary_server_id = 0;
    ChunkVersion version = 1;
    std::uint64_t expiration_time_ms = 0;

    [[nodiscard]] bool IsExpired(
        std::uint64_t now_ms) const noexcept;

    [[nodiscard]] bool IsValid(
        std::uint64_t now_ms) const noexcept;

    [[nodiscard]] std::uint64_t RemainingTimeMs(
        std::uint64_t now_ms) const noexcept;

    friend bool operator==(const Lease& lhs,
                           const Lease& rhs) = default;
};

}  // namespace gfs::master::lease