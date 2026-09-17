#include "gfs/master/lease/lease.hpp"

namespace gfs::master::lease {

bool Lease::IsExpired(
    std::uint64_t now_ms) const noexcept {
    return expiration_time_ms == 0 ||
           now_ms >= expiration_time_ms;
}

bool Lease::IsValid(
    std::uint64_t now_ms) const noexcept {
    return chunk_handle != 0 &&
           primary_server_id != 0 &&
           version != 0 &&
           expiration_time_ms != 0 &&
           now_ms < expiration_time_ms;
}

std::uint64_t Lease::RemainingTimeMs(
    std::uint64_t now_ms) const noexcept {
    if (IsExpired(now_ms)) {
        return 0;
    }

    return expiration_time_ms - now_ms;
}

}  // namespace gfs::master::lease