#include "gfs/client/retry/retry_policy.hpp"

namespace gfs::client::retry {

RetryPolicy::RetryPolicy(
    std::size_t max_attempts) noexcept
    : max_attempts_(
          max_attempts == 0 ? 1 : max_attempts) {
}

std::size_t RetryPolicy::MaxAttempts()
    const noexcept {
    return max_attempts_;
}

bool RetryPolicy::CanRetry(
    std::size_t attempts_used) const noexcept {
    return attempts_used < max_attempts_;
}

}  // namespace gfs::client::retry