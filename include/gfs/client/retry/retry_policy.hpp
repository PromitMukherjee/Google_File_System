#pragma once

#include <cstddef>

namespace gfs::client::retry {

class RetryPolicy {
public:
    explicit RetryPolicy(
        std::size_t max_attempts = 16) noexcept;

    [[nodiscard]] std::size_t MaxAttempts()
        const noexcept;

    [[nodiscard]] bool CanRetry(
        std::size_t attempts_used) const noexcept;

private:
    std::size_t max_attempts_;
};

}  // namespace gfs::client::retry