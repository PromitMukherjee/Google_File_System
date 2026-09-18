#include "gfs/testing/failure_injector.hpp"

#include <functional>

namespace gfs::testing {

std::size_t FailureInjector::FailureKeyHash::operator()(
    const FailureKey& key) const noexcept {
    const std::size_t operation_hash =
        std::hash<std::uint32_t>{}(
            static_cast<std::uint32_t>(
                key.operation));

    const std::size_t server_hash =
        std::hash<ServerId>{}(key.server_id);

    return operation_hash ^
           (server_hash +
            static_cast<std::size_t>(
                0x9e3779b9U) +
            (operation_hash << 6U) +
            (operation_hash >> 2U));
}

void FailureInjector::Enable() noexcept {
    std::lock_guard lock(mutex_);
    enabled_ = true;
}

void FailureInjector::Disable() noexcept {
    std::lock_guard lock(mutex_);
    enabled_ = false;
}

bool FailureInjector::IsEnabled() const noexcept {
    std::lock_guard lock(mutex_);
    return enabled_;
}

void FailureInjector::FailNext(
    FailureOperation operation,
    ServerId server_id,
    std::size_t count) {
    if (count == 0U) {
        return;
    }

    std::lock_guard lock(mutex_);

    failures_[FailureKey{
        operation,
        server_id}] += count;
}

void FailureInjector::SetFailureCount(
    FailureOperation operation,
    ServerId server_id,
    std::size_t count) {
    std::lock_guard lock(mutex_);

    const FailureKey key{
        operation,
        server_id};

    if (count == 0U) {
        failures_.erase(key);
        return;
    }

    failures_[key] = count;
}

bool FailureInjector::ShouldFail(
    FailureOperation operation,
    ServerId server_id) {
    std::lock_guard lock(mutex_);

    if (!enabled_) {
        return false;
    }

    const FailureKey exact{
        operation,
        server_id};

    auto it = failures_.find(exact);

    if (it == failures_.end() &&
        server_id != 0) {
        it = failures_.find(
            FailureKey{
                operation,
                0});
    }

    if (it == failures_.end() ||
        it->second == 0U) {
        return false;
    }

    --it->second;

    if (it->second == 0U) {
        failures_.erase(it);
    }

    return true;
}

std::size_t FailureInjector::GetFailureCount(
    FailureOperation operation,
    ServerId server_id) const {
    std::lock_guard lock(mutex_);

    const auto exact =
        failures_.find(
            FailureKey{
                operation,
                server_id});

    if (exact != failures_.end()) {
        return exact->second;
    }

    if (server_id != 0) {
        const auto wildcard =
            failures_.find(
                FailureKey{
                    operation,
                    0});

        if (wildcard != failures_.end()) {
            return wildcard->second;
        }
    }

    return 0U;
}

void FailureInjector::Clear(
    FailureOperation operation,
    ServerId server_id) {
    std::lock_guard lock(mutex_);

    failures_.erase(
        FailureKey{
            operation,
            server_id});
}

void FailureInjector::ClearAll() {
    std::lock_guard lock(mutex_);
    failures_.clear();
}

}  // namespace gfs::testing