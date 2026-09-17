#include "gfs/master/lease/lease_manager.hpp"

#include "gfs/common/utils.hpp"

#include <limits>
#include <mutex>

namespace gfs::master::lease {

LeaseManager::LeaseManager(
    const replication::ReplicaManager& replica_manager,
    std::uint64_t lease_duration_ms)
    : replica_manager_(replica_manager),
      lease_duration_ms_(lease_duration_ms) {
}

std::uint64_t LeaseManager::GetLeaseDurationMs()
    const noexcept {
    std::shared_lock lock(mutex_);
    return lease_duration_ms_;
}

void LeaseManager::SetLeaseDurationMs(
    std::uint64_t lease_duration_ms) {
    std::unique_lock lock(mutex_);
    lease_duration_ms_ = lease_duration_ms;
}

std::optional<Lease> LeaseManager::AcquireLease(
    ChunkHandle handle,
    ServerId primary_server_id,
    ChunkVersion version) {
    if (handle == 0 ||
        primary_server_id == 0 ||
        version == 0) {
        return std::nullopt;
    }

    if (!IsCurrentPrimary(
            handle,
            primary_server_id)) {
        return std::nullopt;
    }

    const std::uint64_t now_ms =
        CurrentTimeMs();

    std::unique_lock lock(mutex_);

    const auto existing =
        leases_.find(handle);

    if (existing != leases_.end()) {
        if (existing->second.IsValid(now_ms)) {
            return std::nullopt;
        }

        leases_.erase(existing);
    }

    if (lease_duration_ms_ == 0) {
        return std::nullopt;
    }

    const std::uint64_t max_time =
        std::numeric_limits<std::uint64_t>::max();

    const std::uint64_t expiration =
        lease_duration_ms_ >
                max_time - now_ms
            ? max_time
            : now_ms + lease_duration_ms_;

    Lease lease;
    lease.chunk_handle = handle;
    lease.primary_server_id =
        primary_server_id;
    lease.version = version;
    lease.expiration_time_ms = expiration;

    leases_[handle] = lease;

    return lease;
}

std::optional<Lease> LeaseManager::GetLease(
    ChunkHandle handle) const {
    if (handle == 0) {
        return std::nullopt;
    }

    std::shared_lock lock(mutex_);

    const auto it = leases_.find(handle);

    if (it == leases_.end()) {
        return std::nullopt;
    }

    return it->second;
}

bool LeaseManager::HasLease(
    ChunkHandle handle) const {
    return GetLease(handle).has_value();
}

bool LeaseManager::IsLeaseValid(
    ChunkHandle handle) const {
    const auto lease = GetLease(handle);

    if (!lease.has_value()) {
        return false;
    }

    return lease->IsValid(CurrentTimeMs()) &&
           IsCurrentPrimary(
               handle,
               lease->primary_server_id);
}

bool LeaseManager::IsLeaseValid(
    ChunkHandle handle,
    ServerId primary_server_id) const {
    if (primary_server_id == 0) {
        return false;
    }

    const auto lease = GetLease(handle);

    if (!lease.has_value()) {
        return false;
    }

    return lease->primary_server_id ==
               primary_server_id &&
           lease->IsValid(CurrentTimeMs()) &&
           IsCurrentPrimary(
               handle,
               primary_server_id);
}

bool LeaseManager::ExtendLease(
    ChunkHandle handle,
    ServerId primary_server_id) {
    return ExtendLease(
        handle,
        primary_server_id,
        lease_duration_ms_);
}

bool LeaseManager::ExtendLease(
    ChunkHandle handle,
    ServerId primary_server_id,
    std::uint64_t extension_ms) {
    if (handle == 0 ||
        primary_server_id == 0 ||
        extension_ms == 0) {
        return false;
    }

    if (!IsCurrentPrimary(
            handle,
            primary_server_id)) {
        return false;
    }

    const std::uint64_t now_ms =
        CurrentTimeMs();

    std::unique_lock lock(mutex_);

    const auto it = leases_.find(handle);

    if (it == leases_.end()) {
        return false;
    }

    Lease& lease = it->second;

    if (!lease.IsValid(now_ms) ||
        lease.primary_server_id !=
            primary_server_id) {
        return false;
    }

    const std::uint64_t max_time =
        std::numeric_limits<std::uint64_t>::max();

    const std::uint64_t base =
        lease.expiration_time_ms >
                now_ms
            ? lease.expiration_time_ms
            : now_ms;

    lease.expiration_time_ms =
        extension_ms >
                max_time - base
            ? max_time
            : base + extension_ms;

    return true;
}

bool LeaseManager::ReleaseLease(
    ChunkHandle handle) {
    if (handle == 0) {
        return false;
    }

    std::unique_lock lock(mutex_);

    return leases_.erase(handle) > 0;
}

std::size_t LeaseManager::LeaseCount() const {
    std::shared_lock lock(mutex_);
    return leases_.size();
}

std::size_t LeaseManager::RemoveExpiredLeases() {
    const std::uint64_t now_ms =
        CurrentTimeMs();

    std::unique_lock lock(mutex_);

    std::size_t removed = 0;

    for (auto it = leases_.begin();
         it != leases_.end();) {
        if (it->second.IsExpired(now_ms)) {
            it = leases_.erase(it);
            ++removed;
        } else {
            ++it;
        }
    }

    return removed;
}

void LeaseManager::Clear() {
    std::unique_lock lock(mutex_);
    leases_.clear();
}

bool LeaseManager::IsCurrentPrimary(
    ChunkHandle handle,
    ServerId server_id) const {
    if (handle == 0 ||
        server_id == 0) {
        return false;
    }

    const auto primary =
        replica_manager_.GetPrimary(handle);

    return primary.has_value() &&
           *primary == server_id &&
           replica_manager_.HasReplica(
               handle,
               server_id);
}

std::uint64_t LeaseManager::CurrentTimeMs() const {
    return gfs::UnixTimeMillis();
}

}  // namespace gfs::master::lease