#pragma once

#include "gfs/common/types.hpp"
#include "gfs/master/lease/lease.hpp"
#include "gfs/master/replication/replica_manager.hpp"

#include <cstddef>
#include <cstdint>
#include <optional>
#include <shared_mutex>
#include <unordered_map>

namespace gfs::master::lease {

class LeaseManager {
public:
    explicit LeaseManager(
        const replication::ReplicaManager& replica_manager,
        std::uint64_t lease_duration_ms = 60000);

    LeaseManager(const LeaseManager&) = delete;
    LeaseManager& operator=(const LeaseManager&) = delete;

    [[nodiscard]] std::uint64_t GetLeaseDurationMs()
        const noexcept;

    void SetLeaseDurationMs(
        std::uint64_t lease_duration_ms);

    [[nodiscard]] std::optional<Lease> AcquireLease(
        ChunkHandle handle,
        ServerId primary_server_id,
        ChunkVersion version = 1);

    [[nodiscard]] std::optional<Lease> GetLease(
        ChunkHandle handle) const;

    [[nodiscard]] bool HasLease(
        ChunkHandle handle) const;

    [[nodiscard]] bool IsLeaseValid(
        ChunkHandle handle) const;

    [[nodiscard]] bool IsLeaseValid(
        ChunkHandle handle,
        ServerId primary_server_id) const;

    [[nodiscard]] bool ExtendLease(
        ChunkHandle handle,
        ServerId primary_server_id);

    [[nodiscard]] bool ExtendLease(
        ChunkHandle handle,
        ServerId primary_server_id,
        std::uint64_t extension_ms);

    [[nodiscard]] bool ReleaseLease(
        ChunkHandle handle);

    [[nodiscard]] std::size_t LeaseCount() const;

    std::size_t RemoveExpiredLeases();

    void Clear();

private:
    [[nodiscard]] bool IsCurrentPrimary(
        ChunkHandle handle,
        ServerId server_id) const;

    [[nodiscard]] std::uint64_t CurrentTimeMs() const;

    const replication::ReplicaManager& replica_manager_;

    mutable std::shared_mutex mutex_;

    std::unordered_map<ChunkHandle, Lease> leases_;

    std::uint64_t lease_duration_ms_;
};

}  // namespace gfs::master::lease