#pragma once

#include "gfs/common/types.hpp"

#include <cstddef>
#include <cstdint>
#include <mutex>
#include <unordered_map>

namespace gfs::testing {

enum class FailureOperation : std::uint32_t {
    ChunkserverUnavailable = 1,
    ChunkserverRestart = 2,
    ChunkserverFailure = 3,
    ReplicaTransfer = 4,
    ReplicaRead = 5,
    ReplicaWrite = 6,
    NetworkRpc = 7,
    Heartbeat = 8,
    DelayedHeartbeat = 9,
    MutationPropagation = 10,
    Replication = 11,
    ReReplication = 12,
    GarbageCollectionDeletion = 13,
    RebalanceTransfer = 14
};

class FailureInjector {
public:
    FailureInjector() = default;

    FailureInjector(const FailureInjector&) = delete;
    FailureInjector& operator=(const FailureInjector&) = delete;

    void Enable() noexcept;

    void Disable() noexcept;

    [[nodiscard]] bool IsEnabled() const noexcept;

    void FailNext(
        FailureOperation operation,
        ServerId server_id = 0,
        std::size_t count = 1);

    void SetFailureCount(
        FailureOperation operation,
        ServerId server_id,
        std::size_t count);

    [[nodiscard]] bool ShouldFail(
        FailureOperation operation,
        ServerId server_id = 0);

    [[nodiscard]] std::size_t GetFailureCount(
        FailureOperation operation,
        ServerId server_id = 0) const;

    void Clear(
        FailureOperation operation,
        ServerId server_id = 0);

    void ClearAll();

private:
    struct FailureKey {
        FailureOperation operation;
        ServerId server_id;

        friend bool operator==(
            const FailureKey& lhs,
            const FailureKey& rhs) = default;
    };

    struct FailureKeyHash {
        [[nodiscard]] std::size_t operator()(
            const FailureKey& key) const noexcept;
    };

    mutable std::mutex mutex_;
    bool enabled_ = true;

    std::unordered_map<
        FailureKey,
        std::size_t,
        FailureKeyHash>
        failures_;
};

}  // namespace gfs::testing