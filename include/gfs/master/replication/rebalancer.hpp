#pragma once

#include "gfs/common/types.hpp"

#include <cstdint>
#include <vector>

namespace gfs::master {

class Master;

namespace replication {

struct ReplicaMove {
    ChunkHandle handle = 0;
    ServerId source_server_id = 0;
    ServerId destination_server_id = 0;

    friend bool operator==(
        const ReplicaMove&,
        const ReplicaMove&) = default;
};

class Rebalancer {
public:
    explicit Rebalancer(
        Master& master);

    Rebalancer(
        const Rebalancer&) = delete;

    Rebalancer& operator=(
        const Rebalancer&) = delete;

    [[nodiscard]] std::vector<ReplicaMove>
    Plan(
        std::uint64_t now_ms) const;

    [[nodiscard]] std::vector<ReplicaMove>
    FindMoves(
        std::uint64_t now_ms) const;

    [[nodiscard]] bool
    ExecuteMove(
        const ReplicaMove& move,
        std::uint64_t now_ms);

    [[nodiscard]] std::vector<ReplicaMove>
    Rebalance(
        std::uint64_t now_ms);

private:
    [[nodiscard]] bool
    IsEligibleSource(
        ChunkHandle handle,
        ServerId server_id,
        std::uint64_t now_ms) const;

    [[nodiscard]] bool
    IsEligibleDestination(
        ChunkHandle handle,
        ServerId server_id,
        std::uint64_t now_ms) const;

    Master& master_;
};

}  // namespace replication
}  // namespace gfs::master