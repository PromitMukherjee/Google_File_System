#pragma once

#include "gfs/common/types.hpp"

#include <cstddef>
#include <cstdint>
#include <optional>
#include <shared_mutex>
#include <string>
#include <unordered_map>
#include <vector>

namespace gfs::chunkserver {
class Chunkserver;
}

namespace gfs::master {

class Master;

namespace replication {

class ReReplicationManager {
public:
    explicit ReReplicationManager(
        Master& master);

    ReReplicationManager(
        const ReReplicationManager&) = delete;

    ReReplicationManager& operator=(
        const ReReplicationManager&) = delete;

    [[nodiscard]] bool RegisterChunkserver(
        chunkserver::Chunkserver& chunkserver);

    [[nodiscard]] bool UnregisterChunkserver(
        ServerId server_id);

    [[nodiscard]] bool HasChunkserver(
        ServerId server_id) const;

    [[nodiscard]] bool RegisterChunkserverEndpoint(
        ServerId server_id,
        std::string address);

    [[nodiscard]] bool UnregisterChunkserverEndpoint(
        ServerId server_id);

    [[nodiscard]] std::optional<std::string>
    GetChunkserverEndpoint(
        ServerId server_id) const;

    [[nodiscard]] std::size_t
    RegisteredChunkserverCount() const;

    [[nodiscard]] std::size_t
    GetHealthyReplicaCount(
        ChunkHandle handle,
        std::uint64_t now_ms) const;

    [[nodiscard]] bool NeedsReReplication(
        ChunkHandle handle,
        std::uint64_t now_ms) const;

    [[nodiscard]] std::optional<ServerId>
    FindRecoverySource(
        ChunkHandle handle,
        std::uint64_t now_ms) const;

    [[nodiscard]] bool RecoverChunk(
        ChunkHandle handle,
        std::uint64_t now_ms);

    [[nodiscard]] std::vector<ChunkHandle>
    RecoverFailedChunkservers(
        std::uint64_t now_ms);

    [[nodiscard]] std::vector<ChunkHandle>
    RecoverAll(
        std::uint64_t now_ms);

    [[nodiscard]] std::vector<ServerId>
    GetRecoveryDestinations(
        ChunkHandle handle,
        std::uint64_t now_ms) const;

    [[nodiscard]] bool DeleteChunkFromChunkservers(
        ChunkHandle handle);

    [[nodiscard]] bool TransferReplica(
        ChunkHandle handle,
        ServerId source_server_id,
        ServerId destination_server_id) const;

private:
    [[nodiscard]] std::uint32_t
    GetDesiredReplicationFactor(
        ChunkHandle handle) const;

    [[nodiscard]] bool IsHealthyCurrentReplica(
        ChunkHandle handle,
        ServerId server_id,
        std::uint64_t now_ms) const;

    [[nodiscard]] std::vector<ServerId>
    GetHealthyCurrentReplicas(
        ChunkHandle handle,
        std::uint64_t now_ms) const;

    [[nodiscard]] chunkserver::Chunkserver*
    GetChunkserver(
        ServerId server_id) const;

    Master& master_;

    mutable std::shared_mutex mutex_;

    std::unordered_map<
        ServerId,
        chunkserver::Chunkserver*>
        chunkservers_;

    std::unordered_map<
        ServerId,
        std::string>
        endpoints_;
};

}  // namespace replication
}  // namespace gfs::master