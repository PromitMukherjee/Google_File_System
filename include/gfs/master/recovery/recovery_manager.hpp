#pragma once

#include "gfs/common/types.hpp"

#include <cstddef>
#include <cstdint>
#include <optional>
#include <vector>

namespace gfs::chunkserver {
class Chunkserver;
}

namespace gfs::master {

namespace replication {
class ReReplicationManager;
}

namespace recovery {

class RecoveryManager {
public:
    explicit RecoveryManager(
        replication::ReReplicationManager& re_replication);

    RecoveryManager(const RecoveryManager&) = delete;
    RecoveryManager& operator=(const RecoveryManager&) = delete;

    [[nodiscard]] bool RegisterChunkserver(
        chunkserver::Chunkserver& chunkserver);

    [[nodiscard]] bool UnregisterChunkserver(
        ServerId server_id);

    [[nodiscard]] std::size_t
    GetHealthyReplicaCount(
        ChunkHandle handle,
        std::uint64_t now_ms) const;

    [[nodiscard]] bool NeedsRecovery(
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

private:
    replication::ReReplicationManager& re_replication_;
};

}  // namespace recovery
}  // namespace gfs::master