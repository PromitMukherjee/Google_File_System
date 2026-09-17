#include "gfs/master/recovery/recovery_manager.hpp"

#include "gfs/master/replication/re_replication.hpp"

namespace gfs::master::recovery {

RecoveryManager::RecoveryManager(
    replication::ReReplicationManager& re_replication)
    : re_replication_(re_replication) {
}

bool RecoveryManager::RegisterChunkserver(
    chunkserver::Chunkserver& chunkserver) {
    return re_replication_.RegisterChunkserver(
        chunkserver);
}

bool RecoveryManager::UnregisterChunkserver(
    ServerId server_id) {
    return re_replication_.UnregisterChunkserver(
        server_id);
}

std::size_t RecoveryManager::GetHealthyReplicaCount(
    ChunkHandle handle,
    std::uint64_t now_ms) const {
    return re_replication_.GetHealthyReplicaCount(
        handle,
        now_ms);
}

bool RecoveryManager::NeedsRecovery(
    ChunkHandle handle,
    std::uint64_t now_ms) const {
    return re_replication_.NeedsReReplication(
        handle,
        now_ms);
}

std::optional<ServerId>
RecoveryManager::FindRecoverySource(
    ChunkHandle handle,
    std::uint64_t now_ms) const {
    return re_replication_.FindRecoverySource(
        handle,
        now_ms);
}

bool RecoveryManager::RecoverChunk(
    ChunkHandle handle,
    std::uint64_t now_ms) {
    return re_replication_.RecoverChunk(
        handle,
        now_ms);
}

std::vector<ChunkHandle>
RecoveryManager::RecoverFailedChunkservers(
    std::uint64_t now_ms) {
    return re_replication_.RecoverFailedChunkservers(
        now_ms);
}

std::vector<ChunkHandle>
RecoveryManager::RecoverAll(
    std::uint64_t now_ms) {
    return re_replication_.RecoverAll(now_ms);
}

}  // namespace gfs::master::recovery