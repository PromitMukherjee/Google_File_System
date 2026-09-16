#pragma once

#include "gfs/chunkserver/replication/clone_manager.hpp"
#include "gfs/chunkserver/replication/replica_receiver.hpp"
#include "gfs/chunkserver/replication/replica_sender.hpp"
#include "gfs/chunkserver/storage/storage_manager.hpp"
#include "gfs/common/types.hpp"

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

namespace gfs::chunkserver {

class Chunkserver {
public:
    Chunkserver(
        ServerId server_id,
        std::string storage_directory);

    Chunkserver(const Chunkserver&) = delete;
    Chunkserver& operator=(const Chunkserver&) = delete;

    [[nodiscard]] bool Initialize();

    [[nodiscard]] ServerId GetServerId() const noexcept;

    [[nodiscard]] const std::string&
    GetStorageDirectory() const noexcept;

    [[nodiscard]] bool CreateChunk(
        ChunkHandle handle);

    [[nodiscard]] bool OpenChunk(
        ChunkHandle handle);

    [[nodiscard]] bool DeleteChunk(
        ChunkHandle handle);

    [[nodiscard]] bool ChunkExists(
        ChunkHandle handle) const;

    [[nodiscard]] bool ReadChunk(
        ChunkHandle handle,
        std::uint64_t offset,
        std::size_t length,
        std::vector<std::uint8_t>& data) const;

    [[nodiscard]] bool ReadChunk(
        ChunkHandle handle,
        std::uint64_t offset,
        std::size_t length,
        std::string& data) const;

    [[nodiscard]] bool WriteChunk(
        ChunkHandle handle,
        std::uint64_t offset,
        const std::vector<std::uint8_t>& data);

    [[nodiscard]] bool WriteChunk(
        ChunkHandle handle,
        std::uint64_t offset,
        const std::string& data);

    [[nodiscard]] bool TruncateChunk(
        ChunkHandle handle,
        std::uint64_t size);

    [[nodiscard]] std::uint64_t GetChunkSize(
        ChunkHandle handle) const;

    [[nodiscard]] std::vector<ChunkHandle>
    ListChunks() const;

    [[nodiscard]] std::size_t ChunkCount() const;

    [[nodiscard]] storage::StorageManager&
    GetStorageManager() noexcept;

    [[nodiscard]] const storage::StorageManager&
    GetStorageManager() const noexcept;

    [[nodiscard]] replication::ReplicaSender&
    GetReplicaSender() noexcept;

    [[nodiscard]] replication::ReplicaReceiver&
    GetReplicaReceiver() noexcept;

    [[nodiscard]] replication::CloneManager&
    GetCloneManager() noexcept;

private:
    ServerId server_id_;
    storage::StorageManager storage_manager_;

    std::unique_ptr<replication::ReplicaSender>
        replica_sender_;

    std::unique_ptr<replication::ReplicaReceiver>
        replica_receiver_;

    std::unique_ptr<replication::CloneManager>
        clone_manager_;

    bool initialized_ = false;
};

}  // namespace gfs::chunkserver