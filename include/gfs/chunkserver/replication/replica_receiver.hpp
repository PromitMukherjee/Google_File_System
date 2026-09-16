#pragma once

#include "gfs/common/types.hpp"

#include <cstddef>
#include <cstdint>
#include <string>

namespace gfs::chunkserver {
class Chunkserver;
}

namespace gfs::chunkserver::replication {

class ReplicaReceiver {
public:
    explicit ReplicaReceiver(
        chunkserver::Chunkserver& chunkserver);

    ReplicaReceiver(const ReplicaReceiver&) = delete;
    ReplicaReceiver& operator=(const ReplicaReceiver&) = delete;

    [[nodiscard]] bool Receive(
        ChunkHandle handle,
        const std::string& data);

    [[nodiscard]] bool Receive(
        ChunkHandle handle,
        const char* data,
        std::size_t size);

    [[nodiscard]] bool ReceiveFromOffset(
        ChunkHandle handle,
        std::uint64_t offset,
        const std::string& data);

    [[nodiscard]] bool HasReplica(
        ChunkHandle handle) const;

    [[nodiscard]] std::uint64_t GetReplicaSize(
        ChunkHandle handle) const;

    [[nodiscard]] chunkserver::Chunkserver&
    GetChunkserver() noexcept;

private:
    chunkserver::Chunkserver& chunkserver_;
};

}  // namespace gfs::chunkserver::replication