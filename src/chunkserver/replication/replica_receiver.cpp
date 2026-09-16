#include "gfs/chunkserver/replication/replica_receiver.hpp"

#include "gfs/chunkserver/chunkserver.hpp"
#include "gfs/common/constants.hpp"

#include <vector>

namespace gfs::chunkserver::replication {

ReplicaReceiver::ReplicaReceiver(
    chunkserver::Chunkserver& chunkserver)
    : chunkserver_(chunkserver) {}

bool ReplicaReceiver::Receive(
    ChunkHandle handle,
    const std::string& data) {
    if (handle == 0 ||
        data.size() > gfs::constants::kChunkSize) {
        return false;
    }

    const std::vector<std::uint8_t> buffer(
        reinterpret_cast<const std::uint8_t*>(data.data()),
        reinterpret_cast<const std::uint8_t*>(data.data()) +
            data.size());

    if (chunkserver_.CreateChunk(handle)) {
        return chunkserver_.WriteChunk(
            handle,
            0,
            buffer);
    }

    if (!chunkserver_.ChunkExists(handle)) {
        return false;
    }

    if (!chunkserver_.TruncateChunk(handle, 0)) {
        return false;
    }

    return chunkserver_.WriteChunk(
        handle,
        0,
        buffer);
}

bool ReplicaReceiver::Receive(
    ChunkHandle handle,
    const char* data,
    std::size_t size) {
    if (size != 0 && data == nullptr) {
        return false;
    }

    return Receive(
        handle,
        std::string(
            data == nullptr ? "" : data,
            size));
}

bool ReplicaReceiver::ReceiveFromOffset(
    ChunkHandle handle,
    std::uint64_t offset,
    const std::string& data) {
    if (handle == 0 ||
        data.empty() ||
        offset + data.size() >
            gfs::constants::kChunkSize) {
        return false;
    }

    if (!chunkserver_.ChunkExists(handle) &&
        !chunkserver_.CreateChunk(handle)) {
        return false;
    }

    const std::vector<std::uint8_t> buffer(
        reinterpret_cast<const std::uint8_t*>(data.data()),
        reinterpret_cast<const std::uint8_t*>(data.data()) +
            data.size());

    return chunkserver_.WriteChunk(
        handle,
        offset,
        buffer);
}

bool ReplicaReceiver::HasReplica(
    ChunkHandle handle) const {
    if (handle == 0) {
        return false;
    }

    return chunkserver_.ChunkExists(handle);
}

std::uint64_t ReplicaReceiver::GetReplicaSize(
    ChunkHandle handle) const {
    if (handle == 0) {
        return 0;
    }

    return chunkserver_.GetChunkSize(handle);
}

chunkserver::Chunkserver&
ReplicaReceiver::GetChunkserver() noexcept {
    return chunkserver_;
}

}  // namespace gfs::chunkserver::replication