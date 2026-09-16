#include "gfs/chunkserver/replication/replica_sender.hpp"

#include "gfs/chunkserver/chunkserver.hpp"
#include "gfs/common/constants.hpp"

#include <cstdint>
#include <string>
#include <utility>
#include <vector>

namespace gfs::chunkserver::replication {

ReplicaSender::ReplicaSender(
    chunkserver::Chunkserver& chunkserver,
    TransferFunction transfer_function)
    : chunkserver_(chunkserver),
      transfer_function_(std::move(transfer_function)) {}

void ReplicaSender::SetTransferFunction(
    TransferFunction transfer_function) {
    transfer_function_ = std::move(transfer_function);
}

bool ReplicaSender::Send(
    ChunkHandle handle,
    ServerId target_server) {
    return Send(
        handle,
        target_server,
        transfer_function_);
}

bool ReplicaSender::Send(
    ChunkHandle handle,
    ServerId target_server,
    const TransferFunction& transfer_function) const {
    if (handle == 0 ||
        target_server == 0 ||
        !transfer_function) {
        return false;
    }

    std::string data;

    if (!ReadLocalChunk(handle, data)) {
        return false;
    }

    if (data.size() > gfs::constants::kChunkSize) {
        return false;
    }

    return transfer_function(
        target_server,
        handle,
        data);
}

bool ReplicaSender::ReadLocalChunk(
    ChunkHandle handle,
    std::string& data) const {
    data.clear();

    if (handle == 0 ||
        !chunkserver_.ChunkExists(handle)) {
        return false;
    }

    const std::uint64_t size =
        chunkserver_.GetChunkSize(handle);

    if (size > gfs::constants::kChunkSize) {
        return false;
    }

    std::vector<std::uint8_t> buffer;

    if (!chunkserver_.ReadChunk(
            handle,
            0,
            static_cast<std::size_t>(size),
            buffer)) {
        data.clear();
        return false;
    }

    data.assign(
        reinterpret_cast<const char*>(buffer.data()),
        buffer.size());

    return true;
}

chunkserver::Chunkserver&
ReplicaSender::GetChunkserver() noexcept {
    return chunkserver_;
}

}  // namespace gfs::chunkserver::replication