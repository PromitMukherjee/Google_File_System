#pragma once

#include "gfs/chunkserver/replication/replica_receiver.hpp"
#include "gfs/common/types.hpp"

#include <cstddef>
#include <cstdint>
#include <functional>
#include <string>

namespace gfs::chunkserver {
class Chunkserver;
}

namespace gfs::chunkserver::replication {

class ReplicaSender {
public:
    using TransferFunction = std::function<bool(
        ServerId,
        ChunkHandle,
        const std::string&)>;

    explicit ReplicaSender(
        chunkserver::Chunkserver& chunkserver,
        TransferFunction transfer_function = {});

    ReplicaSender(const ReplicaSender&) = delete;
    ReplicaSender& operator=(const ReplicaSender&) = delete;

    void SetTransferFunction(
        TransferFunction transfer_function);

    [[nodiscard]] bool Send(
        ChunkHandle handle,
        ServerId target_server);

    [[nodiscard]] bool Send(
        ChunkHandle handle,
        ServerId target_server,
        const TransferFunction& transfer_function) const;

    [[nodiscard]] bool ReadLocalChunk(
        ChunkHandle handle,
        std::string& data) const;

    [[nodiscard]] chunkserver::Chunkserver&
    GetChunkserver() noexcept;

private:
    chunkserver::Chunkserver& chunkserver_;
    TransferFunction transfer_function_;
};

}  // namespace gfs::chunkserver::replication