#include "gfs/chunkserver/chunkserver.hpp"

#include <cstdint>
#include <mutex>
#include <utility>
#include <vector>

namespace gfs::chunkserver {

Chunkserver::Chunkserver(
    ServerId server_id,
    std::string storage_directory)
    : server_id_(server_id),
      storage_manager_(std::move(storage_directory)),
      checksum_manager_(storage_manager_),
      mutation_manager_(*this) {
}

bool Chunkserver::Initialize() {
    if (server_id_ == 0) {
        return false;
    }

    if (!storage_manager_.Initialize()) {
        return false;
    }

    if (!checksum_manager_.Initialize()) {
        return false;
    }

    replica_receiver_ =
        std::make_unique<
            replication::ReplicaReceiver>(*this);

    clone_manager_ =
        std::make_unique<
            replication::CloneManager>(*this);

    replica_sender_ =
        std::make_unique<
            replication::ReplicaSender>(*this);

    initialized_ = true;
    return true;
}

ServerId Chunkserver::GetServerId() const noexcept {
    return server_id_;
}

const std::string&
Chunkserver::GetStorageDirectory() const noexcept {
    return storage_manager_.GetStorageDirectory();
}

bool Chunkserver::CreateChunk(
    ChunkHandle handle) {
    if (!initialized_ || handle == 0) {
        return false;
    }

    std::unique_lock lock(io_mutex_);

    return storage_manager_.CreateChunk(handle);
}

bool Chunkserver::OpenChunk(
    ChunkHandle handle) {
    if (!initialized_ || handle == 0) {
        return false;
    }

    std::unique_lock lock(io_mutex_);

    if (!storage_manager_.OpenChunk(handle)) {
        return false;
    }

    if (!checksum_manager_.HasChecksums(handle) &&
        storage_manager_.GetChunkSize(handle) != 0) {
        return checksum_manager_.ComputeChunkChecksums(handle);
    }

    return true;
}

bool Chunkserver::DeleteChunk(
    ChunkHandle handle) {
    if (!initialized_ || handle == 0) {
        return false;
    }

    std::unique_lock lock(io_mutex_);

    mutation_manager_.ResetChunk(handle);

    if (!storage_manager_.DeleteChunk(handle)) {
        return false;
    }

    return checksum_manager_.DeleteChecksums(handle);
}

bool Chunkserver::ChunkExists(
    ChunkHandle handle) const {
    if (!initialized_ || handle == 0) {
        return false;
    }

    return storage_manager_.ChunkExists(handle);
}

bool Chunkserver::ReadChunk(
    ChunkHandle handle,
    std::uint64_t offset,
    std::size_t length,
    std::vector<std::uint8_t>& data) const {
    if (!initialized_ || handle == 0) {
        data.clear();
        return false;
    }

    std::shared_lock lock(io_mutex_);

    if (!checksum_manager_.VerifyChunkRange(
            handle,
            offset,
            length)) {
        data.clear();
        return false;
    }

    auto& storage_manager =
        const_cast<storage::StorageManager&>(
            storage_manager_);

    return storage_manager.ReadChunk(
        handle,
        offset,
        length,
        data);
}

bool Chunkserver::ReadChunk(
    ChunkHandle handle,
    std::uint64_t offset,
    std::size_t length,
    std::string& data) const {
    std::vector<std::uint8_t> buffer;

    if (!ReadChunk(
            handle,
            offset,
            length,
            buffer)) {
        data.clear();
        return false;
    }

    data.assign(
        reinterpret_cast<const char*>(
            buffer.data()),
        buffer.size());

    return true;
}

bool Chunkserver::WriteChunk(
    ChunkHandle handle,
    std::uint64_t offset,
    const std::vector<std::uint8_t>& data) {
    if (!initialized_ || handle == 0) {
        return false;
    }

    std::unique_lock lock(io_mutex_);

    if (!storage_manager_.WriteChunk(
            handle,
            offset,
            data)) {
        return false;
    }

    if (data.empty()) {
        return true;
    }

    return checksum_manager_.UpdateAfterWrite(
        handle,
        offset,
        data.size());
}

bool Chunkserver::WriteChunk(
    ChunkHandle handle,
    std::uint64_t offset,
    const std::string& data) {
    const std::vector<std::uint8_t> buffer(
        reinterpret_cast<const std::uint8_t*>(
            data.data()),
        reinterpret_cast<const std::uint8_t*>(
            data.data()) +
            data.size());

    return WriteChunk(
        handle,
        offset,
        buffer);
}

bool Chunkserver::TruncateChunk(
    ChunkHandle handle,
    std::uint64_t size) {
    if (!initialized_ || handle == 0) {
        return false;
    }

    std::unique_lock lock(io_mutex_);

    const auto chunk =
        storage_manager_.GetChunk(handle);

    if (!chunk) {
        return false;
    }

    if (!chunk->Truncate(size)) {
        return false;
    }

    return checksum_manager_.UpdateAfterTruncate(
        handle,
        size);
}

std::uint64_t Chunkserver::GetChunkSize(
    ChunkHandle handle) const {
    if (!initialized_ || handle == 0) {
        return 0;
    }

    return storage_manager_.GetChunkSize(handle);
}

std::vector<ChunkHandle>
Chunkserver::ListChunks() const {
    if (!initialized_) {
        return {};
    }

    return storage_manager_.ListChunks();
}

std::size_t Chunkserver::ChunkCount() const {
    if (!initialized_) {
        return 0;
    }

    return storage_manager_.ChunkCount();
}

storage::StorageManager&
Chunkserver::GetStorageManager() noexcept {
    return storage_manager_;
}

const storage::StorageManager&
Chunkserver::GetStorageManager() const noexcept {
    return storage_manager_;
}

checksum::ChecksumManager&
Chunkserver::GetChecksumManager() noexcept {
    return checksum_manager_;
}

const checksum::ChecksumManager&
Chunkserver::GetChecksumManager() const noexcept {
    return checksum_manager_;
}

replication::ReplicaSender&
Chunkserver::GetReplicaSender() noexcept {
    return *replica_sender_;
}

replication::ReplicaReceiver&
Chunkserver::GetReplicaReceiver() noexcept {
    return *replica_receiver_;
}

replication::CloneManager&
Chunkserver::GetCloneManager() noexcept {
    return *clone_manager_;
}

mutation::MutationManager&
Chunkserver::GetMutationManager() noexcept {
    return mutation_manager_;
}

const mutation::MutationManager&
Chunkserver::GetMutationManager() const noexcept {
    return mutation_manager_;
}

}  // namespace gfs::chunkserver