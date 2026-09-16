#include "gfs/chunkserver/chunkserver.hpp"

#include <utility>

namespace gfs::chunkserver {

Chunkserver::Chunkserver(
    ServerId server_id,
    std::string storage_directory)
    : server_id_(server_id),
      storage_manager_(std::move(storage_directory)) {
}

Chunkserver::~Chunkserver() = default;

bool Chunkserver::Initialize() {
    return storage_manager_.Initialize();
}

ServerId Chunkserver::GetServerId() const noexcept {
    return server_id_;
}

const std::string&
Chunkserver::GetStorageDirectory() const noexcept {
    return storage_manager_.GetStorageDirectory();
}

bool Chunkserver::CreateChunk(ChunkHandle handle) {
    return storage_manager_.CreateChunk(handle);
}

bool Chunkserver::OpenChunk(ChunkHandle handle) {
    return storage_manager_.OpenChunk(handle);
}

bool Chunkserver::DeleteChunk(ChunkHandle handle) {
    return storage_manager_.DeleteChunk(handle);
}

bool Chunkserver::ChunkExists(
    ChunkHandle handle) const {
    return storage_manager_.ChunkExists(handle);
}

bool Chunkserver::ReadChunk(
    ChunkHandle handle,
    std::uint64_t offset,
    std::size_t length,
    std::vector<std::uint8_t>& data) {

    return storage_manager_.ReadChunk(
        handle,
        offset,
        length,
        data);
}

bool Chunkserver::WriteChunk(
    ChunkHandle handle,
    std::uint64_t offset,
    const std::vector<std::uint8_t>& data) {

    return storage_manager_.WriteChunk(
        handle,
        offset,
        data);
}

std::uint64_t Chunkserver::GetChunkSize(
    ChunkHandle handle) const {

    return storage_manager_.GetChunkSize(handle);
}

std::vector<ChunkHandle>
Chunkserver::ListChunks() const {
    return storage_manager_.ListChunks();
}

std::size_t Chunkserver::ChunkCount() const {
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

}  // namespace gfs::chunkserver