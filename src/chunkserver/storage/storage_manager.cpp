#include "gfs/chunkserver/storage/storage_manager.hpp"

#include <utility>

namespace gfs::chunkserver::storage {

StorageManager::StorageManager(
    std::string storage_directory)
    : storage_(std::move(storage_directory)) {
}

StorageManager::~StorageManager() = default;

bool StorageManager::Initialize() {
    return storage_.Initialize();
}

bool StorageManager::CreateChunk(ChunkHandle handle) {
    return storage_.CreateChunk(handle);
}

bool StorageManager::OpenChunk(ChunkHandle handle) {
    return storage_.OpenChunk(handle);
}

bool StorageManager::DeleteChunk(ChunkHandle handle) {
    return storage_.DeleteChunk(handle);
}

bool StorageManager::ChunkExists(ChunkHandle handle) const {
    return storage_.ChunkExists(handle);
}

std::shared_ptr<ChunkFile>
StorageManager::GetChunk(ChunkHandle handle) {
    return storage_.GetChunk(handle);
}

std::shared_ptr<const ChunkFile>
StorageManager::GetChunk(ChunkHandle handle) const {
    return storage_.GetChunk(handle);
}

bool StorageManager::ReadChunk(
    ChunkHandle handle,
    std::uint64_t offset,
    std::size_t length,
    std::vector<std::uint8_t>& data) {

    return storage_.ReadChunk(
        handle,
        offset,
        length,
        data);
}

bool StorageManager::WriteChunk(
    ChunkHandle handle,
    std::uint64_t offset,
    const std::vector<std::uint8_t>& data) {

    return storage_.WriteChunk(
        handle,
        offset,
        data);
}

std::uint64_t StorageManager::GetChunkSize(
    ChunkHandle handle) const {

    return storage_.GetChunkSize(handle);
}

std::vector<ChunkHandle>
StorageManager::ListChunks() const {
    return storage_.ListChunks();
}

const std::string&
StorageManager::GetStorageDirectory() const noexcept {
    return storage_.GetStorageDirectory();
}

std::size_t StorageManager::ChunkCount() const {
    return storage_.ChunkCount();
}

}  // namespace gfs::chunkserver::storage