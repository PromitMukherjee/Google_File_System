#include "gfs/chunkserver/storage/chunk_storage.hpp"

#include "gfs/chunkserver/storage/chunk_file.hpp"

#include <algorithm>
#include <filesystem>
#include <limits>
#include <mutex>
#include <system_error>
#include <utility>

namespace gfs::chunkserver::storage {

ChunkStorage::ChunkStorage(std::string storage_directory)
    : storage_directory_(std::move(storage_directory)) {
}

ChunkStorage::~ChunkStorage() = default;

bool ChunkStorage::Initialize() {
    std::error_code error;

    if (storage_directory_.empty()) {
        return false;
    }

    std::filesystem::create_directories(
        storage_directory_,
        error);

    if (error) {
        return false;
    }

    const std::filesystem::path directory(storage_directory_);

    for (const auto& entry :
         std::filesystem::directory_iterator(directory, error)) {

        if (error) {
            return false;
        }

        if (!entry.is_regular_file(error) || error) {
            continue;
        }

        const std::string filename =
            entry.path().filename().string();

        constexpr const char* prefix = "chunk_";

        if (filename.rfind(prefix, 0) != 0) {
            continue;
        }

        const std::string handle_text =
            filename.substr(6);

        if (handle_text.empty()) {
            continue;
        }

        try {
            const unsigned long long value =
                std::stoull(handle_text);

            if (value == 0 ||
                value >
                    std::numeric_limits<ChunkHandle>::max()) {
                continue;
            }

            const ChunkHandle handle =
                static_cast<ChunkHandle>(value);

            auto chunk = std::make_shared<ChunkFile>(
                handle,
                entry.path().string());

            if (chunk->Open()) {
                std::unique_lock lock(mutex_);
                chunks_[handle] = std::move(chunk);
            }
        } catch (...) {
            continue;
        }
    }

    return true;
}

bool ChunkStorage::CreateChunk(ChunkHandle handle) {
    if (handle == 0) {
        return false;
    }

    {
        std::shared_lock lock(mutex_);

        if (chunks_.contains(handle)) {
            return false;
        }
    }

    const std::string path = GetChunkPath(handle);

    auto chunk = std::make_shared<ChunkFile>(
        handle,
        path);

    if (!chunk->Create()) {
        return false;
    }

    std::unique_lock lock(mutex_);

    if (chunks_.contains(handle)) {
        chunk->Close();
        return false;
    }

    chunks_.emplace(handle, std::move(chunk));
    return true;
}

bool ChunkStorage::OpenChunk(ChunkHandle handle) {
    if (handle == 0) {
        return false;
    }

    {
        std::shared_lock lock(mutex_);

        if (chunks_.contains(handle)) {
            return true;
        }
    }

    const std::string path = GetChunkPath(handle);

    if (!std::filesystem::exists(path)) {
        return false;
    }

    auto chunk = std::make_shared<ChunkFile>(
        handle,
        path);

    if (!chunk->Open()) {
        return false;
    }

    std::unique_lock lock(mutex_);

    if (chunks_.contains(handle)) {
        return true;
    }

    chunks_.emplace(handle, std::move(chunk));
    return true;
}

bool ChunkStorage::DeleteChunk(ChunkHandle handle) {
    std::shared_ptr<ChunkFile> chunk;

    {
        std::unique_lock lock(mutex_);

        const auto it = chunks_.find(handle);

        if (it == chunks_.end()) {
            const std::string path = GetChunkPath(handle);
            std::error_code error;

            const bool removed =
                std::filesystem::remove(path, error);

            return removed && !error;
        }

        chunk = std::move(it->second);
        chunks_.erase(it);
    }

    return chunk->Delete();
}

bool ChunkStorage::ChunkExists(ChunkHandle handle) const {
    {
        std::shared_lock lock(mutex_);

        if (chunks_.contains(handle)) {
            return true;
        }
    }

    return std::filesystem::exists(GetChunkPath(handle));
}

std::shared_ptr<ChunkFile>
ChunkStorage::GetChunk(ChunkHandle handle) {
    std::shared_lock lock(mutex_);

    const auto it = chunks_.find(handle);

    if (it == chunks_.end()) {
        return nullptr;
    }

    return it->second;
}

std::shared_ptr<const ChunkFile>
ChunkStorage::GetChunk(ChunkHandle handle) const {
    std::shared_lock lock(mutex_);

    const auto it = chunks_.find(handle);

    if (it == chunks_.end()) {
        return nullptr;
    }

    return it->second;
}

bool ChunkStorage::ReadChunk(
    ChunkHandle handle,
    std::uint64_t offset,
    std::size_t length,
    std::vector<std::uint8_t>& data) {

    auto chunk = GetOrOpenChunk(handle);

    if (!chunk) {
        return false;
    }

    return chunk->Read(offset, length, data);
}

bool ChunkStorage::WriteChunk(
    ChunkHandle handle,
    std::uint64_t offset,
    const std::vector<std::uint8_t>& data) {

    auto chunk = GetChunk(handle);

    if (!chunk) {
        return false;
    }

    return chunk->Write(offset, data);
}

std::uint64_t ChunkStorage::GetChunkSize(
    ChunkHandle handle) const {

    auto chunk = GetChunk(handle);

    if (!chunk) {
        return 0;
    }

    return chunk->GetSize();
}

std::string ChunkStorage::GetChunkPath(
    ChunkHandle handle) const {

    return (
        std::filesystem::path(storage_directory_) /
        ("chunk_" + std::to_string(handle))
    ).string();
}

std::vector<ChunkHandle> ChunkStorage::ListChunks() const {
    std::shared_lock lock(mutex_);

    std::vector<ChunkHandle> handles;
    handles.reserve(chunks_.size());

    for (const auto& [handle, chunk] : chunks_) {
        handles.push_back(handle);
    }

    std::sort(handles.begin(), handles.end());

    return handles;
}

const std::string&
ChunkStorage::GetStorageDirectory() const noexcept {
    return storage_directory_;
}

std::size_t ChunkStorage::ChunkCount() const {
    std::shared_lock lock(mutex_);
    return chunks_.size();
}

std::shared_ptr<ChunkFile>
ChunkStorage::GetOrOpenChunk(ChunkHandle handle) {
    auto chunk = GetChunk(handle);

    if (chunk) {
        return chunk;
    }

    if (!OpenChunk(handle)) {
        return nullptr;
    }

    return GetChunk(handle);
}

}  // namespace gfs::chunkserver::storage