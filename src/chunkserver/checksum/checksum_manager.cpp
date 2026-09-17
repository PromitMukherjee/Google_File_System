#include "gfs/chunkserver/checksum/checksum_manager.hpp"

#include "gfs/common/constants.hpp"

#include <algorithm>
#include <filesystem>
#include <fstream>
#include <limits>
#include <mutex>
#include <string>
#include <utility>

namespace gfs::chunkserver::checksum {

namespace {

constexpr std::uint32_t kCRC32Polynomial =
    0xEDB88320U;

constexpr std::size_t kChecksumValueSize =
    sizeof(std::uint32_t);

}  // namespace

ChecksumManager::ChecksumManager(
    storage::StorageManager& storage_manager)
    : storage_manager_(storage_manager) {
}

bool ChecksumManager::Initialize() {
    const auto handles =
        storage_manager_.ListChunks();

    std::unique_lock lock(mutex_);

    checksums_.clear();

    for (const ChunkHandle handle : handles) {
        if (handle == 0) {
            continue;
        }

        const std::uint64_t size =
            storage_manager_.GetChunkSize(handle);

        const std::uint64_t expected_blocks =
            ExpectedBlockCountLocked(size);

        const std::string path =
            GetChecksumPath(handle);

        bool loaded = false;

        if (std::filesystem::exists(path)) {
            loaded = LoadChecksumsLocked(handle);

            if (loaded) {
                const auto it =
                    checksums_.find(handle);

                if (it == checksums_.end() ||
                    it->second.size() != expected_blocks) {
                    loaded = false;
                }
            }
        }

        if (expected_blocks == 0) {
            checksums_.erase(handle);

            if (std::filesystem::exists(path)) {
                std::error_code error;
                std::filesystem::remove(
                    path,
                    error);

                if (error) {
                    return false;
                }
            }

            continue;
        }

        if (!loaded) {
            if (!RecomputeBlocksLocked(
                    handle,
                    0,
                    expected_blocks - 1)) {
                return false;
            }

            if (!PersistChecksumsLocked(handle)) {
                return false;
            }
        }
    }

    return true;
}

std::uint32_t ChecksumManager::ComputeCRC32(
    const std::vector<std::uint8_t>& data) {
    std::uint32_t crc = 0xFFFFFFFFU;

    for (const std::uint8_t byte : data) {
        crc ^= static_cast<std::uint32_t>(byte);

        for (int bit = 0; bit < 8; ++bit) {
            if ((crc & 1U) != 0U) {
                crc =
                    (crc >> 1U) ^
                    kCRC32Polynomial;
            } else {
                crc >>= 1U;
            }
        }
    }

    return crc ^ 0xFFFFFFFFU;
}

bool ChecksumManager::ComputeChunkChecksums(
    ChunkHandle handle) {
    if (handle == 0 ||
        !storage_manager_.ChunkExists(handle)) {
        return false;
    }

    std::unique_lock lock(mutex_);

    const std::uint64_t size =
        storage_manager_.GetChunkSize(handle);

    checksums_.erase(handle);

    if (size == 0) {
        const std::string path =
            GetChecksumPath(handle);

        if (std::filesystem::exists(path)) {
            std::error_code error;
            std::filesystem::remove(
                path,
                error);

            return !error;
        }

        return true;
    }

    const std::uint64_t block_count =
        ExpectedBlockCountLocked(size);

    if (!RecomputeBlocksLocked(
            handle,
            0,
            block_count - 1)) {
        return false;
    }

    return PersistChecksumsLocked(handle);
}

bool ChecksumManager::UpdateAfterWrite(
    ChunkHandle handle,
    std::uint64_t offset,
    std::size_t length) {
    if (handle == 0 ||
        length == 0 ||
        !storage_manager_.ChunkExists(handle)) {
        return length == 0 &&
               handle != 0 &&
               storage_manager_.ChunkExists(handle);
    }

    const std::uint64_t end =
        offset + static_cast<std::uint64_t>(length);

    if (end < offset) {
        return false;
    }

    std::unique_lock lock(mutex_);

    const std::uint64_t size =
        storage_manager_.GetChunkSize(handle);

    if (offset > size ||
        end > size) {
        return false;
    }

    const std::uint64_t first_block =
        offset /
        constants::kChecksumBlockSize;

    const std::uint64_t last_block =
        (end - 1) /
        constants::kChecksumBlockSize;

    if (!RecomputeBlocksLocked(
            handle,
            first_block,
            last_block)) {
        return false;
    }

    return PersistChecksumsLocked(handle);
}

bool ChecksumManager::UpdateAfterTruncate(
    ChunkHandle handle,
    std::uint64_t size) {
    if (handle == 0 ||
        !storage_manager_.ChunkExists(handle)) {
        return false;
    }

    std::unique_lock lock(mutex_);

    const std::uint64_t actual_size =
        storage_manager_.GetChunkSize(handle);

    if (actual_size != size) {
        return false;
    }

    const std::uint64_t block_count =
        ExpectedBlockCountLocked(size);

    if (block_count == 0) {
        checksums_.erase(handle);

        const std::string path =
            GetChecksumPath(handle);

        if (std::filesystem::exists(path)) {
            std::error_code error;
            std::filesystem::remove(
                path,
                error);

            return !error;
        }

        return true;
    }

    checksums_.erase(handle);

    if (!RecomputeBlocksLocked(
            handle,
            0,
            block_count - 1)) {
        return false;
    }

    return PersistChecksumsLocked(handle);
}

bool ChecksumManager::VerifyChunk(
    ChunkHandle handle) {
    if (handle == 0 ||
        !storage_manager_.ChunkExists(handle)) {
        return false;
    }

    const std::uint64_t size =
        storage_manager_.GetChunkSize(handle);

    if (size == 0) {
        return true;
    }

    return VerifyChunkRange(
        handle,
        0,
        static_cast<std::size_t>(size));
}

bool ChecksumManager::VerifyChunkRange(
    ChunkHandle handle,
    std::uint64_t offset,
    std::size_t length) const {
    if (handle == 0 ||
        !storage_manager_.ChunkExists(handle)) {
        return false;
    }

    if (length == 0) {
        return true;
    }

    const std::uint64_t size =
        storage_manager_.GetChunkSize(handle);

    const std::uint64_t end =
        offset + static_cast<std::uint64_t>(length);

    if (end < offset ||
        offset > size ||
        end > size) {
        return false;
    }

    const std::uint64_t first_block =
        offset /
        constants::kChecksumBlockSize;

    const std::uint64_t last_block =
        (end - 1) /
        constants::kChecksumBlockSize;

    std::shared_lock lock(mutex_);

    const auto it =
        checksums_.find(handle);

    if (it == checksums_.end()) {
        return false;
    }

    const auto& blocks = it->second;

    if (last_block >= blocks.size()) {
        return false;
    }

    for (std::uint64_t block_index = first_block;
         block_index <= last_block;
         ++block_index) {
        std::vector<std::uint8_t> data;

        if (!ReadBlockLocked(
                handle,
                block_index,
                data)) {
            return false;
        }

        const std::uint32_t calculated =
            ComputeCRC32(data);

        if (blocks[
                static_cast<std::size_t>(
                    block_index)]
                .GetChecksum() != calculated) {
            return false;
        }
    }

    return true;
}

std::optional<std::uint32_t>
ChecksumManager::GetChecksum(
    ChunkHandle handle,
    std::uint64_t block_index) const {
    if (handle == 0) {
        return std::nullopt;
    }

    std::shared_lock lock(mutex_);

    const auto it =
        checksums_.find(handle);

    if (it == checksums_.end() ||
        block_index >= it->second.size()) {
        return std::nullopt;
    }

    return it->second[
        static_cast<std::size_t>(block_index)]
        .GetChecksum();
}

std::vector<ChecksumBlock>
ChecksumManager::GetChecksums(
    ChunkHandle handle) const {
    if (handle == 0) {
        return {};
    }

    std::shared_lock lock(mutex_);

    const auto it =
        checksums_.find(handle);

    if (it == checksums_.end()) {
        return {};
    }

    return it->second;
}

std::size_t ChecksumManager::ChecksumBlockCount(
    ChunkHandle handle) const {
    if (handle == 0) {
        return 0;
    }

    std::shared_lock lock(mutex_);

    const auto it =
        checksums_.find(handle);

    if (it == checksums_.end()) {
        return 0;
    }

    return it->second.size();
}

bool ChecksumManager::HasChecksums(
    ChunkHandle handle) const {
    return ChecksumBlockCount(handle) != 0;
}

bool ChecksumManager::DeleteChecksums(
    ChunkHandle handle) {
    if (handle == 0) {
        return false;
    }

    std::unique_lock lock(mutex_);

    checksums_.erase(handle);

    const std::string path =
        GetChecksumPath(handle);

    if (!std::filesystem::exists(path)) {
        return true;
    }

    std::error_code error;

    const bool removed =
        std::filesystem::remove(
            path,
            error);

    return removed && !error;
}

std::string ChecksumManager::GetChecksumPath(
    ChunkHandle handle) const {
    return (
        std::filesystem::path(
            storage_manager_.GetStorageDirectory()) /
        ("checksum_" + std::to_string(handle))
    ).string();
}

bool ChecksumManager::RecomputeBlocksLocked(
    ChunkHandle handle,
    std::uint64_t first_block,
    std::uint64_t last_block) {
    if (handle == 0 ||
        first_block > last_block ||
        !storage_manager_.ChunkExists(handle)) {
        return false;
    }

    const std::uint64_t size =
        storage_manager_.GetChunkSize(handle);

    const std::uint64_t block_count =
        ExpectedBlockCountLocked(size);

    if (block_count == 0 ||
        first_block >= block_count ||
        last_block >= block_count) {
        return false;
    }

    auto& blocks = checksums_[handle];

    blocks.resize(
        static_cast<std::size_t>(block_count));

    for (std::uint64_t block_index = first_block;
         block_index <= last_block;
         ++block_index) {
        std::vector<std::uint8_t> data;

        if (!ReadBlockLocked(
                handle,
                block_index,
                data)) {
            return false;
        }

        blocks[
            static_cast<std::size_t>(block_index)] =
            ChecksumBlock(
                handle,
                block_index,
                ComputeCRC32(data));
    }

    return true;
}

bool ChecksumManager::LoadChecksumsLocked(
    ChunkHandle handle) {
    if (handle == 0) {
        return false;
    }

    const std::string path =
        GetChecksumPath(handle);

    std::ifstream input(
        path,
        std::ios::binary);

    if (!input) {
        return false;
    }

    input.seekg(
        0,
        std::ios::end);

    const std::streamoff file_size =
        input.tellg();

    if (file_size < 0 ||
        file_size %
            static_cast<std::streamoff>(
                kChecksumValueSize) != 0) {
        return false;
    }

    input.seekg(
        0,
        std::ios::beg);

    const std::size_t count =
        static_cast<std::size_t>(
            file_size /
            static_cast<std::streamoff>(
                kChecksumValueSize));

    std::vector<ChecksumBlock> blocks;
    blocks.reserve(count);

    for (std::size_t index = 0;
         index < count;
         ++index) {
        std::uint32_t checksum = 0;

        input.read(
            reinterpret_cast<char*>(&checksum),
            sizeof(checksum));

        if (!input) {
            return false;
        }

        blocks.emplace_back(
            handle,
            static_cast<std::uint64_t>(index),
            checksum);
    }

    checksums_[handle] =
        std::move(blocks);

    return true;
}

bool ChecksumManager::PersistChecksumsLocked(
    ChunkHandle handle) const {
    if (handle == 0) {
        return false;
    }

    const auto it =
        checksums_.find(handle);

    if (it == checksums_.end() ||
        it->second.empty()) {
        const std::string path =
            GetChecksumPath(handle);

        if (!std::filesystem::exists(path)) {
            return true;
        }

        std::error_code error;
        const bool removed =
            std::filesystem::remove(
                path,
                error);

        return removed && !error;
    }

    const std::string path =
        GetChecksumPath(handle);

    const std::string temporary_path =
        path + ".tmp";

    {
        std::ofstream output(
            temporary_path,
            std::ios::binary |
            std::ios::trunc);

        if (!output) {
            return false;
        }

        for (const auto& block : it->second) {
            const std::uint32_t checksum =
                block.GetChecksum();

            output.write(
                reinterpret_cast<const char*>(
                    &checksum),
                sizeof(checksum));

            if (!output) {
                return false;
            }
        }

        output.flush();

        if (!output) {
            return false;
        }
    }

    std::error_code error;

    std::filesystem::rename(
        temporary_path,
        path,
        error);

    if (!error) {
        return true;
    }

    std::filesystem::remove(
        temporary_path,
        error);

    return false;
}

bool ChecksumManager::ReadBlockLocked(
    ChunkHandle handle,
    std::uint64_t block_index,
    std::vector<std::uint8_t>& data) const {
    if (handle == 0) {
        return false;
    }

    const std::uint64_t size =
        storage_manager_.GetChunkSize(handle);

    const std::uint64_t block_start =
        block_index *
        constants::kChecksumBlockSize;

    if (block_start >= size) {
        data.clear();
        return false;
    }

    const std::uint64_t remaining =
        size - block_start;

    const std::uint64_t block_size =
        std::min<std::uint64_t>(
            remaining,
            constants::kChecksumBlockSize);

    if (block_size >
        static_cast<std::uint64_t>(
            std::numeric_limits<std::size_t>::max())) {
        return false;
    }

    return storage_manager_.ReadChunk(
        handle,
        block_start,
        static_cast<std::size_t>(block_size),
        data);
}

std::uint64_t ChecksumManager::ExpectedBlockCountLocked(
    std::uint64_t chunk_size) const {
    if (chunk_size == 0) {
        return 0;
    }

    return (
        (chunk_size - 1) /
        constants::kChecksumBlockSize) +
        1;
}

}  // namespace gfs::chunkserver::checksum