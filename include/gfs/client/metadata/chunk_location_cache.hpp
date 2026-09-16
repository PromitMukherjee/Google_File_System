#pragma once

#include "gfs/client/metadata/master_client.hpp"
#include "gfs/common/types.hpp"

#include <chrono>
#include <cstddef>
#include <optional>
#include <shared_mutex>
#include <string>
#include <unordered_map>
#include <vector>

namespace gfs::client::metadata {

struct ChunkCacheKey {
    FilePath path;
    ChunkIndex chunk_index = 0;

    friend bool operator==(const ChunkCacheKey& lhs,
                           const ChunkCacheKey& rhs) = default;
};

struct ChunkCacheKeyHash {
    std::size_t operator()(const ChunkCacheKey& key) const noexcept;
};

struct ChunkLocationCacheEntry {
    ChunkHandle handle = 0;
    ChunkIndex chunk_index = 0;
    std::vector<ChunkLocation> locations;
    std::chrono::steady_clock::time_point inserted_at;
};

class ChunkLocationCache {
public:
    explicit ChunkLocationCache(
        std::chrono::seconds ttl = std::chrono::seconds::zero());

    ChunkLocationCache(const ChunkLocationCache&) = delete;
    ChunkLocationCache& operator=(const ChunkLocationCache&) = delete;

    [[nodiscard]] bool Insert(
        const FilePath& path,
        ChunkIndex chunk_index,
        const ChunkLocationCacheEntry& entry);

    [[nodiscard]] bool Insert(
        const FilePath& path,
        ChunkIndex chunk_index,
        ChunkHandle handle,
        std::vector<ChunkLocation> locations);

    [[nodiscard]] std::optional<ChunkLocationCacheEntry> Lookup(
        const FilePath& path,
        ChunkIndex chunk_index) const;

    [[nodiscard]] std::optional<ChunkHandle> LookupHandle(
        const FilePath& path,
        ChunkIndex chunk_index) const;

    [[nodiscard]] std::vector<ChunkLocation> LookupLocations(
        const FilePath& path,
        ChunkIndex chunk_index) const;

    bool Invalidate(
        const FilePath& path,
        ChunkIndex chunk_index);

    void InvalidateFile(const FilePath& path);

    void Clear();

    [[nodiscard]] std::size_t Size() const;

    [[nodiscard]] bool Empty() const;

    void SetTtl(std::chrono::seconds ttl);

    [[nodiscard]] std::chrono::seconds GetTtl() const;

private:
    [[nodiscard]] bool IsExpired(
        const ChunkLocationCacheEntry& entry) const;

    mutable std::shared_mutex mutex_;
    std::unordered_map<
        ChunkCacheKey,
        ChunkLocationCacheEntry,
        ChunkCacheKeyHash> entries_;

    std::chrono::seconds ttl_;
};

}  // namespace gfs::client::metadata