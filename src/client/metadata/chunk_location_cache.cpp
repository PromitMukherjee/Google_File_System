#include "gfs/client/metadata/chunk_location_cache.hpp"

#include <functional>
#include <mutex>
#include <utility>

namespace gfs::client::metadata {

std::size_t ChunkCacheKeyHash::operator()(
    const ChunkCacheKey& key) const noexcept {
    const std::size_t path_hash =
        std::hash<std::string>{}(key.path);

    const std::size_t index_hash =
        std::hash<ChunkIndex>{}(key.chunk_index);

    return path_hash ^
           (index_hash + static_cast<std::size_t>(0x9e3779b9U) +
            (path_hash << 6U) +
            (path_hash >> 2U));
}

ChunkLocationCache::ChunkLocationCache(std::chrono::seconds ttl)
    : ttl_(ttl) {}

bool ChunkLocationCache::Insert(
    const FilePath& path,
    ChunkIndex chunk_index,
    const ChunkLocationCacheEntry& entry) {
    if (path.empty() || entry.handle == 0) {
        return false;
    }

    ChunkLocationCacheEntry copy = entry;
    copy.chunk_index = chunk_index;

    if (copy.inserted_at.time_since_epoch().count() == 0) {
        copy.inserted_at = std::chrono::steady_clock::now();
    }

    std::unique_lock lock(mutex_);

    entries_[ChunkCacheKey{path, chunk_index}] = std::move(copy);
    return true;
}

bool ChunkLocationCache::Insert(
    const FilePath& path,
    ChunkIndex chunk_index,
    ChunkHandle handle,
    std::vector<ChunkLocation> locations) {
    ChunkLocationCacheEntry entry;
    entry.handle = handle;
    entry.chunk_index = chunk_index;
    entry.locations = std::move(locations);
    entry.inserted_at = std::chrono::steady_clock::now();

    return Insert(path, chunk_index, entry);
}

std::optional<ChunkLocationCacheEntry> ChunkLocationCache::Lookup(
    const FilePath& path,
    ChunkIndex chunk_index) const {
    if (path.empty()) {
        return std::nullopt;
    }

    std::shared_lock lock(mutex_);

    const auto it =
        entries_.find(ChunkCacheKey{path, chunk_index});

    if (it == entries_.end()) {
        return std::nullopt;
    }

    if (IsExpired(it->second)) {
        return std::nullopt;
    }

    return it->second;
}

std::optional<ChunkHandle> ChunkLocationCache::LookupHandle(
    const FilePath& path,
    ChunkIndex chunk_index) const {
    const auto entry = Lookup(path, chunk_index);

    if (!entry.has_value()) {
        return std::nullopt;
    }

    return entry->handle;
}

std::vector<ChunkLocation> ChunkLocationCache::LookupLocations(
    const FilePath& path,
    ChunkIndex chunk_index) const {
    const auto entry = Lookup(path, chunk_index);

    if (!entry.has_value()) {
        return {};
    }

    return entry->locations;
}

bool ChunkLocationCache::Invalidate(
    const FilePath& path,
    ChunkIndex chunk_index) {
    if (path.empty()) {
        return false;
    }

    std::unique_lock lock(mutex_);

    return entries_.erase(
               ChunkCacheKey{path, chunk_index}) > 0;
}

void ChunkLocationCache::InvalidateFile(const FilePath& path) {
    if (path.empty()) {
        return;
    }

    std::unique_lock lock(mutex_);

    for (auto it = entries_.begin(); it != entries_.end();) {
        if (it->first.path == path) {
            it = entries_.erase(it);
        } else {
            ++it;
        }
    }
}

void ChunkLocationCache::Clear() {
    std::unique_lock lock(mutex_);
    entries_.clear();
}

std::size_t ChunkLocationCache::Size() const {
    std::shared_lock lock(mutex_);
    return entries_.size();
}

bool ChunkLocationCache::Empty() const {
    std::shared_lock lock(mutex_);
    return entries_.empty();
}

void ChunkLocationCache::SetTtl(std::chrono::seconds ttl) {
    if (ttl.count() < 0) {
        ttl = std::chrono::seconds::zero();
    }

    std::unique_lock lock(mutex_);
    ttl_ = ttl;
}

std::chrono::seconds ChunkLocationCache::GetTtl() const {
    std::shared_lock lock(mutex_);
    return ttl_;
}

bool ChunkLocationCache::IsExpired(
    const ChunkLocationCacheEntry& entry) const {
    if (ttl_.count() == 0) {
        return false;
    }

    const auto now = std::chrono::steady_clock::now();

    return now - entry.inserted_at >= ttl_;
}

}  // namespace gfs::client::metadata