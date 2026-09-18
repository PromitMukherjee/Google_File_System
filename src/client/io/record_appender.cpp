#include "gfs/client/io/record_appender.hpp"

#include "gfs/common/constants.hpp"

#include <algorithm>
#include <chrono>
#include <limits>
#include <utility>

namespace gfs::client::io {

RecordAppender::RecordAppender(
    metadata::MasterClient& master_client,
    metadata::ChunkLocationCache& location_cache,
    retry::RetryPolicy retry_policy)
    : master_client_(master_client),
      location_cache_(location_cache),
      retry_policy_(retry_policy) {
}

bool RecordAppender::Append(
    const FilePath& path,
    const std::string& data,
    std::uint64_t& offset) {
    offset = 0;

    if (path.empty() ||
        data.empty() ||
        data.size() >
            constants::kChunkSize / 4U ||
        !append_function_) {
        return false;
    }

    const auto file =
        master_client_.LookupFile(path);

    if (!file.has_value()) {
        return false;
    }

    ChunkIndex chunk_index =
        file->chunk_count == 0
            ? 0
            : file->chunk_count - 1;

    std::size_t attempts = 0;

    while (retry_policy_.CanRetry(attempts)) {
        ++attempts;

        metadata::ChunkLocationCacheEntry entry;

        if (!ResolveOrAllocateChunk(
                path,
                chunk_index,
                entry)) {
            if (!retry_policy_.CanRetry(attempts)) {
                return false;
            }

            continue;
        }

        if (entry.locations.empty() ||
            entry.handle == 0) {
            location_cache_.Invalidate(
                path,
                chunk_index);
            continue;
        }

        const auto primary_server_id =
            SelectPrimary(
                entry.handle,
                entry.locations);

        if (!primary_server_id.has_value()) {
            location_cache_.Invalidate(
                path,
                chunk_index);
            continue;
        }

        if (!ValidateLease(
                entry.handle,
                *primary_server_id)) {
            location_cache_.Invalidate(
                path,
                chunk_index);
            continue;
        }

        const auto primary_it =
            std::find_if(
                entry.locations.begin(),
                entry.locations.end(),
                [primary_server_id](
                    const metadata::ChunkLocation& location) {
                    return location.server_id ==
                           *primary_server_id;
                });

        if (primary_it == entry.locations.end()) {
            location_cache_.Invalidate(
                path,
                chunk_index);
            continue;
        }

        auto chunk =
            master_client_.LookupChunk(
                path,
                chunk_index);

        if (!chunk.has_value() ||
            chunk->handle != entry.handle) {
            location_cache_.Invalidate(
                path,
                chunk_index);
            continue;
        }

        const AppendResult result =
            append_function_(
                *primary_it,
                entry.handle,
                chunk->version == 0
                    ? 1
                    : chunk->version,
                entry.locations,
                data);

        if (result.status ==
            AppendStatus::Success) {
            const std::uint64_t chunk_base =
                static_cast<std::uint64_t>(
                    chunk_index) *
                static_cast<std::uint64_t>(
                    constants::kChunkSize);

            if (result.offset >
                std::numeric_limits<
                    std::uint64_t>::max() -
                    chunk_base) {
                return false;
            }

            const std::uint64_t global_offset =
                chunk_base + result.offset;

            if (global_offset >
                std::numeric_limits<
                    std::uint64_t>::max() -
                    static_cast<std::uint64_t>(
                        data.size())) {
                return false;
            }

            const std::uint64_t end_offset =
                global_offset +
                static_cast<std::uint64_t>(
                    data.size());

            if (result.chunk_size != 0 &&
                chunk_size_updater_ &&
                !chunk_size_updater_(
                    entry.handle,
                    result.chunk_size)) {
                return false;
            }

            if (file_size_updater_) {
                const std::uint64_t current_size =
                    file->size > end_offset
                        ? file->size
                        : end_offset;

                if (!file_size_updater_(
                        path,
                        current_size)) {
                    return false;
                }
            }

            offset = global_offset;

            return result.bytes_appended ==
                   data.size();
        }

        if (result.status ==
            AppendStatus::RetryNextChunk) {
            if (result.chunk_size != 0 &&
                chunk_size_updater_ &&
                !chunk_size_updater_(
                    entry.handle,
                    result.chunk_size)) {
                return false;
            }

            location_cache_.Invalidate(
                path,
                chunk_index);

            if (chunk_index ==
                std::numeric_limits<
                    ChunkIndex>::max()) {
                return false;
            }

            ++chunk_index;
            continue;
        }

        location_cache_.Invalidate(
            path,
            chunk_index);
    }

    return false;
}

void RecordAppender::SetAppendFunction(
    AppendFunction append_function) {
    append_function_ =
        std::move(append_function);
}

void RecordAppender::SetLeaseValidator(
    LeaseValidator lease_validator) {
    lease_validator_ =
        std::move(lease_validator);
}

void RecordAppender::SetPrimarySelector(
    PrimarySelector primary_selector) {
    primary_selector_ =
        std::move(primary_selector);
}

void RecordAppender::SetFileSizeUpdater(
    FileSizeUpdater file_size_updater) {
    file_size_updater_ =
        std::move(file_size_updater);
}

void RecordAppender::SetChunkSizeUpdater(
    ChunkSizeUpdater chunk_size_updater) {
    chunk_size_updater_ =
        std::move(chunk_size_updater);
}

void RecordAppender::SetRetryPolicy(
    retry::RetryPolicy retry_policy) {
    retry_policy_ = retry_policy;
}

const retry::RetryPolicy&
RecordAppender::GetRetryPolicy()
    const noexcept {
    return retry_policy_;
}

bool RecordAppender::ResolveOrAllocateChunk(
    const FilePath& path,
    ChunkIndex chunk_index,
    metadata::ChunkLocationCacheEntry& entry) {
    const auto cached =
        location_cache_.Lookup(
            path,
            chunk_index);

    if (cached.has_value()) {
        entry = *cached;
        return true;
    }

    auto chunk =
        master_client_.LookupChunk(
            path,
            chunk_index);

    if (!chunk.has_value()) {
        chunk =
            master_client_.AllocateChunk(
                path,
                chunk_index);
    }

    if (!chunk.has_value() ||
        chunk->handle == 0 ||
        chunk->locations.empty()) {
        return false;
    }

    entry.handle = chunk->handle;
    entry.chunk_index = chunk_index;
    entry.locations = chunk->locations;
    entry.inserted_at =
        std::chrono::steady_clock::now();

    return location_cache_.Insert(
        path,
        chunk_index,
        entry);
}

std::optional<ServerId>
RecordAppender::SelectPrimary(
    ChunkHandle handle,
    const std::vector<
        metadata::ChunkLocation>& replicas)
    const {
    if (handle == 0 ||
        replicas.empty()) {
        return std::nullopt;
    }

    if (primary_selector_) {
        return primary_selector_(
            handle,
            replicas);
    }

    for (const auto& replica :
         replicas) {
        if (replica.server_id != 0) {
            return replica.server_id;
        }
    }

    return std::nullopt;
}

bool RecordAppender::ValidateLease(
    ChunkHandle handle,
    ServerId primary_server_id) const {
    if (handle == 0 ||
        primary_server_id == 0) {
        return false;
    }

    if (!lease_validator_) {
        return true;
    }

    return lease_validator_(
        handle,
        primary_server_id);
}

}  // namespace gfs::client::io