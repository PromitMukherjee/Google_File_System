#pragma once

#include "gfs/client/metadata/chunk_location_cache.hpp"
#include "gfs/client/metadata/master_client.hpp"
#include "gfs/client/retry/retry_policy.hpp"
#include "gfs/common/types.hpp"

#include <cstddef>
#include <cstdint>
#include <functional>
#include <optional>
#include <string>
#include <vector>

namespace gfs::client::io {

class RecordAppender {
public:
    enum class AppendStatus {
        Success,
        RetryNextChunk,
        Failed
    };

    struct AppendResult {
        AppendStatus status = AppendStatus::Failed;
        std::uint64_t offset = 0;
        std::uint64_t chunk_size = 0;
        std::size_t bytes_appended = 0;
    };

    using AppendFunction = std::function<AppendResult(
        const FilePath& path,
        const metadata::ChunkLocation& primary,
        ChunkHandle handle,
        ChunkVersion version,
        const std::vector<metadata::ChunkLocation>& replicas,
        const std::string& data)>;

    using LeaseValidator = std::function<bool(
        ChunkHandle handle,
        ServerId primary_server_id)>;

    using PrimarySelector = std::function<std::optional<ServerId>(
        ChunkHandle handle,
        const std::vector<metadata::ChunkLocation>& replicas)>;

    using FileSizeUpdater = std::function<bool(
        const FilePath& path,
        std::uint64_t size)>;

    using ChunkSizeUpdater = std::function<bool(
        ChunkHandle handle,
        std::uint64_t size)>;

    explicit RecordAppender(
        metadata::MasterClient& master_client,
        metadata::ChunkLocationCache& location_cache,
        retry::RetryPolicy retry_policy =
            retry::RetryPolicy{});

    [[nodiscard]] bool Append(
        const FilePath& path,
        const std::string& data,
        std::uint64_t& offset);

    void SetAppendFunction(
        AppendFunction append_function);

    void SetLeaseValidator(
        LeaseValidator lease_validator);

    void SetPrimarySelector(
        PrimarySelector primary_selector);

    void SetFileSizeUpdater(
        FileSizeUpdater file_size_updater);

    void SetChunkSizeUpdater(
        ChunkSizeUpdater chunk_size_updater);

    void SetRetryPolicy(
        retry::RetryPolicy retry_policy);

    [[nodiscard]] const retry::RetryPolicy&
    GetRetryPolicy() const noexcept;

private:
    [[nodiscard]] bool ResolveOrAllocateChunk(
        const FilePath& path,
        ChunkIndex chunk_index,
        metadata::ChunkLocationCacheEntry& entry);

    [[nodiscard]] std::optional<ServerId>
    SelectPrimary(
        ChunkHandle handle,
        const std::vector<metadata::ChunkLocation>& replicas)
        const;

    [[nodiscard]] bool ValidateLease(
        ChunkHandle handle,
        ServerId primary_server_id) const;

    metadata::MasterClient& master_client_;
    metadata::ChunkLocationCache& location_cache_;

    AppendFunction append_function_;
    LeaseValidator lease_validator_;
    PrimarySelector primary_selector_;

    FileSizeUpdater file_size_updater_;
    ChunkSizeUpdater chunk_size_updater_;

    retry::RetryPolicy retry_policy_;
};

}  // namespace gfs::client::io