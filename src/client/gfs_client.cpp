#include "gfs/client/gfs_client.hpp"

#include <utility>

namespace gfs::client {

GFSClient::GFSClient(
    std::unique_ptr<metadata::MasterClient>
        master_client,
    std::unique_ptr<metadata::ChunkLocationCache>
        location_cache,
    io::Reader::ReadFunction read_function,
    io::Writer::WriteFunction write_function,
    io::Writer::FileSizeUpdater
        file_size_updater)
    : master_client_(std::move(master_client)),
      location_cache_(std::move(location_cache)),
      reader_(
          *master_client_,
          *location_cache_,
          std::move(read_function)),
      writer_(
          *master_client_,
          *location_cache_,
          std::move(write_function),
          file_size_updater),
      record_appender_(
          *master_client_,
          *location_cache_) {
    record_appender_.SetFileSizeUpdater(
        std::move(file_size_updater));
}

GFSClient::~GFSClient() = default;

void GFSClient::SetFileCreateFunction(
    FileCreateFunction create_function) {
    file_create_function_ =
        std::move(create_function);
}

bool GFSClient::CreateFile(
    const FilePath& path) {
    if (path.empty() ||
        !file_create_function_) {
        return false;
    }

    return file_create_function_(path);
}

bool GFSClient::FileExists(
    const FilePath& path) const {
    if (path.empty()) {
        return false;
    }

    return master_client_->LookupFile(
               path)
        .has_value();
}

std::optional<metadata::FileMetadata>
GFSClient::LookupFile(
    const FilePath& path) const {
    if (path.empty()) {
        return std::nullopt;
    }

    return master_client_->LookupFile(
        path);
}

std::optional<metadata::ChunkMetadata>
GFSClient::LookupChunk(
    const FilePath& path,
    ChunkIndex chunk_index) const {
    if (path.empty()) {
        return std::nullopt;
    }

    return master_client_->LookupChunk(
        path,
        chunk_index);
}

std::vector<metadata::ChunkLocation>
GFSClient::LookupChunkLocations(
    const FilePath& path,
    ChunkIndex chunk_index) const {
    if (path.empty()) {
        return {};
    }

    return location_cache_->LookupLocations(
        path,
        chunk_index);
}

bool GFSClient::Read(
    const FilePath& path,
    std::uint64_t offset,
    std::size_t length,
    std::string& data) {
    return reader_.Read(
        path,
        offset,
        length,
        data);
}

bool GFSClient::Write(
    const FilePath& path,
    std::uint64_t offset,
    const std::string& data) {
    return writer_.Write(
        path,
        offset,
        data);
}

bool GFSClient::Append(
    const FilePath& path,
    const std::string& data,
    std::uint64_t& offset) {
    return record_appender_.Append(
        path,
        data,
        offset);
}

void GFSClient::SetRecordAppendFunction(
    io::RecordAppender::AppendFunction
        append_function) {
    record_appender_.SetAppendFunction(
        std::move(append_function));
}

void GFSClient::SetRecordAppendLeaseValidator(
    io::RecordAppender::LeaseValidator
        lease_validator) {
    record_appender_.SetLeaseValidator(
        std::move(lease_validator));
}

void GFSClient::SetRecordAppendPrimarySelector(
    io::RecordAppender::PrimarySelector
        primary_selector) {
    record_appender_.SetPrimarySelector(
        std::move(primary_selector));
}

void GFSClient::SetRecordAppendFileSizeUpdater(
    io::RecordAppender::FileSizeUpdater
        file_size_updater) {
    record_appender_.SetFileSizeUpdater(
        std::move(file_size_updater));
}

void GFSClient::SetRecordAppendChunkSizeUpdater(
    io::RecordAppender::ChunkSizeUpdater
        chunk_size_updater) {
    record_appender_.SetChunkSizeUpdater(
        std::move(chunk_size_updater));
}

void GFSClient::SetRecordAppendRetryPolicy(
    retry::RetryPolicy retry_policy) {
    record_appender_.SetRetryPolicy(
        retry_policy);
}

metadata::MasterClient&
GFSClient::GetMasterClient() noexcept {
    return *master_client_;
}

const metadata::MasterClient&
GFSClient::GetMasterClient() const noexcept {
    return *master_client_;
}

metadata::ChunkLocationCache&
GFSClient::GetLocationCache() noexcept {
    return *location_cache_;
}

const metadata::ChunkLocationCache&
GFSClient::GetLocationCache() const noexcept {
    return *location_cache_;
}

}  // namespace gfs::client