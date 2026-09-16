#include "gfs/chunkserver/storage/chunk_file.hpp"

#include <algorithm>
#include <filesystem>
#include <limits>
#include <mutex>
#include <utility>

namespace gfs::chunkserver::storage {

ChunkFile::ChunkFile(
    ChunkHandle handle,
    std::string path)
    : handle_(handle),
      path_(std::move(path)) {
}

ChunkFile::~ChunkFile() {
    Close();
}

bool ChunkFile::Create() {
    std::unique_lock lock(mutex_);

    if (handle_ == 0 || path_.empty()) {
        return false;
    }

    if (file_handle_ && file_handle_->IsOpen()) {
        return false;
    }

    std::error_code error;

    const std::filesystem::path file_path(path_);

    if (file_path.has_parent_path()) {
        std::filesystem::create_directories(
            file_path.parent_path(),
            error);

        if (error) {
            return false;
        }
    }

    if (std::filesystem::exists(file_path, error)) {
        return false;
    }

    auto handle =
        std::make_unique<gfs::storage::FileHandle>();

    if (!handle->Open(
            path_,
            gfs::storage::FileHandle::OpenMode::ReadWrite,
            true,
            true)) {
        return false;
    }

    file_handle_ = std::move(handle);
    size_ = 0;

    return true;
}

bool ChunkFile::Open() {
    std::unique_lock lock(mutex_);

    if (handle_ == 0 || path_.empty()) {
        return false;
    }

    if (file_handle_ && file_handle_->IsOpen()) {
        return true;
    }

    if (!std::filesystem::exists(path_)) {
        return false;
    }

    auto handle =
        std::make_unique<gfs::storage::FileHandle>();

    if (!handle->Open(
            path_,
            gfs::storage::FileHandle::OpenMode::ReadWrite,
            false,
            false)) {
        return false;
    }

    const auto file_size = handle->Size();

    if (!file_size.has_value() ||
        *file_size > constants::kChunkSize) {
        handle->Close();
        return false;
    }

    file_handle_ = std::move(handle);
    size_ = *file_size;

    return true;
}

bool ChunkFile::Close() {
    std::unique_lock lock(mutex_);

    if (!file_handle_) {
        return true;
    }

    const bool result = file_handle_->Close();
    file_handle_.reset();

    return result;
}

bool ChunkFile::Delete() {
    std::unique_lock lock(mutex_);

    if (file_handle_) {
        if (!file_handle_->Close()) {
            return false;
        }

        file_handle_.reset();
    }

    std::error_code error;

    const bool removed =
        std::filesystem::remove(path_, error);

    size_ = 0;

    return removed && !error;
}

bool ChunkFile::IsOpen() const noexcept {
    std::shared_lock lock(mutex_);

    return file_handle_ &&
           file_handle_->IsOpen();
}

bool ChunkFile::Exists() const {
    std::shared_lock lock(mutex_);

    return std::filesystem::exists(path_);
}

ChunkHandle ChunkFile::GetHandle() const noexcept {
    return handle_;
}

const std::string& ChunkFile::GetPath() const noexcept {
    return path_;
}

std::uint64_t ChunkFile::GetSize() const {
    std::shared_lock lock(mutex_);

    return size_;
}

bool ChunkFile::Read(
    std::uint64_t offset,
    std::size_t length,
    std::vector<std::uint8_t>& data) const {

    std::shared_lock lock(mutex_);

    if (!file_handle_ ||
        !file_handle_->IsOpen() ||
        !IsRangeValid(offset, length)) {
        return false;
    }

    data.resize(length);

    if (length == 0) {
        return true;
    }

    const auto bytes_read =
        file_handle_->ReadAt(
            offset,
            data.data(),
            length);

    if (!bytes_read.has_value()) {
        data.clear();
        return false;
    }

    data.resize(*bytes_read);
    return true;
}

bool ChunkFile::Write(
    std::uint64_t offset,
    const std::vector<std::uint8_t>& data) {

    std::unique_lock lock(mutex_);

    if (!file_handle_ ||
        !file_handle_->IsOpen()) {
        return false;
    }

    if (offset > constants::kChunkSize ||
        data.size() >
            constants::kChunkSize - offset) {
        return false;
    }

    if (data.empty()) {
        return true;
    }

    const auto bytes_written =
        file_handle_->WriteAt(
            offset,
            data.data(),
            data.size());

    if (!bytes_written.has_value() ||
        *bytes_written != data.size()) {
        return false;
    }

    size_ = std::max(
        size_,
        offset + data.size());

    return true;
}

bool ChunkFile::Truncate(std::uint64_t size) {
    std::unique_lock lock(mutex_);

    if (!file_handle_ ||
        !file_handle_->IsOpen() ||
        size > constants::kChunkSize) {
        return false;
    }

    if (!file_handle_->Truncate(size)) {
        return false;
    }

    size_ = size;
    return true;
}

bool ChunkFile::IsRangeValid(
    std::uint64_t offset,
    std::size_t length) const {

    if (offset > size_) {
        return false;
    }

    return length <= size_ - offset;
}

}  // namespace gfs::chunkserver::storage