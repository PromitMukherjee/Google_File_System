#include "gfs/client/metadata/master_client.hpp"

#include <chrono>
#include <grpcpp/grpcpp.h>
#include <memory>
#include <utility>

#include "master.grpc.pb.h"

namespace gfs::client::metadata {
namespace {

std::unique_ptr<
    ::gfs::protocol::MasterService::Stub>
MakeStub(
    const std::string& address) {
    if (address.empty()) {
        return nullptr;
    }

    const auto channel =
        ::grpc::CreateChannel(
            address,
            ::grpc::InsecureChannelCredentials());

    if (!channel) {
        return nullptr;
    }

    return ::gfs::protocol::MasterService::
        NewStub(channel);
}

void SetDeadline(
    ::grpc::ClientContext& context) {
    context.set_deadline(
        std::chrono::system_clock::now() +
        std::chrono::seconds(3));
}

}  // namespace

MasterClient::MasterClient(
    std::string master_address)
    : master_address_(
          std::move(master_address)) {
}

MasterClient::MasterClient(
    std::string master_address,
    FileLookup file_lookup,
    ChunkLookup chunk_lookup,
    ChunkAllocation chunk_allocator)
    : master_address_(
          std::move(master_address)),
      file_lookup_(
          std::move(file_lookup)),
      chunk_lookup_(
          std::move(chunk_lookup)),
      chunk_allocator_(
          std::move(chunk_allocator)) {
}

void MasterClient::SetMasterAddress(
    std::string master_address) {
    master_address_ =
        std::move(master_address);
}

const std::string&
MasterClient::GetMasterAddress() const noexcept {
    return master_address_;
}

void MasterClient::SetFileLookup(
    FileLookup lookup) {
    file_lookup_ =
        std::move(lookup);
}

void MasterClient::SetChunkLookup(
    ChunkLookup lookup) {
    chunk_lookup_ =
        std::move(lookup);
}

void MasterClient::SetChunkAllocator(
    ChunkAllocation allocator) {
    chunk_allocator_ =
        std::move(allocator);
}

bool MasterClient::IsConfigured()
    const noexcept {
    return !master_address_.empty() ||
           (static_cast<bool>(file_lookup_) &&
            static_cast<bool>(chunk_lookup_));
}

std::optional<FileMetadata>
MasterClient::LookupFile(
    const FilePath& path) const {
    if (path.empty()) {
        return std::nullopt;
    }

    if (file_lookup_) {
        return file_lookup_(path);
    }

    auto stub =
        MakeStub(master_address_);

    if (!stub) {
        return std::nullopt;
    }

    ::gfs::protocol::GetFileInfoRequest
        request;

    request.set_path(path);

    ::gfs::protocol::GetFileInfoResponse
        response;

    ::grpc::ClientContext context;

    SetDeadline(context);

    const auto status =
        stub->GetFileInfo(
            &context,
            request,
            &response);

    if (!status.ok() ||
        !response.success()) {
        return std::nullopt;
    }

    FileMetadata result;

    result.path =
        response.metadata().path();

    result.size =
        response.metadata().size();

    result.replication_factor =
        response.metadata()
            .replication_factor();

    result.chunk_count =
        static_cast<std::size_t>(
            response.metadata()
                .chunk_handles_size());

    result.chunk_handles.reserve(
        static_cast<std::size_t>(
            response.metadata()
                .chunk_handles_size()));

    for (const auto handle :
         response.metadata()
             .chunk_handles()) {
        result.chunk_handles.push_back(
            handle);
    }

    return result;
}

std::optional<ChunkHandle>
MasterClient::LookupChunkHandle(
    const FilePath& path,
    ChunkIndex chunk_index) const {
    const auto metadata =
        LookupChunk(
            path,
            chunk_index);

    if (!metadata.has_value()) {
        return std::nullopt;
    }

    return metadata->handle;
}

std::optional<ChunkMetadata>
MasterClient::LookupChunk(
    const FilePath& path,
    ChunkIndex chunk_index) const {
    if (path.empty()) {
        return std::nullopt;
    }

    if (chunk_lookup_) {
        return chunk_lookup_(
            path,
            chunk_index);
    }

    auto stub =
        MakeStub(master_address_);

    if (!stub) {
        return std::nullopt;
    }

    ::gfs::protocol::GetChunkLocationsRequest
        request;

    request.set_path(path);
    request.set_chunk_index(
        chunk_index);

    ::gfs::protocol::GetChunkLocationsResponse
        response;

    ::grpc::ClientContext context;

    SetDeadline(context);

    const auto status =
        stub->GetChunkLocations(
            &context,
            request,
            &response);

    if (!status.ok() ||
        !response.success()) {
        return std::nullopt;
    }

    ChunkMetadata result;

    result.handle =
        response.location()
            .chunk_handle();

    result.index =
        chunk_index;

    result.version =
        response.location()
            .version();

    for (int index = 0;
         index <
         response.location()
             .replica_server_ids_size();
         ++index) {
        ChunkLocation location;

        location.server_id =
            response.location()
                .replica_server_ids(index);

        if (index <
            response.location()
                .replica_addresses_size()) {
            location.address =
                response.location()
                    .replica_addresses(index);
        }

        result.locations.push_back(
            std::move(location));
    }

    return result;
}

std::vector<ChunkLocation>
MasterClient::LookupChunkLocations(
    const FilePath& path,
    ChunkIndex chunk_index) const {
    const auto metadata =
        LookupChunk(
            path,
            chunk_index);

    if (!metadata.has_value()) {
        return {};
    }

    return metadata->locations;
}

std::optional<ChunkMetadata>
MasterClient::AllocateChunk(
    const FilePath& path,
    ChunkIndex chunk_index) const {
    if (path.empty()) {
        return std::nullopt;
    }

    if (chunk_allocator_) {
        return chunk_allocator_(
            path,
            chunk_index);
    }

    const auto file =
        LookupFile(path);

    if (!file.has_value() ||
        chunk_index !=
            file->chunk_count) {
        return std::nullopt;
    }

    auto stub =
        MakeStub(master_address_);

    if (!stub) {
        return std::nullopt;
    }

    ::gfs::protocol::AllocateChunkRequest
        request;

    request.set_path(path);

    ::gfs::protocol::AllocateChunkResponse
        response;

    ::grpc::ClientContext context;

    SetDeadline(context);

    const auto status =
        stub->AllocateChunk(
            &context,
            request,
            &response);

    if (!status.ok() ||
        !response.success()) {
        return std::nullopt;
    }

    ChunkMetadata result;

    result.handle =
        response.chunk_handle();

    result.index =
        chunk_index;

    result.version =
        response.chunk_version();

    for (int index = 0;
         index <
         response.replica_server_ids_size();
         ++index) {
        ChunkLocation location;

        location.server_id =
            response.replica_server_ids(
                index);

        if (index <
            response.replica_addresses_size()) {
            location.address =
                response.replica_addresses(
                    index);
        }

        result.locations.push_back(
            std::move(location));
    }

    return result;
}

bool MasterClient::CreateFile(
    const FilePath& path,
    std::uint32_t replication_factor) const {
    if (path.empty()) {
        return false;
    }

    auto stub =
        MakeStub(master_address_);

    if (!stub) {
        return false;
    }

    ::gfs::protocol::CreateFileRequest
        request;

    request.set_path(path);
    request.set_replication_factor(
        replication_factor);

    ::gfs::protocol::CreateFileResponse
        response;

    ::grpc::ClientContext context;

    SetDeadline(context);

    const auto status =
        stub->CreateFile(
            &context,
            request,
            &response);

    return status.ok() &&
           response.success();
}

bool MasterClient::CreateDirectory(
    const FilePath& path) const {
    if (path.empty()) {
        return false;
    }

    auto stub =
        MakeStub(master_address_);

    if (!stub) {
        return false;
    }

    ::gfs::protocol::CreateDirectoryRequest
        request;

    request.set_path(path);

    ::gfs::protocol::CreateDirectoryResponse
        response;

    ::grpc::ClientContext context;

    SetDeadline(context);

    const auto status =
        stub->CreateDirectory(
            &context,
            request,
            &response);

    return status.ok() &&
           response.success();
}

std::vector<DirectoryEntry>
MasterClient::ListDirectory(
    const FilePath& path) const {
    if (path.empty()) {
        return {};
    }

    auto stub =
        MakeStub(master_address_);

    if (!stub) {
        return {};
    }

    ::gfs::protocol::ListDirectoryRequest
        request;

    request.set_path(path);

    ::gfs::protocol::ListDirectoryResponse
        response;

    ::grpc::ClientContext context;

    SetDeadline(context);

    const auto status =
        stub->ListDirectory(
            &context,
            request,
            &response);

    if (!status.ok() ||
        !response.success()) {
        return {};
    }

    std::vector<DirectoryEntry> result;

    result.reserve(
        static_cast<std::size_t>(
            response.entries_size()));

    for (const auto& entry :
         response.entries()) {
        result.push_back(
            DirectoryEntry{
                entry.path(),
                entry.directory()});
    }

    return result;
}

bool MasterClient::DeleteFile(
    const FilePath& path) const {
    if (path.empty()) {
        return false;
    }

    auto stub =
        MakeStub(master_address_);

    if (!stub) {
        return false;
    }

    ::gfs::protocol::DeleteFileRequest
        request;

    request.set_path(path);

    ::gfs::protocol::DeleteFileResponse
        response;

    ::grpc::ClientContext context;

    SetDeadline(context);

    const auto status =
        stub->DeleteFile(
            &context,
            request,
            &response);

    return status.ok() &&
           response.success();
}

bool MasterClient::RenameFile(
    const FilePath& source_path,
    const FilePath& destination_path) const {
    if (source_path.empty() ||
        destination_path.empty()) {
        return false;
    }

    auto stub =
        MakeStub(master_address_);

    if (!stub) {
        return false;
    }

    ::gfs::protocol::RenameFileRequest
        request;

    request.set_source_path(
        source_path);

    request.set_destination_path(
        destination_path);

    ::gfs::protocol::RenameFileResponse
        response;

    ::grpc::ClientContext context;

    SetDeadline(context);

    const auto status =
        stub->RenameFile(
            &context,
            request,
            &response);

    return status.ok() &&
           response.success();
}

bool MasterClient::CreateSnapshot(
    const FilePath& source_path,
    const FilePath& snapshot_path) const {
    if (source_path.empty() ||
        snapshot_path.empty()) {
        return false;
    }

    auto stub =
        MakeStub(master_address_);

    if (!stub) {
        return false;
    }

    ::gfs::protocol::CreateSnapshotRequest
        request;

    request.set_source_path(
        source_path);

    request.set_snapshot_path(
        snapshot_path);

    ::gfs::protocol::CreateSnapshotResponse
        response;

    ::grpc::ClientContext context;

    SetDeadline(context);

    const auto status =
        stub->CreateSnapshot(
            &context,
            request,
            &response);

    return status.ok() &&
           response.success();
}

bool MasterClient::UpdateFileSize(
    const FilePath& path,
    std::uint64_t size) const {
    if (path.empty()) {
        return false;
    }

    auto stub =
        MakeStub(master_address_);

    if (!stub) {
        return false;
    }

    ::gfs::protocol::UpdateFileSizeRequest
        request;

    request.set_path(path);
    request.set_size(size);

    ::gfs::protocol::UpdateFileSizeResponse
        response;

    ::grpc::ClientContext context;

    SetDeadline(context);

    const auto status =
        stub->UpdateFileSize(
            &context,
            request,
            &response);

    return status.ok() &&
           response.success();
}

}  // namespace gfs::client::metadata