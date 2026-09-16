#include "gfs/master/master.hpp"

#include <utility>

namespace gfs::master {

Master::Master() = default;

Master::~Master() = default;

bool Master::CreateFile(
    const std::string& path,
    std::uint32_t replication_factor) {

    auto lock = namespace_lock_.AcquireWrite(path);

    if (namespace_manager_.Exists(path)) {
        return false;
    }

    if (!namespace_manager_.CreateFile(path)) {
        return false;
    }

    if (!metadata_.CreateFile(path, replication_factor)) {
        namespace_manager_.DeleteFile(path);
        return false;
    }

    return true;
}

bool Master::DeleteFile(const std::string& path) {
    auto lock = namespace_lock_.AcquireWrite(path);

    if (!namespace_manager_.IsFile(path)) {
        return false;
    }

    if (!metadata_.DeleteFile(path)) {
        return false;
    }

    if (!namespace_manager_.DeleteFile(path)) {
        return false;
    }

    return true;
}

bool Master::RenameFile(
    const std::string& source_path,
    const std::string& destination_path) {

    auto lock = namespace_lock_.AcquireWritePath(
        {source_path, destination_path});

    if (!namespace_manager_.IsFile(source_path) ||
        namespace_manager_.Exists(destination_path)) {
        return false;
    }

    if (!metadata_.RenameFile(
            source_path,
            destination_path)) {
        return false;
    }

    if (!namespace_manager_.Rename(
            source_path,
            destination_path)) {
        metadata_.RenameFile(
            destination_path,
            source_path);
        return false;
    }

    return true;
}

bool Master::CreateDirectory(const std::string& path) {
    auto lock = namespace_lock_.AcquireWrite(path);
    return namespace_manager_.CreateDirectory(path);
}

bool Master::DeleteDirectory(const std::string& path) {
    auto lock = namespace_lock_.AcquireWrite(path);
    return namespace_manager_.DeleteDirectory(path);
}

bool Master::FileExists(const std::string& path) const {
    return namespace_manager_.IsFile(path);
}

bool Master::DirectoryExists(const std::string& path) const {
    return namespace_manager_.IsDirectory(path);
}

std::optional<Master::FileInfo>
Master::GetFileInfo(const std::string& path) const {

    const auto metadata = metadata_.GetFile(path);
    if (!metadata.has_value()) {
        return std::nullopt;
    }

    FileInfo info;
    info.path = metadata->GetPath();
    info.size = metadata->GetSize();
    info.replication_factor =
        metadata->GetReplicationFactor();
    info.chunk_handles =
        metadata->GetChunkHandles();

    return info;
}

std::optional<Master::ChunkInfo>
Master::GetChunkInfo(
    ChunkHandle chunk_handle) const {

    const auto metadata =
        metadata_.GetChunk(chunk_handle);

    if (!metadata.has_value()) {
        return std::nullopt;
    }

    ChunkInfo info;
    info.handle = metadata->GetHandle();
    info.version = metadata->GetVersion();
    info.size = metadata->GetSize();
    info.replicas =
        metadata->GetReplicaServerIds();

    return info;
}

std::optional<ChunkHandle>
Master::AllocateChunk(const std::string& path) {

    auto lock = namespace_lock_.AcquireWrite(path);

    if (!namespace_manager_.IsFile(path)) {
        return std::nullopt;
    }

    return metadata_.AllocateChunk(path);
}

bool Master::AddReplica(
    ChunkHandle chunk_handle,
    ServerId server_id) {

    return metadata_.AddReplica(
        chunk_handle,
        server_id);
}

bool Master::RemoveReplica(
    ChunkHandle chunk_handle,
    ServerId server_id) {

    return metadata_.RemoveReplica(
        chunk_handle,
        server_id);
}

std::vector<ServerId>
Master::GetChunkReplicas(
    ChunkHandle chunk_handle) const {

    return metadata_.GetReplicas(chunk_handle);
}

std::size_t Master::FileCount() const {
    return metadata_.FileCount();
}

std::size_t Master::ChunkCount() const {
    return metadata_.ChunkCount();
}

std::size_t Master::NamespaceNodeCount() const {
    return namespace_manager_.NodeCount();
}

namespace_management::NamespaceManager&
Master::GetNamespaceManager() noexcept {
    return namespace_manager_;
}

const namespace_management::NamespaceManager&
Master::GetNamespaceManager() const noexcept {
    return namespace_manager_;
}

metadata::Metadata& Master::GetMetadata() noexcept {
    return metadata_;
}

const metadata::Metadata&
Master::GetMetadata() const noexcept {
    return metadata_;
}

namespace_management::NamespaceLock&
Master::GetNamespaceLock() noexcept {
    return namespace_lock_;
}

}  // namespace gfs::master