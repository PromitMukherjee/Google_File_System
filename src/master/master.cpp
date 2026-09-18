#include "gfs/master/master.hpp"

#include <algorithm>
#include <cstdint>
#include <filesystem>
#include <limits>
#include <optional>
#include <string>
#include <vector>

namespace gfs::master {

Master::Master(
    std::uint32_t default_replication_factor,
    std::filesystem::path persistence_directory)
    : metadata_(),
      namespace_manager_(),
      replica_manager_(),
      placement_policy_(default_replication_factor),
      lease_manager_(replica_manager_),
      heartbeat_manager_(),
      re_replication_manager_(*this),
      recovery_manager_(re_replication_manager_),
      persistence_directory_(
          std::move(persistence_directory)),
      operation_log_(),
      checkpoint_(),
      orphan_chunk_manager_(),
      garbage_collector_(
          *this,
          orphan_chunk_manager_),
      rebalancer_(*this) {
    if (!persistence_directory_.empty()) {
        checkpoint_ =
            recovery::Checkpoint(
                persistence_directory_);

        operation_log_.Open(
            persistence_directory_ /
            "operation.log");
    }
}

bool Master::AppendOperation(
    recovery::OperationType type,
    const std::vector<std::string>& fields) {
    if (persistence_directory_.empty()) {
        return true;
    }

    if (!operation_log_.IsOpen()) {
        return false;
    }

    const std::string payload =
        recovery::OperationLog::EncodeFields(
            fields);

    if (!operation_log_.Append(
            type,
            payload)) {
        return false;
    }

    return operation_log_.Flush();
}

bool Master::CreateFile(
    const std::string& path,
    std::uint32_t replication_factor) {
    if (path.empty() ||
        namespace_manager_.Exists(path)) {
        return false;
    }

    const std::uint32_t factor =
        replication_factor == 0
            ? 3
            : replication_factor;

    if (!AppendOperation(
            recovery::OperationType::CreateFile,
            {path, std::to_string(factor)})) {
        return false;
    }

    if (!namespace_manager_.CreateFile(path)) {
        return false;
    }

    if (!metadata_.CreateFile(
            path,
            factor)) {
        static_cast<void>(
            namespace_manager_.DeleteFile(path));

        return false;
    }

    return true;
}

bool Master::DeleteFile(
    const std::string& path) {
    if (!namespace_manager_.IsFile(path)) {
        return false;
    }

    const auto chunks =
        metadata_.GetFileChunks(path);

    for (const ChunkHandle handle :
         chunks) {
        if (metadata_.GetChunkReferenceCount(handle) == 1) {
            static_cast<void>(
                orphan_chunk_manager_.MarkOrphan(handle));
        }
    }

    if (!AppendOperation(
            recovery::OperationType::DeleteFile,
            {path})) {
        return false;
    }

    if (!metadata_.DeleteFile(path)) {
        return false;
    }

    for (const ChunkHandle handle :
         chunks) {
        if (metadata_.GetChunkReferenceCount(handle) == 0) {
            static_cast<void>(
                replica_manager_.RemoveChunk(handle));

            static_cast<void>(
                lease_manager_.ReleaseLease(handle));
        }
    }

    return namespace_manager_.DeleteFile(path);
}

bool Master::RenameFile(
    const std::string& source_path,
    const std::string& destination_path) {
    if (!namespace_manager_.IsFile(source_path) ||
        namespace_manager_.Exists(destination_path)) {
        return false;
    }

    if (!AppendOperation(
            recovery::OperationType::RenameFile,
            {source_path, destination_path})) {
        return false;
    }

    if (!namespace_manager_.Rename(
            source_path,
            destination_path)) {
        return false;
    }

    if (!metadata_.RenameFile(
            source_path,
            destination_path)) {
        static_cast<void>(
            namespace_manager_.Rename(
                destination_path,
                source_path));

        return false;
    }

    return true;
}

bool Master::FileExists(
    const std::string& path) const {
    return namespace_manager_.Exists(path) &&
           namespace_manager_.IsFile(path);
}

bool Master::CreateDirectory(
    const std::string& path) {
    if (namespace_manager_.Exists(path)) {
        return false;
    }

    if (!AppendOperation(
            recovery::OperationType::CreateDirectory,
            {path})) {
        return false;
    }

    return namespace_manager_.CreateDirectory(
        path);
}

bool Master::DirectoryExists(
    const std::string& path) const {
    return namespace_manager_.Exists(path) &&
           namespace_manager_.IsDirectory(path);
}

std::optional<metadata::FileMetadata>
Master::GetFile(
    const std::string& path) const {
    return metadata_.GetFile(path);
}

std::optional<metadata::FileMetadata>
Master::GetFileInfo(
    const std::string& path) const {
    return metadata_.GetFile(path);
}

bool Master::CreateSnapshot(
    const std::string& source_path,
    const std::string& snapshot_path) {
    if (source_path.empty() ||
        snapshot_path.empty() ||
        source_path == snapshot_path ||
        !namespace_manager_.Exists(source_path) ||
        namespace_manager_.Exists(snapshot_path) ||
        namespace_management::NamespaceManager::IsRootPath(
            snapshot_path)) {
        return false;
    }

    if (!AppendOperation(
            recovery::OperationType::CreateSnapshot,
            {source_path, snapshot_path})) {
        return false;
    }

    return CreateSnapshotInternal(
        source_path,
        snapshot_path);
}

bool Master::CreateSnapshotInternal(
    const std::string& source_path,
    const std::string& snapshot_path) {
    const bool source_is_file =
        namespace_manager_.IsFile(source_path);

    const auto nodes =
        namespace_manager_.ExportNodes();

    std::vector<
        namespace_management::NamespaceManager::NodeInfo>
        affected_nodes;

    if (source_is_file) {
        const auto it =
            std::find_if(
                nodes.begin(),
                nodes.end(),
                [&source_path](
                    const auto& node) {
                    return node.path == source_path;
                });

        if (it == nodes.end()) {
            return false;
        }

        affected_nodes.push_back(*it);
    } else {
        const std::string prefix =
            source_path == "/"
                ? "/"
                : source_path + "/";

        for (const auto& node :
             nodes) {
            if (node.path == source_path ||
                (source_path == "/"
                     ? node.path != "/"
                     : node.path.rfind(
                           prefix,
                           0) == 0)) {
                affected_nodes.push_back(node);
            }
        }

        std::sort(
            affected_nodes.begin(),
            affected_nodes.end(),
            [](const auto& a,
               const auto& b) {
                const auto depth =
                    [](const std::string& path) {
                        return static_cast<std::size_t>(
                            std::count(
                                path.begin(),
                                path.end(),
                                '/'));
                    };

                const auto depth_a =
                    depth(a.path);
                const auto depth_b =
                    depth(b.path);

                if (depth_a != depth_b) {
                    return depth_a < depth_b;
                }

                return a.path < b.path;
            });
    }

    std::vector<std::string> created_files;
    std::vector<std::string> created_directories;

    auto destination_for =
        [&](const std::string& source) {
            if (source_is_file) {
                return snapshot_path;
            }

            if (source == source_path) {
                return snapshot_path;
            }

            const std::string prefix =
                source_path == "/"
                    ? "/"
                    : source_path + "/";

            const std::string suffix =
                source_path == "/"
                    ? source.substr(1)
                    : source.substr(prefix.size());

            return snapshot_path + "/" + suffix;
        };

    auto rollback =
        [&]() {
            for (auto it =
                     created_files.rbegin();
                 it != created_files.rend();
                 ++it) {
                static_cast<void>(
                    metadata_.DeleteFile(*it));

                static_cast<void>(
                    namespace_manager_.DeleteFile(
                        *it));
            }

            for (auto it =
                     created_directories.rbegin();
                 it != created_directories.rend();
                 ++it) {
                static_cast<void>(
                    namespace_manager_
                        .DeleteDirectory(*it));
            }
        };

    if (source_is_file) {
        const auto source_file =
            metadata_.GetFile(source_path);

        if (!source_file.has_value()) {
            return false;
        }

        if (!namespace_manager_.CreateFile(
                snapshot_path)) {
            return false;
        }

        created_files.push_back(
            snapshot_path);

        if (!metadata_.CloneFileMetadata(
                source_path,
                snapshot_path)) {
            rollback();
            return false;
        }

        for (const ChunkHandle handle :
             source_file->GetChunkHandles()) {
            static_cast<void>(
                lease_manager_.ReleaseLease(handle));
        }

        return true;
    }

    if (!namespace_manager_.CreateDirectory(
            snapshot_path)) {
        return false;
    }

    created_directories.push_back(
        snapshot_path);

    for (const auto& node :
         affected_nodes) {
        if (node.path == source_path) {
            continue;
        }

        const std::string destination =
            destination_for(node.path);

        if (node.type ==
            namespace_management::NamespaceManager::
                NodeType::Directory) {
            if (!namespace_manager_.CreateDirectory(
                    destination)) {
                rollback();
                return false;
            }

            created_directories.push_back(
                destination);
            continue;
        }

        const auto source_file =
            metadata_.GetFile(node.path);

        if (!source_file.has_value()) {
            rollback();
            return false;
        }

        if (!namespace_manager_.CreateFile(
                destination)) {
            rollback();
            return false;
        }

        created_files.push_back(
            destination);

        if (!metadata_.CloneFileMetadata(
                node.path,
                destination)) {
            rollback();
            return false;
        }

        for (const ChunkHandle handle :
             source_file->GetChunkHandles()) {
            static_cast<void>(
                lease_manager_.ReleaseLease(handle));
        }
    }

    return true;
}

std::optional<ChunkHandle>
Master::PrepareCopyOnWrite(
    const std::string& path,
    ChunkIndex chunk_index,
    const ChunkCloneFunction& clone_function) {
    if (path.empty() ||
        !metadata_.FileExists(path)) {
        return std::nullopt;
    }

    const auto chunks =
        metadata_.GetFileChunks(path);

    if (chunk_index >= chunks.size()) {
        return std::nullopt;
    }

    const std::size_t index =
        static_cast<std::size_t>(
            chunk_index);

    const ChunkHandle source_handle =
        chunks[index];

    if (source_handle == 0) {
        return std::nullopt;
    }

    const std::size_t references =
        metadata_.GetChunkReferenceCount(
            source_handle);

    if (references <= 1) {
        return source_handle;
    }

    const auto source_chunk =
        metadata_.GetChunk(source_handle);

    if (!source_chunk.has_value()) {
        return std::nullopt;
    }

    const auto replicas =
        replica_manager_.GetReplicaServers(
            source_handle);

    const ChunkHandle destination_handle =
        metadata_.GetNextChunkHandle();

    if (destination_handle == 0 ||
        !metadata_.AllocateStandaloneChunk(
            destination_handle,
            source_chunk->version,
            source_chunk->size)) {
        return std::nullopt;
    }

    bool registered = true;
    bool primary_registered = false;

    for (const ServerId server_id :
         replicas) {
        const bool is_primary =
            !primary_registered;

        if (!replica_manager_.RegisterReplica(
                destination_handle,
                server_id,
                is_primary)) {
            registered = false;
            break;
        }

        primary_registered |= is_primary;
    }

    if (!registered ||
        (!replicas.empty() &&
         !primary_registered)) {
        static_cast<void>(
            replica_manager_.RemoveChunk(
                destination_handle));

        static_cast<void>(
            metadata_.DeleteChunk(
                destination_handle));

        return std::nullopt;
    }

    static_cast<void>(
        lease_manager_.ReleaseLease(
            source_handle));

    if (clone_function &&
        !clone_function(
            source_handle,
            destination_handle,
            replicas)) {
        static_cast<void>(
            replica_manager_.RemoveChunk(
                destination_handle));

        static_cast<void>(
            metadata_.DeleteChunk(
                destination_handle));

        return std::nullopt;
    }

    /*
     * IMPORTANT:
     *
     * Do not use AddChunkToFile() followed by
     * RemoveChunkFromFile() here.
     *
     * COW must replace the exact chunk slot.
     *
     * Example:
     *
     *   [chunk0, chunk1, chunk2]
     *
     * COW on chunk1 must produce:
     *
     *   [chunk0, new_chunk, chunk2]
     *
     * and never:
     *
     *   [chunk0, chunk2, new_chunk]
     *
     * This is required for multiple independent
     * shared chunks and preserves chunk indexes.
     */
    if (!metadata_.ReplaceChunkInFile(
            path,
            chunk_index,
            destination_handle)) {
        static_cast<void>(
            replica_manager_.RemoveChunk(
                destination_handle));

        static_cast<void>(
            metadata_.DeleteChunk(
                destination_handle));

        return std::nullopt;
    }

    if (!AppendOperation(
            recovery::OperationType::CopyOnWrite,
            {path,
             std::to_string(chunk_index),
             std::to_string(source_handle),
             std::to_string(destination_handle),
             std::to_string(source_chunk->version),
             std::to_string(source_chunk->size)})) {
        /*
         * Restore the original chunk at the exact
         * same index if persistence fails.
         */
        static_cast<void>(
            metadata_.ReplaceChunkInFile(
                path,
                chunk_index,
                source_handle));

        static_cast<void>(
            replica_manager_.RemoveChunk(
                destination_handle));

        static_cast<void>(
            metadata_.DeleteChunk(
                destination_handle));

        return std::nullopt;
    }

    return destination_handle;
}

std::size_t Master::GetChunkReferenceCount(
    ChunkHandle handle) const {
    return metadata_.GetChunkReferenceCount(handle);
}

std::optional<metadata::ChunkMetadata>
Master::GetChunkInfo(
    ChunkHandle handle) const {
    return metadata_.GetChunk(handle);
}

std::optional<std::uint32_t>
Master::GetChunkReplicationFactor(
    ChunkHandle handle) const {
    return metadata_.GetChunkReplicationFactor(handle);
}

bool Master::SetChunkVersion(
    ChunkHandle handle,
    ChunkVersion version) {
    if (!metadata_.ChunkExists(handle) ||
        version == 0) {
        return false;
    }

    if (!AppendOperation(
            recovery::OperationType::SetChunkVersion,
            {std::to_string(handle),
             std::to_string(version)})) {
        return false;
    }

    return metadata_.SetChunkVersion(
        handle,
        version);
}

std::optional<ChunkHandle>
Master::AllocateChunk(
    const std::string& path) {
    if (!metadata_.FileExists(path)) {
        return std::nullopt;
    }

    const ChunkHandle handle =
        metadata_.GetNextChunkHandle();

    if (handle == 0) {
        return std::nullopt;
    }

    if (!AppendOperation(
            recovery::OperationType::AllocateChunk,
            {path,
             std::to_string(handle),
             "1",
             "0"})) {
        return std::nullopt;
    }

    if (!metadata_.AllocateChunk(
            path,
            handle,
            1,
            0)) {
        return std::nullopt;
    }

    return handle;
}

bool Master::AddChunkToFile(
    const std::string& path,
    ChunkHandle handle) {
    if (!metadata_.FileExists(path) ||
        !metadata_.ChunkExists(handle)) {
        return false;
    }

    if (!AppendOperation(
            recovery::OperationType::AddChunkToFile,
            {path,
             std::to_string(handle)})) {
        return false;
    }

    return metadata_.AddChunkToFile(
        path,
        handle);
}

bool Master::RemoveChunkFromFile(
    const std::string& path,
    ChunkHandle handle) {
    if (!metadata_.FileExists(path) ||
        !metadata_.ChunkExists(handle)) {
        return false;
    }

    if (!AppendOperation(
            recovery::OperationType::RemoveChunkFromFile,
            {path,
             std::to_string(handle)})) {
        return false;
    }

    const bool removed =
        metadata_.RemoveChunkFromFile(
            path,
            handle);

    if (removed &&
        metadata_.GetChunkReferenceCount(handle) == 0) {
        static_cast<void>(
            orphan_chunk_manager_.MarkOrphan(handle));
    }

    return removed;
}

std::vector<ChunkHandle>
Master::GetFileChunks(
    const std::string& path) const {
    return metadata_.GetFileChunks(path);
}

std::optional<std::size_t>
Master::GetChunkCount(
    const std::string& path) const {
    return metadata_.GetChunkCount(path);
}

std::size_t Master::ChunkCount() const {
    return metadata_.ChunkCount();
}

metadata::Metadata&
Master::GetMetadata() noexcept {
    return metadata_;
}

const metadata::Metadata&
Master::GetMetadata() const noexcept {
    return metadata_;
}

garbage_collection::OrphanChunkManager&
Master::GetOrphanChunkManager() noexcept {
    return orphan_chunk_manager_;
}

const garbage_collection::OrphanChunkManager&
Master::GetOrphanChunkManager() const noexcept {
    return orphan_chunk_manager_;
}

garbage_collection::GarbageCollector&
Master::GetGarbageCollector() noexcept {
    return garbage_collector_;
}

const garbage_collection::GarbageCollector&
Master::GetGarbageCollector() const noexcept {
    return garbage_collector_;
}

bool Master::GarbageCollectChunk(
    ChunkHandle handle) {
    if (handle == 0 ||
        metadata_.GetChunkReferenceCount(handle) != 0) {
        return false;
    }

    if (!metadata_.ChunkExists(handle)) {
        static_cast<void>(
            orphan_chunk_manager_.RemoveOrphan(handle));
        return true;
    }

    if (!AppendOperation(
            recovery::OperationType::DeleteChunk,
            {std::to_string(handle)})) {
        return false;
    }

    if (!re_replication_manager_.DeleteChunkFromChunkservers(
            handle)) {
        return false;
    }

    static_cast<void>(
        replica_manager_.RemoveChunk(handle));

    static_cast<void>(
        lease_manager_.ReleaseLease(handle));

    if (!metadata_.DeleteChunk(handle)) {
        return false;
    }

    static_cast<void>(
        orphan_chunk_manager_.RemoveOrphan(handle));

    return true;
}

std::vector<ChunkHandle>
Master::GetAllChunkHandles() const {
    const auto chunks =
        metadata_.ExportChunks();

    std::vector<ChunkHandle> handles;
    handles.reserve(chunks.size());

    for (const auto& chunk :
         chunks) {
        handles.push_back(chunk.handle);
    }

    return handles;
}

std::size_t Master::NamespaceNodeCount() const {
    return namespace_manager_.NodeCount();
}

std::size_t Master::FileCount() const {
    return metadata_.FileCount();
}

namespace_management::NamespaceManager&
Master::GetNamespaceManager() noexcept {
    return namespace_manager_;
}

const namespace_management::NamespaceManager&
Master::GetNamespaceManager() const noexcept {
    return namespace_manager_;
}

replication::ReplicaManager&
Master::GetReplicaManager() noexcept {
    return replica_manager_;
}

const replication::ReplicaManager&
Master::GetReplicaManager() const noexcept {
    return replica_manager_;
}

replication::PlacementPolicy&
Master::GetPlacementPolicy() noexcept {
    return placement_policy_;
}

const replication::PlacementPolicy&
Master::GetPlacementPolicy() const noexcept {
    return placement_policy_;
}

lease::LeaseManager&
Master::GetLeaseManager() noexcept {
    return lease_manager_;
}

const lease::LeaseManager&
Master::GetLeaseManager() const noexcept {
    return lease_manager_;
}

heartbeat::HeartbeatManager&
Master::GetHeartbeatManager() noexcept {
    return heartbeat_manager_;
}

const heartbeat::HeartbeatManager&
Master::GetHeartbeatManager() const noexcept {
    return heartbeat_manager_;
}

replication::ReReplicationManager&
Master::GetReReplicationManager() noexcept {
    return re_replication_manager_;
}

const replication::ReReplicationManager&
Master::GetReReplicationManager() const noexcept {
    return re_replication_manager_;
}

recovery::RecoveryManager&
Master::GetRecoveryManager() noexcept {
    return recovery_manager_;
}

const recovery::RecoveryManager&
Master::GetRecoveryManager() const noexcept {
    return recovery_manager_;
}

bool Master::Initialize() {
    if (persistence_directory_.empty()) {
        initialized_ = true;
        return true;
    }

    if (!operation_log_.IsOpen()) {
        if (!operation_log_.Open(
                persistence_directory_ /
                "operation.log")) {
            return false;
        }
    }

    if (!operation_log_.Validate()) {
        return false;
    }

    recovery::Checkpoint::State state;

    std::uint64_t checkpoint_sequence = 0;

    if (checkpoint_.LoadLatest(state)) {
        checkpoint_sequence =
            state.sequence;

        if (checkpoint_sequence >
            operation_log_.LastSequence()) {
            return false;
        }

        if (!checkpoint_.Restore(
                state,
                metadata_,
                namespace_manager_)) {
            return false;
        }
    } else {
        metadata_.Clear();
        namespace_manager_.Clear();
    }

    const auto records =
        operation_log_.Replay(
            checkpoint_sequence);

    for (const auto& record :
         records) {
        if (!ReplayOperation(record)) {
            return false;
        }
    }

    replica_manager_.Clear();
    orphan_chunk_manager_.Clear();

    initialized_ = true;
    return true;
}

bool Master::CreateCheckpoint() {
    if (persistence_directory_.empty() ||
        !operation_log_.IsOpen()) {
        return false;
    }

    return checkpoint_.Create(
        metadata_,
        namespace_manager_,
        operation_log_.LastSequence());
}

std::uint64_t
Master::GetLastOperationSequence()
    const noexcept {
    return operation_log_.LastSequence();
}

const std::filesystem::path&
Master::GetPersistenceDirectory()
    const noexcept {
    return persistence_directory_;
}

const std::filesystem::path&
Master::GetOperationLogPath()
    const noexcept {
    return operation_log_.GetPath();
}

bool Master::ReplayCopyOnWrite(
    const std::vector<std::string>& fields) {
    if (fields.size() != 6) {
        return false;
    }

    std::uint64_t chunk_index = 0;
    std::uint64_t source_handle = 0;
    std::uint64_t destination_handle = 0;
    std::uint64_t version = 0;
    std::uint64_t size = 0;

    try {
        std::size_t position = 0;

        chunk_index =
            std::stoull(fields[1], &position);

        if (position != fields[1].size()) {
            return false;
        }

        source_handle =
            std::stoull(fields[2], &position);

        if (position != fields[2].size()) {
            return false;
        }

        destination_handle =
            std::stoull(fields[3], &position);

        if (position != fields[3].size()) {
            return false;
        }

        version =
            std::stoull(fields[4], &position);

        if (position != fields[4].size()) {
            return false;
        }

        size =
            std::stoull(fields[5], &position);

        if (position != fields[5].size()) {
            return false;
        }
    } catch (...) {
        return false;
    }

    if (source_handle == 0 ||
        destination_handle == 0 ||
        version == 0 ||
        source_handle == destination_handle) {
        return false;
    }

    const auto chunks =
        metadata_.GetFileChunks(fields[0]);

    if (chunk_index >= chunks.size()) {
        return false;
    }

    if (chunks[
            static_cast<std::size_t>(
                chunk_index)] ==
        destination_handle) {
        return true;
    }

    if (chunks[
            static_cast<std::size_t>(
                chunk_index)] !=
        source_handle) {
        return false;
    }

    if (!metadata_.ChunkExists(
            destination_handle)) {
        if (!metadata_.AllocateStandaloneChunk(
                destination_handle,
                static_cast<ChunkVersion>(
                    version),
                size)) {
            return false;
        }
    }

    return metadata_.ReplaceChunkInFile(
        fields[0],
        static_cast<ChunkIndex>(
            chunk_index),
        static_cast<ChunkHandle>(
            destination_handle));
}

bool Master::ReplayOperation(
    const recovery::OperationRecord& record) {
    std::vector<std::string> fields;

    if (!recovery::OperationLog::DecodeFields(
            record.payload,
            fields)) {
        return false;
    }

    auto parse_u64 =
        [](const std::string& value,
           std::uint64_t& result) {
            try {
                std::size_t position = 0;

                result =
                    std::stoull(
                        value,
                        &position);

                return position ==
                       value.size();
            } catch (...) {
                return false;
            }
        };

    switch (record.type) {
    case recovery::OperationType::CreateDirectory: {
        if (fields.size() != 1) {
            return false;
        }

        if (namespace_manager_.Exists(
                fields[0])) {
            return namespace_manager_.IsDirectory(
                fields[0]);
        }

        return namespace_manager_.CreateDirectory(
            fields[0]);
    }

    case recovery::OperationType::CreateFile: {
        if (fields.size() != 2) {
            return false;
        }

        std::uint64_t factor = 0;

        if (!parse_u64(
                fields[1],
                factor) ||
            factor == 0 ||
            factor >
                std::numeric_limits<
                    std::uint32_t>::max()) {
            return false;
        }

        if (namespace_manager_.Exists(
                fields[0])) {
            return namespace_manager_.IsFile(
                       fields[0]) &&
                   metadata_.FileExists(
                       fields[0]);
        }

        if (!namespace_manager_.CreateFile(
                fields[0])) {
            return false;
        }

        return metadata_.CreateFile(
            fields[0],
            static_cast<std::uint32_t>(
                factor));
    }

    case recovery::OperationType::DeleteFile: {
        if (fields.size() != 1) {
            return false;
        }

        const bool metadata_exists =
            metadata_.FileExists(fields[0]);

        const bool namespace_exists =
            namespace_manager_.IsFile(
                fields[0]);

        if (!metadata_exists &&
            !namespace_exists) {
            return true;
        }

        if (metadata_exists &&
            !metadata_.DeleteFile(
                fields[0])) {
            return false;
        }

        if (namespace_exists &&
            !namespace_manager_.DeleteFile(
                fields[0])) {
            return false;
        }

        return true;
    }

    case recovery::OperationType::RenameFile: {
        if (fields.size() != 2) {
            return false;
        }

        if (!namespace_manager_.IsFile(
                fields[0])) {
            return false;
        }

        if (!namespace_manager_.Rename(
                fields[0],
                fields[1])) {
            return false;
        }

        return metadata_.RenameFile(
            fields[0],
            fields[1]);
    }

    case recovery::OperationType::AllocateChunk: {
        if (fields.size() != 4) {
            return false;
        }

        std::uint64_t handle = 0;
        std::uint64_t version = 0;
        std::uint64_t size = 0;

        if (!parse_u64(
                fields[1],
                handle) ||
            !parse_u64(
                fields[2],
                version) ||
            !parse_u64(
                fields[3],
                size) ||
            handle == 0 ||
            version == 0) {
            return false;
        }

        if (metadata_.ChunkExists(
                static_cast<ChunkHandle>(
                    handle))) {
            return true;
        }

        return metadata_.AllocateChunk(
            fields[0],
            static_cast<ChunkHandle>(
                handle),
            static_cast<ChunkVersion>(
                version),
            size);
    }

    case recovery::OperationType::UpdateFileSize: {
        if (fields.size() != 2) {
            return false;
        }

        std::uint64_t size = 0;

        if (!parse_u64(
                fields[1],
                size)) {
            return false;
        }

        return metadata_.UpdateFileSize(
            fields[0],
            size);
    }

    case recovery::OperationType::SetChunkVersion: {
        if (fields.size() != 2) {
            return false;
        }

        std::uint64_t handle = 0;
        std::uint64_t version = 0;

        if (!parse_u64(
                fields[0],
                handle) ||
            !parse_u64(
                fields[1],
                version) ||
            handle == 0 ||
            version == 0) {
            return false;
        }

        return metadata_.SetChunkVersion(
            static_cast<ChunkHandle>(
                handle),
            static_cast<ChunkVersion>(
                version));
    }

    case recovery::OperationType::AddChunkToFile: {
        if (fields.size() != 2) {
            return false;
        }

        std::uint64_t handle = 0;

        if (!parse_u64(
                fields[1],
                handle) ||
            handle == 0) {
            return false;
        }

        if (!metadata_.ChunkExists(
                static_cast<ChunkHandle>(
                    handle))) {
            return false;
        }

        const auto chunks =
            metadata_.GetFileChunks(
                fields[0]);

        if (std::find(
                chunks.begin(),
                chunks.end(),
                static_cast<ChunkHandle>(
                    handle)) != chunks.end()) {
            return true;
        }

        return metadata_.AddChunkToFile(
            fields[0],
            static_cast<ChunkHandle>(
                handle));
    }

    case recovery::OperationType::
        RemoveChunkFromFile: {
        if (fields.size() != 2) {
            return false;
        }

        std::uint64_t handle = 0;

        if (!parse_u64(
                fields[1],
                handle) ||
            handle == 0) {
            return false;
        }

        const auto chunks =
            metadata_.GetFileChunks(
                fields[0]);

        if (std::find(
                chunks.begin(),
                chunks.end(),
                static_cast<ChunkHandle>(
                    handle)) == chunks.end()) {
            return true;
        }

        return metadata_.RemoveChunkFromFile(
            fields[0],
            static_cast<ChunkHandle>(
                handle));
    }

    case recovery::OperationType::DeleteChunk: {
        if (fields.size() != 1) {
            return false;
        }

        std::uint64_t handle = 0;

        if (!parse_u64(
                fields[0],
                handle) ||
            handle == 0) {
            return false;
        }

        if (!metadata_.ChunkExists(
                static_cast<ChunkHandle>(
                    handle))) {
            return true;
        }

        return metadata_.DeleteChunk(
            static_cast<ChunkHandle>(
                handle));
    }

    case recovery::OperationType::SetChunkSize: {
        if (fields.size() != 2) {
            return false;
        }

        std::uint64_t handle = 0;
        std::uint64_t size = 0;

        if (!parse_u64(
                fields[0],
                handle) ||
            !parse_u64(
                fields[1],
                size) ||
            handle == 0) {
            return false;
        }

        return metadata_.SetChunkSize(
            static_cast<ChunkHandle>(
                handle),
            size);
    }

    case recovery::OperationType::CreateSnapshot: {
        if (fields.size() != 2) {
            return false;
        }

        if (namespace_manager_.Exists(
                fields[1])) {
            return true;
        }

        return CreateSnapshotInternal(
            fields[0],
            fields[1]);
    }

    case recovery::OperationType::CopyOnWrite: {
        return ReplayCopyOnWrite(fields);
    }
    }

    return false;
}

bool Master::RegisterChunkReplica(
    ChunkHandle handle,
    ServerId server_id,
    bool is_primary) {
    return RegisterReplica(
        handle,
        server_id,
        is_primary);
}

bool Master::HasChunkReplica(
    ChunkHandle handle,
    ServerId server_id) const {
    return HasReplica(
        handle,
        server_id);
}

std::optional<ServerId>
Master::GetChunkPrimary(
    ChunkHandle handle) const {
    return GetPrimary(handle);
}

std::vector<ServerId>
Master::PlaceChunkReplicas(
    ChunkHandle handle,
    const std::vector<
        replication::PlacementCandidate>& candidates) {
    const auto servers =
        SelectReplicaServers(
            handle,
            candidates);

    if (servers.empty()) {
        return {};
    }

    bool primary_registered = false;

    for (const ServerId server_id :
         servers) {
        const bool is_primary =
            !primary_registered;

        if (!RegisterReplica(
                handle,
                server_id,
                is_primary)) {
            return {};
        }

        if (is_primary) {
            primary_registered = true;
        }
    }

    if (!primary_registered) {
        return {};
    }

    return servers;
}

bool Master::SetChunkPrimary(
    ChunkHandle handle,
    ServerId server_id) {
    return SetPrimary(
        handle,
        server_id);
}

bool Master::AddReplica(
    ChunkHandle handle,
    ServerId server_id) {
    if (!metadata_.AddReplica(
            handle,
            server_id)) {
        return false;
    }

    if (!replica_manager_.RegisterReplica(
            handle,
            server_id,
            false)) {
        static_cast<void>(
            metadata_.RemoveReplica(
                handle,
                server_id));

        return false;
    }

    return true;
}

std::vector<ServerId>
Master::GetChunkReplicas(
    ChunkHandle handle) const {
    return replica_manager_
        .GetReplicaServers(handle);
}

bool Master::RegisterReplica(
    ChunkHandle handle,
    ServerId server_id,
    bool is_primary) {
    return replica_manager_.RegisterReplica(
        handle,
        server_id,
        is_primary);
}

bool Master::RemoveReplica(
    ChunkHandle handle,
    ServerId server_id) {
    const bool removed =
        replica_manager_.RemoveReplica(
            handle,
            server_id);

    if (removed) {
        static_cast<void>(
            metadata_.RemoveReplica(
                handle,
                server_id));

        const auto primary =
            replica_manager_.GetPrimary(handle);

        const auto lease =
            lease_manager_.GetLease(handle);

        if (lease.has_value() &&
            (!primary.has_value() ||
             *primary !=
                 lease->primary_server_id)) {
            static_cast<void>(
                lease_manager_.ReleaseLease(
                    handle));
        }
    }

    return removed;
}

bool Master::HasReplica(
    ChunkHandle handle,
    ServerId server_id) const {
    return replica_manager_.HasReplica(
        handle,
        server_id);
}

std::vector<ServerId>
Master::GetReplicaServers(
    ChunkHandle handle) const {
    return replica_manager_
        .GetReplicaServers(handle);
}

std::optional<ServerId>
Master::GetPrimary(
    ChunkHandle handle) const {
    return replica_manager_
        .GetPrimary(handle);
}

bool Master::SetPrimary(
    ChunkHandle handle,
    ServerId server_id) {
    if (!replica_manager_.SetPrimary(
            handle,
            server_id)) {
        return false;
    }

    const auto lease =
        lease_manager_.GetLease(handle);

    if (lease.has_value() &&
        lease->primary_server_id !=
            server_id) {
        static_cast<void>(
            lease_manager_.ReleaseLease(handle));
    }

    return true;
}

std::vector<ServerId>
Master::SelectReplicaServers(
    ChunkHandle handle,
    const std::vector<
        replication::PlacementCandidate>& candidates)
    const {
    return placement_policy_.SelectReplicas(
        handle,
        candidates);
}

std::optional<lease::Lease>
Master::AcquireLease(
    ChunkHandle handle,
    ServerId primary_server_id) {
    return lease_manager_.AcquireLease(
        handle,
        primary_server_id);
}

std::optional<lease::Lease>
Master::GetLease(
    ChunkHandle handle) const {
    return lease_manager_.GetLease(
        handle);
}

bool Master::IsLeaseValid(
    ChunkHandle handle) const {
    return lease_manager_.IsLeaseValid(
        handle);
}

bool Master::IsLeaseValid(
    ChunkHandle handle,
    ServerId primary_server_id) const {
    return lease_manager_.IsLeaseValid(
        handle,
        primary_server_id);
}

bool Master::ExtendLease(
    ChunkHandle handle,
    ServerId primary_server_id) {
    return lease_manager_.ExtendLease(
        handle,
        primary_server_id);
}

bool Master::ExtendLease(
    ChunkHandle handle,
    ServerId primary_server_id,
    std::uint64_t extension_ms) {
    return lease_manager_.ExtendLease(
        handle,
        primary_server_id,
        extension_ms);
}

bool Master::ReleaseLease(
    ChunkHandle handle) {
    return lease_manager_.ReleaseLease(
        handle);
}

bool Master::ProcessHeartbeat(
    ServerId server_id,
    std::uint64_t timestamp_ms,
    const std::vector<
        heartbeat::ReportedChunk>& chunks) {
    if (!heartbeat_manager_.ProcessHeartbeat(
            server_id,
            timestamp_ms,
            chunks)) {
        return false;
    }

    for (const auto& chunk : chunks) {
        if (chunk.handle == 0 ||
            chunk.version == 0) {
            continue;
        }

        if (!replica_manager_.HasReplica(
                chunk.handle,
                server_id)) {
            static_cast<void>(
                replica_manager_.RegisterReplica(
                    chunk.handle,
                    server_id,
                    false));
        }
    }

    return true;
}

bool Master::ProcessHeartbeat(
    ServerId server_id,
    std::uint64_t timestamp_ms) {
    return heartbeat_manager_.ProcessHeartbeat(
        server_id,
        timestamp_ms);
}

bool Master::IsChunkserverAlive(
    ServerId server_id) const {
    return heartbeat_manager_
        .IsServerAlive(server_id);
}

bool Master::IsChunkserverAlive(
    ServerId server_id,
    std::uint64_t now_ms) const {
    return heartbeat_manager_
        .IsServerAlive(
            server_id,
            now_ms);
}

std::vector<ServerId>
Master::DetectFailedChunkservers(
    std::uint64_t now_ms) {
    return heartbeat_manager_
        .DetectFailedServers(now_ms);
}

bool Master::IsStaleReplica(
    ChunkHandle handle,
    ServerId server_id) const {
    if (handle == 0 ||
        server_id == 0) {
        return false;
    }

    const auto master_version =
        metadata_.GetChunkVersion(handle);

    if (!master_version.has_value() ||
        *master_version == 0) {
        return false;
    }

    const auto state =
        heartbeat_manager_
            .GetServerState(server_id);

    if (!state.has_value()) {
        return false;
    }

    const auto reported_chunks =
        state->GetReportedChunks();

    const auto it =
        std::find_if(
            reported_chunks.begin(),
            reported_chunks.end(),
            [handle](
                const heartbeat::ReportedChunk& chunk) {
                return chunk.handle == handle;
            });

    if (it == reported_chunks.end()) {
        return false;
    }

    return it->version < *master_version;
}

std::vector<ServerId>
Master::GetStaleReplicas(
    ChunkHandle handle) const {
    std::vector<ServerId> stale;

    if (handle == 0) {
        return stale;
    }

    const auto master_version =
        metadata_.GetChunkVersion(handle);

    if (!master_version.has_value() ||
        *master_version == 0) {
        return stale;
    }

    const auto replicas =
        replica_manager_
            .GetReplicaServers(handle);

    for (const ServerId server_id :
         replicas) {
        if (IsStaleReplica(
                handle,
                server_id)) {
            stale.push_back(server_id);
        }
    }

    std::sort(
        stale.begin(),
        stale.end());

    return stale;
}

std::vector<ChunkHandle>
Master::GetStaleChunks(
    ServerId server_id) const {
    std::vector<ChunkHandle> stale;

    if (server_id == 0) {
        return stale;
    }

    const auto state =
        heartbeat_manager_
            .GetServerState(server_id);

    if (!state.has_value()) {
        return stale;
    }

    const auto reported_chunks =
        state->GetReportedChunks();

    for (const auto& reported :
         reported_chunks) {
        if (IsStaleReplica(
                reported.handle,
                server_id)) {
            stale.push_back(
                reported.handle);
        }
    }

    std::sort(
        stale.begin(),
        stale.end());

    return stale;
}

replication::Rebalancer&
Master::GetRebalancer() noexcept {
    return rebalancer_;
}

const replication::Rebalancer&
Master::GetRebalancer() const noexcept {
    return rebalancer_;
}

}  // namespace gfs::master