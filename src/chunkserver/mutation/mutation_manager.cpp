#include "gfs/chunkserver/mutation/mutation_manager.hpp"

#include "gfs/chunkserver/chunkserver.hpp"

#include <limits>
#include <utility>

namespace gfs::chunkserver::mutation {

MutationManager::MutationManager(
    Chunkserver& chunkserver)
    : chunkserver_(chunkserver) {
}

std::optional<Mutation>
MutationManager::CreatePrimaryMutation(
    ChunkHandle handle,
    ChunkVersion version,
    std::uint64_t offset,
    const std::string& data) {
    Mutation mutation;
    mutation.chunk_handle = handle;
    mutation.chunk_version = version;
    mutation.offset = offset;
    mutation.data = data;

    if (handle == 0 ||
        version == 0 ||
        !mutation.FitsChunk()) {
        return std::nullopt;
    }

    std::lock_guard lock(mutex_);

    mutation.mutation_id =
        AssignMutationIdLocked(handle);

    if (mutation.mutation_id == 0) {
        return std::nullopt;
    }

    return mutation;
}

bool MutationManager::ApplyMutation(
    const Mutation& mutation) {
    if (!mutation.IsValid()) {
        return false;
    }

    std::lock_guard lock(mutex_);

    return ApplyMutationLocked(mutation);
}

bool MutationManager::ExecutePrimaryMutation(
    ChunkHandle handle,
    ChunkVersion version,
    std::uint64_t offset,
    const std::string& data,
    const PropagationFunction&
        propagation_function) {
    Mutation mutation;
    mutation.chunk_handle = handle;
    mutation.chunk_version = version;
    mutation.offset = offset;
    mutation.data = data;

    if (handle == 0 ||
        version == 0 ||
        !mutation.FitsChunk()) {
        return false;
    }

    {
        std::lock_guard lock(mutex_);

        const std::uint64_t next_id =
            AssignMutationIdLocked(handle);

        if (next_id == 0) {
            return false;
        }

        mutation.mutation_id = next_id;

        if (!ApplyMutationLocked(mutation)) {
            return false;
        }
    }

    if (propagation_function) {
        return propagation_function(mutation);
    }

    return true;
}

std::uint64_t
MutationManager::LastAppliedMutationId(
    ChunkHandle handle) const {
    if (handle == 0) {
        return 0;
    }

    std::lock_guard lock(mutex_);

    const auto it =
        last_applied_mutation_ids_.find(handle);

    if (it == last_applied_mutation_ids_.end()) {
        return 0;
    }

    return it->second;
}

std::uint64_t
MutationManager::NextMutationId(
    ChunkHandle handle) const {
    if (handle == 0) {
        return 0;
    }

    std::lock_guard lock(mutex_);

    const auto it =
        next_mutation_ids_.find(handle);

    if (it == next_mutation_ids_.end()) {
        return 1;
    }

    return it->second;
}

bool MutationManager::HasAppliedMutations(
    ChunkHandle handle) const {
    return LastAppliedMutationId(handle) != 0;
}

bool MutationManager::ResetChunk(
    ChunkHandle handle) {
    if (handle == 0) {
        return false;
    }

    std::lock_guard lock(mutex_);

    const bool removed_next =
        next_mutation_ids_.erase(handle) > 0;

    const bool removed_last =
        last_applied_mutation_ids_.erase(handle) > 0;

    return removed_next || removed_last;
}

std::size_t MutationManager::TrackedChunkCount()
    const {
    std::lock_guard lock(mutex_);

    return last_applied_mutation_ids_.size();
}

void MutationManager::Clear() {
    std::lock_guard lock(mutex_);

    next_mutation_ids_.clear();
    last_applied_mutation_ids_.clear();
}

std::uint64_t
MutationManager::AssignMutationIdLocked(
    ChunkHandle handle) {
    if (handle == 0) {
        return 0;
    }

    auto& next_id =
        next_mutation_ids_[handle];

    if (next_id == 0) {
        next_id = 1;
    }

    const std::uint64_t assigned_id = next_id;

    if (next_id ==
        std::numeric_limits<std::uint64_t>::max()) {
        next_id = 0;
    } else {
        ++next_id;
    }

    return assigned_id;
}

bool MutationManager::ApplyMutationLocked(
    const Mutation& mutation) {
    if (!mutation.IsValid()) {
        return false;
    }

    const std::uint64_t expected_id =
        last_applied_mutation_ids_.contains(
            mutation.chunk_handle)
            ? last_applied_mutation_ids_.at(
                  mutation.chunk_handle) +
                  1
            : 1;

    if (mutation.mutation_id != expected_id) {
        return false;
    }

    if (!chunkserver_.ChunkExists(
            mutation.chunk_handle)) {
        if (!chunkserver_.CreateChunk(
                mutation.chunk_handle)) {
            return false;
        }
    }

    if (!chunkserver_.WriteChunk(
            mutation.chunk_handle,
            mutation.offset,
            mutation.data)) {
        return false;
    }

    last_applied_mutation_ids_[
        mutation.chunk_handle] =
        mutation.mutation_id;

    auto& next_id =
        next_mutation_ids_[mutation.chunk_handle];

    if (next_id <= mutation.mutation_id) {
        if (mutation.mutation_id ==
            std::numeric_limits<
                std::uint64_t>::max()) {
            next_id = 0;
        } else {
            next_id =
                mutation.mutation_id + 1;
        }
    }

    return true;
}

}  // namespace gfs::chunkserver::mutation