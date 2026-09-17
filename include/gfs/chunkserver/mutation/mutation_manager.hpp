#pragma once

#include "gfs/chunkserver/mutation/mutation.hpp"

#include <cstddef>
#include <cstdint>
#include <functional>
#include <mutex>
#include <optional>
#include <unordered_map>

namespace gfs::chunkserver {

class Chunkserver;

namespace mutation {

class MutationManager {
public:
    using PropagationFunction =
        std::function<bool(const Mutation&)>;

    explicit MutationManager(
        Chunkserver& chunkserver);

    MutationManager(const MutationManager&) = delete;
    MutationManager& operator=(const MutationManager&) = delete;

    [[nodiscard]] std::optional<Mutation>
    CreatePrimaryMutation(
        ChunkHandle handle,
        ChunkVersion version,
        std::uint64_t offset,
        const std::string& data);

    [[nodiscard]] bool ApplyMutation(
        const Mutation& mutation);

    [[nodiscard]] bool ExecutePrimaryMutation(
        ChunkHandle handle,
        ChunkVersion version,
        std::uint64_t offset,
        const std::string& data,
        const PropagationFunction&
            propagation_function = {});

    [[nodiscard]] std::uint64_t
    LastAppliedMutationId(
        ChunkHandle handle) const;

    [[nodiscard]] std::uint64_t
    NextMutationId(
        ChunkHandle handle) const;

    [[nodiscard]] bool HasAppliedMutations(
        ChunkHandle handle) const;

    [[nodiscard]] bool ResetChunk(
        ChunkHandle handle);

    [[nodiscard]] std::size_t TrackedChunkCount()
        const;

    void Clear();

private:
    [[nodiscard]] std::uint64_t
    AssignMutationIdLocked(
        ChunkHandle handle);

    [[nodiscard]] bool ApplyMutationLocked(
        const Mutation& mutation);

    Chunkserver& chunkserver_;

    mutable std::mutex mutex_;

    std::unordered_map<
        ChunkHandle,
        std::uint64_t>
        next_mutation_ids_;

    std::unordered_map<
        ChunkHandle,
        std::uint64_t>
        last_applied_mutation_ids_;
};

}  // namespace mutation
}  // namespace gfs::chunkserver