#pragma once

#include "gfs/master/metadata/metadata.hpp"
#include "gfs/master/namespace/namespace_manager.hpp"

#include <cstdint>
#include <filesystem>
#include <string>
#include <vector>

namespace gfs::master::recovery {

class Checkpoint {
public:
    struct FileState {
        std::string path;
        std::uint64_t size = 0;
        std::uint32_t replication_factor = 3;
        std::vector<ChunkHandle> chunk_handles;
    };

    struct ChunkState {
        ChunkHandle handle = 0;
        ChunkVersion version = 1;
        std::uint64_t size = 0;
    };

    struct State {
        std::uint64_t sequence = 0;

        std::vector<
            namespace_management::NamespaceManager::NodeInfo>
            namespace_nodes;

        std::vector<FileState> files;
        std::vector<ChunkState> chunks;
    };

    Checkpoint() = default;
    explicit Checkpoint(
        std::filesystem::path directory);

    [[nodiscard]] bool Create(
        const metadata::Metadata& metadata,
        const namespace_management::NamespaceManager&
            namespace_manager,
        std::uint64_t sequence);

    [[nodiscard]] bool LoadLatest(
        State& state) const;

    [[nodiscard]] bool Load(
        const std::filesystem::path& path,
        State& state) const;

    [[nodiscard]] bool Restore(
        const State& state,
        metadata::Metadata& metadata,
        namespace_management::NamespaceManager&
            namespace_manager) const;

    [[nodiscard]] std::filesystem::path
    GetCheckpointPath(
        std::uint64_t sequence) const;

    [[nodiscard]] const std::filesystem::path&
    GetDirectory() const noexcept;

private:
    static constexpr std::uint32_t kMagic =
        0x47504631U;

    static constexpr std::uint32_t kVersion =
        1U;

    static std::uint32_t ComputeChecksum(
        const std::string& data);

    static bool Serialize(
        const State& state,
        std::string& output);

    static bool Deserialize(
        const std::string& input,
        State& state);

    static bool ValidateState(
        const State& state);

    std::filesystem::path directory_;
};

}  // namespace gfs::master::recovery