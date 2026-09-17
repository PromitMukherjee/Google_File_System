#include "gfs/master/recovery/checkpoint.hpp"

#include <algorithm>
#include <cstring>
#include <fstream>
#include <limits>
#include <string_view>

namespace gfs::master::recovery {

namespace {

template <typename T>
void WriteInteger(
    std::string& output,
    T value) {
    output.append(
        reinterpret_cast<const char*>(&value),
        sizeof(T));
}

template <typename T>
bool ReadInteger(
    const std::string& input,
    std::size_t& offset,
    T& value) {
    if (offset > input.size() ||
        input.size() - offset < sizeof(T)) {
        return false;
    }

    std::memcpy(
        &value,
        input.data() + offset,
        sizeof(T));

    offset += sizeof(T);
    return true;
}

void WriteString(
    std::string& output,
    const std::string& value) {
    WriteInteger(
        output,
        static_cast<std::uint64_t>(
            value.size()));

    output.append(value);
}

bool ReadString(
    const std::string& input,
    std::size_t& offset,
    std::string& value) {
    std::uint64_t size = 0;

    if (!ReadInteger(
            input,
            offset,
            size)) {
        return false;
    }

    if (size >
        static_cast<std::uint64_t>(
            input.size() - offset)) {
        return false;
    }

    value.assign(
        input.data() + offset,
        static_cast<std::size_t>(size));

    offset +=
        static_cast<std::size_t>(size);

    return true;
}

}  // namespace

Checkpoint::Checkpoint(
    std::filesystem::path directory)
    : directory_(std::move(directory)) {
}

bool Checkpoint::Create(
    const metadata::Metadata& metadata,
    const namespace_management::NamespaceManager&
        namespace_manager,
    std::uint64_t sequence) {
    if (directory_.empty()) {
        return false;
    }

    std::error_code ec;

    std::filesystem::create_directories(
        directory_,
        ec);

    if (ec) {
        return false;
    }

    State state;
    state.sequence = sequence;
    state.namespace_nodes =
        namespace_manager.ExportNodes();

    for (const auto& file :
         metadata.ExportFiles()) {
        state.files.push_back(
            FileState{
                file.path,
                file.size,
                file.replication_factor,
                file.chunk_handles});
    }

    for (const auto& chunk :
         metadata.ExportChunks()) {
        state.chunks.push_back(
            ChunkState{
                chunk.handle,
                chunk.version,
                chunk.size});
    }

    std::string serialized;

    if (!Serialize(state, serialized)) {
        return false;
    }

    const auto final_path =
        GetCheckpointPath(sequence);

    const auto temp_path =
        final_path.string() + ".tmp";

    {
        std::ofstream output(
            temp_path,
            std::ios::binary |
            std::ios::trunc);

        if (!output.good()) {
            return false;
        }

        output.write(
            serialized.data(),
            static_cast<std::streamsize>(
                serialized.size()));

        output.flush();

        if (!output.good()) {
            return false;
        }
    }

    std::filesystem::rename(
        temp_path,
        final_path,
        ec);

    if (ec) {
        std::filesystem::remove(
            final_path,
            ec);

        ec.clear();

        std::filesystem::rename(
            temp_path,
            final_path,
            ec);

        if (ec) {
            return false;
        }
    }

    {
        std::ofstream latest(
            directory_ / "LATEST",
            std::ios::trunc);

        if (!latest.good()) {
            return false;
        }

        latest << sequence;
        latest.flush();

        if (!latest.good()) {
            return false;
        }
    }

    return true;
}

bool Checkpoint::LoadLatest(
    State& state) const {
    if (directory_.empty()) {
        return false;
    }

    std::error_code ec;

    if (!std::filesystem::exists(
            directory_,
            ec)) {
        return false;
    }

    std::vector<
        std::pair<
            std::uint64_t,
            std::filesystem::path>>
        candidates;

    for (const auto& entry :
         std::filesystem::directory_iterator(
             directory_,
             ec)) {
        if (ec) {
            return false;
        }

        if (!entry.is_regular_file()) {
            continue;
        }

        const std::string name =
            entry.path().filename().string();

        constexpr std::string_view prefix =
            "checkpoint_";

        constexpr std::string_view suffix =
            ".bin";

        if (name.size() <=
            prefix.size() + suffix.size()) {
            continue;
        }

        if (name.compare(
                0,
                prefix.size(),
                prefix) != 0 ||
            name.compare(
                name.size() - suffix.size(),
                suffix.size(),
                suffix) != 0) {
            continue;
        }

        const std::string number =
            name.substr(
                prefix.size(),
                name.size() -
                    prefix.size() -
                    suffix.size());

        try {
            const std::uint64_t sequence =
                std::stoull(number);

            candidates.emplace_back(
                sequence,
                entry.path());
        } catch (...) {
        }
    }

    std::sort(
        candidates.begin(),
        candidates.end(),
        [](const auto& left,
           const auto& right) {
            return left.first > right.first;
        });

    for (const auto& candidate :
         candidates) {
        State loaded;

        if (Load(
                candidate.second,
                loaded)) {
            state = std::move(loaded);
            return true;
        }
    }

    return false;
}

bool Checkpoint::Load(
    const std::filesystem::path& path,
    State& state) const {
    std::ifstream input(
        path,
        std::ios::binary);

    if (!input.good()) {
        return false;
    }

    input.seekg(
        0,
        std::ios::end);

    const auto end =
        input.tellg();

    if (end < 0) {
        return false;
    }

    input.seekg(
        0,
        std::ios::beg);

    std::string data(
        static_cast<std::size_t>(end),
        '\0');

    if (!data.empty()) {
        input.read(
            data.data(),
            static_cast<std::streamsize>(
                data.size()));

        if (!input.good()) {
            return false;
        }
    }

    return Deserialize(
        data,
        state);
}

bool Checkpoint::Restore(
    const State& state,
    metadata::Metadata& metadata,
    namespace_management::NamespaceManager&
        namespace_manager) const {
    if (!ValidateState(state)) {
        return false;
    }

    namespace_manager.Clear();
    metadata.Clear();

    if (!namespace_manager.RestoreNodes(
            state.namespace_nodes)) {
        return false;
    }

    for (const auto& file :
         state.files) {
        if (!metadata.RestoreFile(
                file.path,
                file.size,
                file.replication_factor,
                file.chunk_handles)) {
            return false;
        }
    }

    for (const auto& chunk :
         state.chunks) {
        if (!metadata.RestoreChunk(
                chunk.handle,
                chunk.version,
                chunk.size)) {
            return false;
        }
    }

    return true;
}

std::filesystem::path
Checkpoint::GetCheckpointPath(
    std::uint64_t sequence) const {
    return directory_ /
           ("checkpoint_" +
            std::to_string(sequence) +
            ".bin");
}

const std::filesystem::path&
Checkpoint::GetDirectory() const noexcept {
    return directory_;
}

std::uint32_t Checkpoint::ComputeChecksum(
    const std::string& data) {
    std::uint32_t crc = 0xFFFFFFFFU;

    for (const unsigned char byte :
         std::string_view(data)) {
        crc ^= byte;

        for (int bit = 0; bit < 8; ++bit) {
            if ((crc & 1U) != 0U) {
                crc =
                    (crc >> 1U) ^
                    0xEDB88320U;
            } else {
                crc >>= 1U;
            }
        }
    }

    return ~crc;
}

bool Checkpoint::Serialize(
    const State& state,
    std::string& output) {
    output.clear();

    std::vector<
        namespace_management::NamespaceManager::NodeInfo>
        nodes = state.namespace_nodes;

    std::vector<FileState> files =
        state.files;

    std::vector<ChunkState> chunks =
        state.chunks;

    std::sort(
        nodes.begin(),
        nodes.end(),
        [](const auto& a, const auto& b) {
            return a.path < b.path;
        });

    std::sort(
        files.begin(),
        files.end(),
        [](const auto& a, const auto& b) {
            return a.path < b.path;
        });

    std::sort(
        chunks.begin(),
        chunks.end(),
        [](const auto& a, const auto& b) {
            return a.handle < b.handle;
        });

    std::string body;

    WriteInteger(
        body,
        state.sequence);

    WriteInteger(
        body,
        static_cast<std::uint64_t>(
            nodes.size()));

    for (const auto& node : nodes) {
        WriteString(body, node.path);

        WriteInteger(
            body,
            static_cast<std::uint32_t>(
                node.type));
    }

    WriteInteger(
        body,
        static_cast<std::uint64_t>(
            files.size()));

    for (const auto& file : files) {
        WriteString(body, file.path);

        WriteInteger(
            body,
            file.size);

        WriteInteger(
            body,
            file.replication_factor);

        WriteInteger(
            body,
            static_cast<std::uint64_t>(
                file.chunk_handles.size()));

        for (const ChunkHandle handle :
             file.chunk_handles) {
            WriteInteger(
                body,
                handle);
        }
    }

    WriteInteger(
        body,
        static_cast<std::uint64_t>(
            chunks.size()));

    for (const auto& chunk : chunks) {
        WriteInteger(
            body,
            chunk.handle);

        WriteInteger(
            body,
            chunk.version);

        WriteInteger(
            body,
            chunk.size);
    }

    WriteInteger(
        output,
        kMagic);

    WriteInteger(
        output,
        kVersion);

    WriteInteger(
        output,
        static_cast<std::uint64_t>(
            body.size()));

    WriteInteger(
        output,
        ComputeChecksum(body));

    output.append(body);

    return true;
}

bool Checkpoint::Deserialize(
    const std::string& input,
    State& state) {
    std::size_t offset = 0;

    std::uint32_t magic = 0;
    std::uint32_t version = 0;
    std::uint64_t body_size = 0;
    std::uint32_t checksum = 0;

    if (!ReadInteger(
            input,
            offset,
            magic) ||
        !ReadInteger(
            input,
            offset,
            version) ||
        !ReadInteger(
            input,
            offset,
            body_size) ||
        !ReadInteger(
            input,
            offset,
            checksum)) {
        return false;
    }

    if (magic != kMagic ||
        version != kVersion) {
        return false;
    }

    if (body_size !=
        static_cast<std::uint64_t>(
            input.size() - offset)) {
        return false;
    }

    const std::string body =
        input.substr(
            offset,
            static_cast<std::size_t>(
                body_size));

    if (ComputeChecksum(body) != checksum) {
        return false;
    }

    offset = 0;

    if (!ReadInteger(
            body,
            offset,
            state.sequence)) {
        return false;
    }

    std::uint64_t node_count = 0;

    if (!ReadInteger(
            body,
            offset,
            node_count)) {
        return false;
    }

    state.namespace_nodes.clear();
    state.namespace_nodes.reserve(
        static_cast<std::size_t>(
            node_count));

    for (std::uint64_t i = 0;
         i < node_count;
         ++i) {
        namespace_management::
            NamespaceManager::NodeInfo node;

        std::uint32_t type = 0;

        if (!ReadString(
                body,
                offset,
                node.path) ||
            !ReadInteger(
                body,
                offset,
                type)) {
            return false;
        }

        if (type > static_cast<std::uint32_t>(
                       namespace_management::
                           NamespaceManager::
                               NodeType::File)) {
            return false;
        }

        node.type =
            static_cast<
                namespace_management::
                    NamespaceManager::NodeType>(
                type);

        state.namespace_nodes.push_back(
            std::move(node));
    }

    std::uint64_t file_count = 0;

    if (!ReadInteger(
            body,
            offset,
            file_count)) {
        return false;
    }

    state.files.clear();
    state.files.reserve(
        static_cast<std::size_t>(
            file_count));

    for (std::uint64_t i = 0;
         i < file_count;
         ++i) {
        FileState file;

        std::uint64_t chunk_count = 0;

        if (!ReadString(
                body,
                offset,
                file.path) ||
            !ReadInteger(
                body,
                offset,
                file.size) ||
            !ReadInteger(
                body,
                offset,
                file.replication_factor) ||
            !ReadInteger(
                body,
                offset,
                chunk_count)) {
            return false;
        }

        if (file.replication_factor == 0) {
            return false;
        }

        file.chunk_handles.reserve(
            static_cast<std::size_t>(
                chunk_count));

        for (std::uint64_t j = 0;
             j < chunk_count;
             ++j) {
            ChunkHandle handle = 0;

            if (!ReadInteger(
                    body,
                    offset,
                    handle) ||
                handle == 0) {
                return false;
            }

            file.chunk_handles.push_back(
                handle);
        }

        state.files.push_back(
            std::move(file));
    }

    std::uint64_t chunk_count = 0;

    if (!ReadInteger(
            body,
            offset,
            chunk_count)) {
        return false;
    }

    state.chunks.clear();
    state.chunks.reserve(
        static_cast<std::size_t>(
            chunk_count));

    for (std::uint64_t i = 0;
         i < chunk_count;
         ++i) {
        ChunkState chunk;

        if (!ReadInteger(
                body,
                offset,
                chunk.handle) ||
            !ReadInteger(
                body,
                offset,
                chunk.version) ||
            !ReadInteger(
                body,
                offset,
                chunk.size)) {
            return false;
        }

        if (chunk.handle == 0 ||
            chunk.version == 0) {
            return false;
        }

        state.chunks.push_back(chunk);
    }

    if (offset != body.size()) {
        return false;
    }

    return ValidateState(state);
}

bool Checkpoint::ValidateState(
    const State& state) {
    bool root_found = false;

    std::vector<std::string> namespace_paths;

    for (const auto& node :
         state.namespace_nodes) {
        if (!namespace_management::
                NamespaceManager::IsValidPath(
                    node.path)) {
            return false;
        }

        if (node.path == "/") {
            if (node.type !=
                namespace_management::
                    NamespaceManager::NodeType::
                        Directory) {
                return false;
            }

            root_found = true;
        }

        namespace_paths.push_back(node.path);
    }

    if (!root_found) {
        return false;
    }

    std::sort(
        namespace_paths.begin(),
        namespace_paths.end());

    if (std::adjacent_find(
            namespace_paths.begin(),
            namespace_paths.end()) !=
        namespace_paths.end()) {
        return false;
    }

    std::vector<ChunkHandle> chunk_handles;

    for (const auto& chunk :
         state.chunks) {
        if (chunk.handle == 0 ||
            chunk.version == 0) {
            return false;
        }

        chunk_handles.push_back(
            chunk.handle);
    }

    std::sort(
        chunk_handles.begin(),
        chunk_handles.end());

    if (std::adjacent_find(
            chunk_handles.begin(),
            chunk_handles.end()) !=
        chunk_handles.end()) {
        return false;
    }

    for (const auto& file :
         state.files) {
        if (!namespace_management::
                NamespaceManager::IsValidPath(
                    file.path) ||
            file.path == "/" ||
            file.replication_factor == 0) {
            return false;
        }

        if (!std::binary_search(
                namespace_paths.begin(),
                namespace_paths.end(),
                file.path)) {
            return false;
        }

        for (const ChunkHandle handle :
             file.chunk_handles) {
            if (!std::binary_search(
                    chunk_handles.begin(),
                    chunk_handles.end(),
                    handle)) {
                return false;
            }
        }
    }

    return true;
}

}  // namespace gfs::master::recovery