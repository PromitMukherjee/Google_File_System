#pragma once

#include "gfs/common/types.hpp"

#include <cstdint>
#include <filesystem>
#include <string>
#include <vector>

namespace gfs::master::recovery {

enum class OperationType : std::uint32_t {
    CreateDirectory = 1,
    CreateFile = 2,
    DeleteFile = 3,
    RenameFile = 4,
    AllocateChunk = 5,
    UpdateFileSize = 6,
    SetChunkVersion = 7,
    AddChunkToFile = 8,
    RemoveChunkFromFile = 9,
    DeleteChunk = 10,
    SetChunkSize = 11,
    CreateSnapshot = 12,
    CopyOnWrite = 13
};

struct OperationRecord {
    std::uint64_t sequence = 0;
    OperationType type = OperationType::CreateFile;
    std::string payload;
};

class OperationLog {
public:
    OperationLog() = default;
    explicit OperationLog(std::filesystem::path path);

    OperationLog(const OperationLog&) = delete;
    OperationLog& operator=(const OperationLog&) = delete;

    bool Open(const std::filesystem::path& path);
    bool Open();

    void Close();

    [[nodiscard]] bool IsOpen() const noexcept;

    [[nodiscard]] bool Append(
        OperationType type,
        const std::string& payload);

    [[nodiscard]] bool Append(
        std::uint64_t sequence,
        OperationType type,
        const std::string& payload);

    [[nodiscard]] bool Flush();

    [[nodiscard]] std::vector<OperationRecord> Replay(
        std::uint64_t after_sequence = 0) const;

    [[nodiscard]] bool Validate() const;

    [[nodiscard]] std::uint64_t LastSequence() const noexcept;

    [[nodiscard]] const std::filesystem::path&
    GetPath() const noexcept;

    static std::string EncodeFields(
        const std::vector<std::string>& fields);

    static bool DecodeFields(
        const std::string& payload,
        std::vector<std::string>& fields);

private:
    static constexpr std::uint32_t kMagic = 0x47464C31U;
    static constexpr std::uint32_t kFormatVersion = 1U;

    static constexpr std::uint64_t kHeaderSize =
        sizeof(std::uint32_t) +
        sizeof(std::uint32_t) +
        sizeof(std::uint64_t) +
        sizeof(std::uint32_t) +
        sizeof(std::uint32_t) +
        sizeof(std::uint32_t);

    static std::uint32_t ComputeChecksum(
        const std::string& data);

    bool ReadRecords(
        std::vector<OperationRecord>* records) const;

    bool WriteRecord(
        std::uint64_t sequence,
        OperationType type,
        const std::string& payload);

    std::filesystem::path path_;
    std::uint64_t last_sequence_ = 0;
    bool open_ = false;
};

}  // namespace gfs::master::recovery