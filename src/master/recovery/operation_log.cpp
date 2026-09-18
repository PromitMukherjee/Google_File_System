#include "gfs/master/recovery/operation_log.hpp"

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstring>
#include <fstream>
#include <limits>
#include <string_view>

namespace gfs::master::recovery {

namespace {

template <typename T>
bool WriteInteger(
    std::ostream& output,
    T value) {
    output.write(
        reinterpret_cast<const char*>(&value),
        static_cast<std::streamsize>(sizeof(T)));

    return output.good();
}

template <typename T>
bool ReadInteger(
    std::istream& input,
    T& value) {
    input.read(
        reinterpret_cast<char*>(&value),
        static_cast<std::streamsize>(sizeof(T)));

    return input.good();
}

bool IsValidOperationType(
    std::uint32_t value) {
    return value >=
               static_cast<std::uint32_t>(
                   OperationType::CreateDirectory) &&
           value <=
               static_cast<std::uint32_t>(
                   OperationType::CopyOnWrite);
}

}  // namespace

OperationLog::OperationLog(
    std::filesystem::path path)
    : path_(std::move(path)) {
}

bool OperationLog::Open(
    const std::filesystem::path& path) {
    Close();

    path_ = path;

    if (path_.empty()) {
        return false;
    }

    std::error_code ec;

    if (!path_.parent_path().empty()) {
        std::filesystem::create_directories(
            path_.parent_path(),
            ec);

        if (ec) {
            return false;
        }
    }

    if (!std::filesystem::exists(path_)) {
        std::ofstream create_file(
            path_,
            std::ios::binary);

        if (!create_file.good()) {
            return false;
        }
    }

    if (!Validate()) {
        return false;
    }

    std::vector<OperationRecord> records;

    if (!ReadRecords(&records)) {
        return false;
    }

    last_sequence_ =
        records.empty()
            ? 0
            : records.back().sequence;

    open_ = true;
    return true;
}

bool OperationLog::Open() {
    return Open(path_);
}

void OperationLog::Close() {
    open_ = false;
}

bool OperationLog::IsOpen() const noexcept {
    return open_;
}

bool OperationLog::Append(
    OperationType type,
    const std::string& payload) {
    if (!open_) {
        return false;
    }

    if (last_sequence_ ==
        std::numeric_limits<std::uint64_t>::max()) {
        return false;
    }

    return Append(
        last_sequence_ + 1,
        type,
        payload);
}

bool OperationLog::Append(
    std::uint64_t sequence,
    OperationType type,
    const std::string& payload) {
    if (!open_ ||
        sequence == 0 ||
        sequence != last_sequence_ + 1) {
        return false;
    }

    if (payload.size() >
        std::numeric_limits<std::uint32_t>::max()) {
        return false;
    }

    if (!WriteRecord(
            sequence,
            type,
            payload)) {
        return false;
    }

    last_sequence_ = sequence;
    return true;
}

bool OperationLog::Flush() {
    if (!open_) {
        return false;
    }

    std::ofstream output(
        path_,
        std::ios::binary |
        std::ios::app);

    if (!output.good()) {
        return false;
    }

    output.flush();

    return output.good();
}

std::vector<OperationRecord>
OperationLog::Replay(
    std::uint64_t after_sequence) const {
    std::vector<OperationRecord> records;

    if (!ReadRecords(&records)) {
        return {};
    }

    if (after_sequence == 0) {
        return records;
    }

    records.erase(
        std::remove_if(
            records.begin(),
            records.end(),
            [after_sequence](
                const OperationRecord& record) {
                return record.sequence <= after_sequence;
            }),
        records.end());

    return records;
}

bool OperationLog::Validate() const {
    std::vector<OperationRecord> records;
    return ReadRecords(&records);
}

std::uint64_t OperationLog::LastSequence()
    const noexcept {
    return last_sequence_;
}

const std::filesystem::path&
OperationLog::GetPath() const noexcept {
    return path_;
}

std::string OperationLog::EncodeFields(
    const std::vector<std::string>& fields) {
    std::string result;

    for (const auto& field : fields) {
        const std::uint64_t length =
            static_cast<std::uint64_t>(field.size());

        result.append(
            reinterpret_cast<const char*>(&length),
            sizeof(length));

        result.append(field);
    }

    return result;
}

bool OperationLog::DecodeFields(
    const std::string& payload,
    std::vector<std::string>& fields) {
    fields.clear();

    std::size_t offset = 0;

    while (offset < payload.size()) {
        if (payload.size() - offset <
            sizeof(std::uint64_t)) {
            return false;
        }

        std::uint64_t length = 0;

        std::memcpy(
            &length,
            payload.data() + offset,
            sizeof(length));

        offset += sizeof(length);

        if (length >
            static_cast<std::uint64_t>(
                payload.size() - offset)) {
            return false;
        }

        fields.emplace_back(
            payload.data() + offset,
            static_cast<std::size_t>(length));

        offset +=
            static_cast<std::size_t>(length);
    }

    return true;
}

std::uint32_t OperationLog::ComputeChecksum(
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

bool OperationLog::ReadRecords(
    std::vector<OperationRecord>* records) const {
    if (records == nullptr ||
        path_.empty()) {
        return false;
    }

    records->clear();

    std::ifstream input(
        path_,
        std::ios::binary);

    if (!input.good()) {
        return false;
    }

    std::uint64_t expected_sequence = 1;

    while (true) {
        const int first =
            input.peek();

        if (first == EOF) {
            break;
        }

        std::uint32_t magic = 0;
        std::uint32_t version = 0;
        std::uint64_t sequence = 0;
        std::uint32_t type_value = 0;
        std::uint32_t payload_size = 0;
        std::uint32_t checksum = 0;

        if (!ReadInteger(input, magic) ||
            !ReadInteger(input, version) ||
            !ReadInteger(input, sequence) ||
            !ReadInteger(input, type_value) ||
            !ReadInteger(input, payload_size) ||
            !ReadInteger(input, checksum)) {
            return false;
        }

        if (magic != kMagic ||
            version != kFormatVersion ||
            sequence != expected_sequence ||
            !IsValidOperationType(type_value)) {
            return false;
        }

        if (payload_size >
            static_cast<std::uint32_t>(
                std::numeric_limits<std::size_t>::max())) {
            return false;
        }

        std::string payload(
            payload_size,
            '\0');

        if (payload_size != 0) {
            input.read(
                payload.data(),
                static_cast<std::streamsize>(
                    payload_size));

            if (!input.good()) {
                return false;
            }
        }

        const std::uint32_t calculated =
            ComputeChecksum(payload);

        if (calculated != checksum) {
            return false;
        }

        records->push_back(
            OperationRecord{
                sequence,
                static_cast<OperationType>(
                    type_value),
                std::move(payload)});

        if (expected_sequence ==
            std::numeric_limits<std::uint64_t>::max()) {
            return false;
        }

        ++expected_sequence;
    }

    return true;
}

bool OperationLog::WriteRecord(
    std::uint64_t sequence,
    OperationType type,
    const std::string& payload) {
    std::ofstream output(
        path_,
        std::ios::binary |
        std::ios::app);

    if (!output.good()) {
        return false;
    }

    const std::uint32_t magic = kMagic;
    const std::uint32_t version =
        kFormatVersion;
    const std::uint32_t type_value =
        static_cast<std::uint32_t>(type);

    const std::uint32_t payload_size =
        static_cast<std::uint32_t>(
            payload.size());

    const std::uint32_t checksum =
        ComputeChecksum(payload);

    if (!WriteInteger(output, magic) ||
        !WriteInteger(output, version) ||
        !WriteInteger(output, sequence) ||
        !WriteInteger(output, type_value) ||
        !WriteInteger(output, payload_size) ||
        !WriteInteger(output, checksum)) {
        return false;
    }

    if (!payload.empty()) {
        output.write(
            payload.data(),
            static_cast<std::streamsize>(
                payload.size()));
    }

    output.flush();

    return output.good();
}

}  // namespace gfs::master::recovery