#include "gfs/client/cli.hpp"

#include "gfs/common/constants.hpp"

#include <charconv>
#include <cstdint>
#include <limits>
#include <string>
#include <system_error>
#include <utility>
#include <vector>

namespace gfs::client::cli {
namespace {

std::optional<std::uint64_t> ParseUnsigned(
    const std::string& value) {
    if (value.empty()) {
        return std::nullopt;
    }

    std::uint64_t result = 0;

    const char* begin = value.data();
    const char* end =
        value.data() + value.size();

    const auto parsed =
        std::from_chars(
            begin,
            end,
            result);

    if (parsed.ec != std::errc{} ||
        parsed.ptr != end) {
        return std::nullopt;
    }

    return result;
}

std::optional<std::uint32_t> ParseReplication(
    const std::string& value) {
    const auto parsed =
        ParseUnsigned(value);

    if (!parsed.has_value() ||
        *parsed == 0 ||
        *parsed >
            std::numeric_limits<
                std::uint32_t>::max()) {
        return std::nullopt;
    }

    return static_cast<std::uint32_t>(
        *parsed);
}

std::string BaseName(
    const std::string& path) {
    if (path.empty() || path == "/") {
        return path;
    }

    const auto slash =
        path.find_last_of('/');

    if (slash == std::string::npos) {
        return path;
    }

    return path.substr(slash + 1);
}

}  // namespace

ParseResult CommandParser::Parse(
    std::string_view line) {
    ParseResult result;

    std::vector<std::string> tokens;
    std::string current;
    char quote = '\0';
    bool escaped = false;

    for (const char character : line) {
        if (escaped) {
            current.push_back(character);
            escaped = false;
            continue;
        }

        if (character == '\\') {
            escaped = true;
            continue;
        }

        if (quote != '\0') {
            if (character == quote) {
                quote = '\0';
            } else {
                current.push_back(character);
            }
            continue;
        }

        if (character == '"' ||
            character == '\'') {
            quote = character;
            continue;
        }

        if (character == ' ' ||
            character == '\t' ||
            character == '\n' ||
            character == '\r') {
            if (!current.empty()) {
                tokens.push_back(
                    std::move(current));
                current.clear();
            }

            continue;
        }

        current.push_back(character);
    }

    if (escaped) {
        current.push_back('\\');
    }

    if (quote != '\0') {
        result.error =
            "Unterminated quoted string";
        return result;
    }

    if (!current.empty()) {
        tokens.push_back(
            std::move(current));
    }

    if (tokens.empty()) {
        result.success = true;
        return result;
    }

    result.success = true;
    result.command.name =
        std::move(tokens.front());

    tokens.erase(tokens.begin());

    result.command.arguments =
        std::move(tokens);

    return result;
}

CLI::CLI(
    GFSClient& client)
    : client_(client) {
}

int CLI::Run(
    std::istream& input,
    std::ostream& output) {
    output
        << "GFS Client connected to master "
        << client_.GetMasterClient()
               .GetMasterAddress()
        << '\n';

    while (true) {
        output
            << "gfs> "
            << std::flush;

        std::string line;

        if (!std::getline(
                input,
                line)) {
            output << '\n';
            return 0;
        }

        const auto parsed =
            CommandParser::Parse(line);

        if (!parsed.success) {
            output
                << "Error: "
                << parsed.error
                << '\n';
            continue;
        }

        if (parsed.command.name.empty()) {
            continue;
        }

        if (parsed.command.name == "exit" ||
            parsed.command.name == "quit") {
            if (!parsed.command.arguments.empty()) {
                output
                    << "Usage: "
                    << parsed.command.name
                    << '\n';
                continue;
            }

            return 0;
        }

        static_cast<void>(
            Execute(
                parsed.command,
                output));
    }
}

bool CLI::Execute(
    const Command& command,
    std::ostream& output) {
    const auto& args =
        command.arguments;

    if (command.name == "help") {
        if (args.empty()) {
            PrintHelp(output);
            return true;
        }

        if (args.size() == 1) {
            PrintCommandHelp(
                args.front(),
                output);
            return true;
        }

        output
            << "Usage: help [command]\n";
        return false;
    }

    if (command.name == "ls") {
        if (args.size() != 1) {
            output
                << "Usage: ls <path>\n";
            return false;
        }

        const auto entries =
            client_.GetMasterClient()
                .ListDirectory(args.front());

        for (const auto& entry :
             entries) {
            output
                << (entry.directory
                        ? "[DIR] "
                        : "[FILE] ")
                << BaseName(entry.path)
                << '\n';
        }

        return true;
    }

    if (command.name == "mkdir") {
        if (args.size() != 1) {
            output
                << "Usage: mkdir <path>\n";
            return false;
        }

        if (!client_.GetMasterClient()
                .CreateDirectory(args.front())) {
            output
                << "Error: failed to create directory\n";
            return false;
        }

        output
            << "Created directory: "
            << args.front()
            << '\n';

        return true;
    }

    if (command.name == "create") {
        if (args.size() != 1 &&
            args.size() != 2) {
            output
                << "Usage: create <path> "
                   "[replication_factor]\n";
            return false;
        }

        std::uint32_t replication = 0;

        if (args.size() == 2) {
            const auto parsed =
                ParseReplication(args[1]);

            if (!parsed.has_value()) {
                output
                    << "Error: invalid replication factor\n";
                return false;
            }

            replication = *parsed;
        }

        if (!client_.GetMasterClient()
                .CreateFile(
                    args[0],
                    replication)) {
            output
                << "Error: failed to create file\n";
            return false;
        }

        output
            << "Created file: "
            << args[0]
            << '\n';

        return true;
    }

    if (command.name == "delete") {
        if (args.size() != 1) {
            output
                << "Usage: delete <path>\n";
            return false;
        }

        if (!client_.GetMasterClient()
                .DeleteFile(args.front())) {
            output
                << "Error: failed to delete file\n";
            return false;
        }

        output
            << "Deleted file: "
            << args.front()
            << '\n';

        return true;
    }

    if (command.name == "rename") {
        if (args.size() != 2) {
            output
                << "Usage: rename <source> "
                   "<destination>\n";
            return false;
        }

        if (!client_.GetMasterClient()
                .RenameFile(
                    args[0],
                    args[1])) {
            output
                << "Error: failed to rename file\n";
            return false;
        }

        output
            << "Renamed "
            << args[0]
            << " -> "
            << args[1]
            << '\n';

        return true;
    }

    if (command.name == "stat") {
        if (args.size() != 1) {
            output
                << "Usage: stat <path>\n";
            return false;
        }

        const auto file =
            client_.LookupFile(
                args.front());

        if (!file.has_value()) {
            output
                << "Error: file not found\n";
            return false;
        }

        output
            << "Path: "
            << file->path
            << '\n'
            << "Size: "
            << file->size
            << '\n'
            << "Replication factor: "
            << file->replication_factor
            << '\n'
            << "Chunks: "
            << file->chunk_count
            << '\n';

        if (!file->chunk_handles.empty()) {
            output
                << "Chunk handles:\n";

            for (std::size_t index = 0;
                 index < file->chunk_handles.size();
                 ++index) {
                output
                    << "  "
                    << index
                    << ": "
                    << file->chunk_handles[index]
                    << '\n';
            }
        }

        return true;
    }

    if (command.name == "read" ||
        command.name == "cat") {
        if (command.name == "cat" &&
            args.size() != 1) {
            output
                << "Usage: cat <path>\n";
            return false;
        }

        if (command.name == "read" &&
            (args.empty() ||
             args.size() > 3)) {
            output
                << "Usage: read <path> "
                   "[offset] [length]\n";
            return false;
        }

        const std::string path =
            args.front();

        const auto file =
            client_.LookupFile(path);

        if (!file.has_value()) {
            output
                << "Error: file not found\n";
            return false;
        }

        std::uint64_t offset = 0;

        std::size_t length =
            static_cast<std::size_t>(
                file->size);

        if (command.name == "read" &&
            args.size() >= 2) {
            const auto parsed =
                ParseUnsigned(args[1]);

            if (!parsed.has_value()) {
                output
                    << "Error: invalid offset\n";
                return false;
            }

            offset = *parsed;

            if (offset > file->size) {
                output
                    << "Error: offset is beyond end of file\n";
                return false;
            }

            length =
                static_cast<std::size_t>(
                    file->size - offset);
        }

        if (command.name == "read" &&
            args.size() == 3) {
            const auto parsed =
                ParseUnsigned(args[2]);

            if (!parsed.has_value() ||
                *parsed >
                    static_cast<std::uint64_t>(
                        std::numeric_limits<
                            std::size_t>::max())) {
                output
                    << "Error: invalid length\n";
                return false;
            }

            length =
                static_cast<std::size_t>(
                    *parsed);
        }

        std::string data;

        if (!client_.Read(
                path,
                offset,
                length,
                data)) {
            output
                << "Error: read failed\n";
            return false;
        }

        output << data;

        if (data.empty() ||
            data.back() != '\n') {
            output << '\n';
        }

        return true;
    }

    if (command.name == "write") {
        if (args.size() != 3) {
            output
                << "Usage: write <path> "
                   "<offset> <data>\n";
            return false;
        }

        const auto offset =
            ParseUnsigned(args[1]);

        if (!offset.has_value()) {
            output
                << "Error: invalid offset\n";
            return false;
        }

        if (!client_.Write(
                args[0],
                *offset,
                args[2])) {
            output
                << "Error: write failed\n";
            return false;
        }

        output
            << "Wrote "
            << args[2].size()
            << " bytes\n";

        return true;
    }

    if (command.name == "append") {
        if (args.size() != 2) {
            output
                << "Usage: append <path> "
                   "<data>\n";
            return false;
        }

        std::uint64_t offset = 0;

        if (!client_.Append(
                args[0],
                args[1],
                offset)) {
            output
                << "Error: append failed\n";
            return false;
        }

        output
            << "Appended "
            << args[1].size()
            << " bytes at offset "
            << offset
            << '\n';

        return true;
    }

    if (command.name == "snapshot") {
        if (args.size() != 2) {
            output
                << "Usage: snapshot <source> "
                   "<destination>\n";
            return false;
        }

        if (!client_.GetMasterClient()
                .CreateSnapshot(
                    args[0],
                    args[1])) {
            output
                << "Error: failed to create snapshot\n";
            return false;
        }

        output
            << "Created snapshot: "
            << args[1]
            << '\n';

        return true;
    }

    if (command.name == "chunks") {
        if (args.size() != 1) {
            output
                << "Usage: chunks <path>\n";
            return false;
        }

        const auto file =
            client_.LookupFile(
                args.front());

        if (!file.has_value()) {
            output
                << "Error: file not found\n";
            return false;
        }

        if (file->chunk_handles.empty()) {
            output
                << "No chunks\n";
            return true;
        }

        output
            << "Chunks:\n";

        for (std::size_t index = 0;
             index < file->chunk_handles.size();
             ++index) {
            const auto chunk =
                client_.LookupChunk(
                    args.front(),
                    static_cast<ChunkIndex>(
                        index));

            output
                << "  "
                << index
                << ": handle="
                << file->chunk_handles[index];

            if (chunk.has_value()) {
                output
                    << " version="
                    << chunk->version
                    << " replicas="
                    << chunk->locations.size();
            }

            output << '\n';
        }

        return true;
    }

    if (command.name == "locations") {
        if (args.size() != 2) {
            output
                << "Usage: locations <path> "
                   "<chunk_index>\n";
            return false;
        }

        const auto index =
            ParseUnsigned(args[1]);

        if (!index.has_value()) {
            output
                << "Error: invalid chunk index\n";
            return false;
        }

        const auto chunk =
            client_.LookupChunk(
                args[0],
                static_cast<ChunkIndex>(
                    *index));

        if (!chunk.has_value()) {
            output
                << "Error: chunk not found\n";
            return false;
        }

        output
            << "Chunk handle: "
            << chunk->handle
            << '\n'
            << "Version: "
            << chunk->version
            << '\n'
            << "Replicas:\n";

        for (const auto& location :
             chunk->locations) {
            output
                << "  Server "
                << location.server_id
                << ": "
                << (location.address.empty()
                        ? "<unknown>"
                        : location.address)
                << '\n';
        }

        return true;
    }

    output
        << "Unknown command: "
        << command.name
        << '\n';

    return false;
}

void CLI::PrintHelp(
    std::ostream& output) {
    output
        << "Commands:\n"
        << "  ls <path>                         "
           "List directory contents\n"
        << "  mkdir <path>                      "
           "Create directory\n"
        << "  create <path> [replication_factor] "
           "Create file\n"
        << "  delete <path>                     "
           "Delete file\n"
        << "  rename <source> <destination>      "
           "Rename file\n"
        << "  stat <path>                        "
           "Show file metadata\n"
        << "  read <path> [offset] [length]      "
           "Read file data\n"
        << "  cat <path>                         "
           "Read complete file\n"
        << "  write <path> <offset> <data>       "
           "Write data\n"
        << "  append <path> <data>               "
           "Append a record\n"
        << "  snapshot <source> <destination>    "
           "Create a snapshot\n"
        << "  chunks <path>                      "
           "Show chunk metadata\n"
        << "  locations <path> <chunk_index>     "
           "Show chunk replicas\n"
        << "  help [command]                     "
           "Show help\n"
        << "  exit                               "
           "Exit the CLI\n"
        << "  quit                               "
           "Exit the CLI\n";
}

void CLI::PrintCommandHelp(
    std::string_view command,
    std::ostream& output) {
    if (command == "ls") {
        output
            << "Usage: ls <path>\n"
            << "List the direct contents of a GFS directory.\n";
        return;
    }

    if (command == "mkdir") {
        output
            << "Usage: mkdir <path>\n"
            << "Create a GFS directory.\n";
        return;
    }

    if (command == "create") {
        output
            << "Usage: create <path> [replication_factor]\n"
            << "Create a GFS file.\n";
        return;
    }

    if (command == "delete") {
        output
            << "Usage: delete <path>\n"
            << "Delete a GFS file.\n";
        return;
    }

    if (command == "rename") {
        output
            << "Usage: rename <source> <destination>\n"
            << "Rename a GFS file.\n";
        return;
    }

    if (command == "stat") {
        output
            << "Usage: stat <path>\n"
            << "Display file size, replication factor, and chunks.\n";
        return;
    }

    if (command == "read") {
        output
            << "Usage: read <path> [offset] [length]\n"
            << "Read bytes from a GFS file.\n";
        return;
    }

    if (command == "cat") {
        output
            << "Usage: cat <path>\n"
            << "Read the complete GFS file.\n";
        return;
    }

    if (command == "write") {
        output
            << "Usage: write <path> <offset> <data>\n"
            << "Write data at a byte offset.\n";
        return;
    }

    if (command == "append") {
        output
            << "Usage: append <path> <data>\n"
            << "Append data using the existing record-appender layer.\n";
        return;
    }

    if (command == "snapshot") {
        output
            << "Usage: snapshot <source> <destination>\n"
            << "Create a GFS snapshot.\n";
        return;
    }

    if (command == "chunks") {
        output
            << "Usage: chunks <path>\n"
            << "Display chunk handles and available chunk metadata.\n";
        return;
    }

    if (command == "locations") {
        output
            << "Usage: locations <path> <chunk_index>\n"
            << "Display the replicas and network addresses for a chunk.\n";
        return;
    }

    if (command == "help") {
        output
            << "Usage: help [command]\n"
            << "Show all commands or command-specific help.\n";
        return;
    }

    if (command == "exit" ||
        command == "quit") {
        output
            << "Usage: "
            << command
            << '\n'
            << "Exit the GFS CLI.\n";
        return;
    }

    output
        << "Unknown command: "
        << command
        << '\n';
}

}  // namespace gfs::client::cli