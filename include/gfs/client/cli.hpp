#pragma once

#include "gfs/client/gfs_client.hpp"

#include <istream>
#include <ostream>
#include <string>
#include <string_view>
#include <vector>

namespace gfs::client::cli {

struct Command {
    std::string name;
    std::vector<std::string> arguments;
};

struct ParseResult {
    bool success = false;
    std::string error;
    Command command;
};

class CommandParser {
public:
    [[nodiscard]] static ParseResult Parse(
        std::string_view line);
};

class CLI {
public:
    explicit CLI(GFSClient& client);

    [[nodiscard]] int Run(
        std::istream& input,
        std::ostream& output);

private:
    [[nodiscard]] bool Execute(
        const Command& command,
        std::ostream& output);

    static void PrintHelp(
        std::ostream& output);

    static void PrintCommandHelp(
        std::string_view command,
        std::ostream& output);

    GFSClient& client_;
};

}  // namespace gfs::client::cli