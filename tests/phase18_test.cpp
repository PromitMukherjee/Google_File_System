#include "gfs/client/cli.hpp"

#include <gtest/gtest.h>

#include <memory>
#include <sstream>
#include <string>

namespace {

using gfs::client::cli::CommandParser;

TEST(
    Phase18CliTest,
    ParsesWhitespaceAndQuotedArguments) {
    const auto result =
        CommandParser::Parse(
            R"(  write   /data/file   0   "Hello distributed file system"  )");

    ASSERT_TRUE(result.success);

    ASSERT_EQ(
        result.command.name,
        "write");

    ASSERT_EQ(
        result.command.arguments.size(),
        3U);

    EXPECT_EQ(
        result.command.arguments[0],
        "/data/file");

    EXPECT_EQ(
        result.command.arguments[1],
        "0");

    EXPECT_EQ(
        result.command.arguments[2],
        "Hello distributed file system");
}

TEST(
    Phase18CliTest,
    ParsesSingleQuotesAndEscapes) {
    const auto result =
        CommandParser::Parse(
            R"(append /data/file 'hello world' )");

    ASSERT_TRUE(result.success);

    ASSERT_EQ(
        result.command.name,
        "append");

    ASSERT_EQ(
        result.command.arguments.size(),
        2U);

    EXPECT_EQ(
        result.command.arguments[1],
        "hello world");

    const auto escaped =
        CommandParser::Parse(
            R"(write /file 0 "a \"quoted\" value")");

    ASSERT_TRUE(escaped.success);

    ASSERT_EQ(
        escaped.command.arguments[2],
        R"(a "quoted" value)");
}

TEST(
    Phase18CliTest,
    RejectsUnterminatedQuotes) {
    const auto result =
        CommandParser::Parse(
            R"(write /file 0 "unterminated)");

    EXPECT_FALSE(result.success);

    EXPECT_EQ(
        result.error,
        "Unterminated quoted string");
}

TEST(
    Phase18CliTest,
    HandlesEmptyInput) {
    const auto result =
        CommandParser::Parse(
            "   \t  ");

    ASSERT_TRUE(result.success);

    EXPECT_TRUE(
        result.command.name.empty());

    EXPECT_TRUE(
        result.command.arguments.empty());
}

TEST(
    Phase18CliTest,
    PreservesMissingArgumentsForDispatch) {
    const auto result =
        CommandParser::Parse(
            "write /file 0");

    ASSERT_TRUE(result.success);

    ASSERT_EQ(
        result.command.name,
        "write");

    ASSERT_EQ(
        result.command.arguments.size(),
        2U);
}

TEST(
    Phase18CliTest,
    PreservesUnknownCommandForDispatch) {
    const auto result =
        CommandParser::Parse(
            "not-a-command /path");

    ASSERT_TRUE(result.success);

    EXPECT_EQ(
        result.command.name,
        "not-a-command");

    ASSERT_EQ(
        result.command.arguments.size(),
        1U);

    EXPECT_EQ(
        result.command.arguments[0],
        "/path");
}

TEST(
    Phase18CliTest,
    ExitTerminatesAndUnknownCommandDoesNotCrash) {
    auto master =
        std::make_unique<
            gfs::client::metadata::MasterClient>(
            "127.0.0.1:5000");

    auto cache =
        std::make_unique<
            gfs::client::metadata::ChunkLocationCache>();

    gfs::client::GFSClient client(
        std::move(master),
        std::move(cache),
        [](
            const gfs::client::metadata::ChunkLocation&,
            gfs::ChunkHandle,
            std::uint64_t,
            std::size_t,
            std::string&) {
            return false;
        },
        [](
            const gfs::client::metadata::ChunkLocation&,
            gfs::ChunkHandle,
            std::uint64_t,
            const std::string&) {
            return false;
        });

    gfs::client::cli::CLI cli(client);

    std::istringstream input(
        "unknown-command\n"
        "exit\n");

    std::ostringstream output;

    EXPECT_EQ(
        cli.Run(
            input,
            output),
        0);

    EXPECT_NE(
        output.str().find(
            "Unknown command: unknown-command"),
        std::string::npos);
}

TEST(
    Phase18CliTest,
    MalformedCommandPrintsUsageAndContinues) {
    auto master =
        std::make_unique<
            gfs::client::metadata::MasterClient>(
            "127.0.0.1:5000");

    auto cache =
        std::make_unique<
            gfs::client::metadata::ChunkLocationCache>();

    gfs::client::GFSClient client(
        std::move(master),
        std::move(cache),
        [](
            const gfs::client::metadata::ChunkLocation&,
            gfs::ChunkHandle,
            std::uint64_t,
            std::size_t,
            std::string&) {
            return false;
        },
        [](
            const gfs::client::metadata::ChunkLocation&,
            gfs::ChunkHandle,
            std::uint64_t,
            const std::string&) {
            return false;
        });

    gfs::client::cli::CLI cli(client);

    std::istringstream input(
        "write /file 0\n"
        "quit\n");

    std::ostringstream output;

    EXPECT_EQ(
        cli.Run(
            input,
            output),
        0);

    EXPECT_NE(
        output.str().find(
            "Usage: write <path> <offset> <data>"),
        std::string::npos);
}

}  // namespace