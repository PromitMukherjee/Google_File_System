#include <cstdint>

#include <gtest/gtest.h>

#include "gfs/common/constants.hpp"
#include "gfs/common/status.hpp"
#include "gfs/common/types.hpp"
#include "gfs/common/utils.hpp"

TEST(CommonTypesTest, TypesAreUsable) {
    const gfs::ChunkHandle handle = 42;
    const gfs::ChunkIndex index = 7;
    const gfs::ChunkVersion version = 3;
    const gfs::ServerId server = 11;
    const gfs::FilePath path = "/foo/bar";

    EXPECT_EQ(handle, 42);
    EXPECT_EQ(index, 7);
    EXPECT_EQ(version, 3);
    EXPECT_EQ(server, 11);
    EXPECT_EQ(path, "/foo/bar");
}

TEST(ConstantsTest, GFSConstantsMatchPaper) {
    EXPECT_EQ(
        gfs::constants::kChunkSize,
        64ULL * 1024ULL * 1024ULL
    );

    EXPECT_EQ(
        gfs::constants::kDefaultReplicationFactor,
        3ULL
    );

    EXPECT_EQ(
        gfs::constants::kChecksumBlockSize,
        64ULL * 1024ULL
    );
}

TEST(StatusTest, OKStatusIsSuccessful) {
    const gfs::Status status = gfs::Status::OK();

    EXPECT_TRUE(status.ok());
    EXPECT_EQ(
        status.code(),
        gfs::StatusCode::kOk
    );
    EXPECT_TRUE(status.message().empty());
    EXPECT_TRUE(static_cast<bool>(status));
}

TEST(StatusTest, ErrorStatusContainsCodeAndMessage) {
    const gfs::Status status =
        gfs::Status::NotFound("file not found");

    EXPECT_FALSE(status.ok());

    EXPECT_EQ(
        status.code(),
        gfs::StatusCode::kNotFound
    );

    EXPECT_EQ(
        status.message(),
        "file not found"
    );

    EXPECT_FALSE(static_cast<bool>(status));
}

TEST(UtilsTest, JoinPath) {
    EXPECT_EQ(
        gfs::JoinPath("/gfs", "file"),
        "/gfs/file"
    );

    EXPECT_EQ(
        gfs::JoinPath("/gfs/", "file"),
        "/gfs/file"
    );

    EXPECT_EQ(
        gfs::JoinPath("/gfs", "/file"),
        "/gfs/file"
    );

    EXPECT_EQ(
        gfs::JoinPath("/gfs/", "/file"),
        "/gfs/file"
    );
}

TEST(UtilsTest, ValidateFilePath) {
    EXPECT_TRUE(
        gfs::IsValidFilePath("/")
    );

    EXPECT_TRUE(
        gfs::IsValidFilePath("/gfs/file")
    );

    EXPECT_FALSE(
        gfs::IsValidFilePath("")
    );

    EXPECT_FALSE(
        gfs::IsValidFilePath("gfs/file")
    );
}

TEST(UtilsTest, UnixTimeMillisIsNonZero) {
    const std::uint64_t timestamp =
        gfs::UnixTimeMillis();

    EXPECT_GT(timestamp, 0ULL);
}