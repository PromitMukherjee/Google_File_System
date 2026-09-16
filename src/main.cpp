#include <iostream>

#include "gfs/common/constants.hpp"
#include "gfs/common/status.hpp"
#include "gfs/common/types.hpp"

int main() {
    const gfs::ChunkHandle chunk_handle = 1;
    const gfs::ChunkIndex chunk_index = 0;
    const gfs::ChunkVersion chunk_version = 1;
    const gfs::ServerId server_id = 1;
    const gfs::FilePath file_path = "/gfs/example";

    const gfs::Status status = gfs::Status::OK();

    std::cout << "GFS-CPP\n";

    std::cout << "Chunk handle: "
              << chunk_handle << '\n';

    std::cout << "Chunk index: "
              << chunk_index << '\n';

    std::cout << "Chunk version: "
              << chunk_version << '\n';

    std::cout << "Server ID: "
              << server_id << '\n';

    std::cout << "File path: "
              << file_path << '\n';

    std::cout << "Chunk size: "
              << gfs::constants::kChunkSize /
                     (1024ULL * 1024ULL)
              << " MB\n";

    std::cout << "Replication factor: "
              << gfs::constants::kDefaultReplicationFactor
              << '\n';

    std::cout << "Checksum block size: "
              << gfs::constants::kChecksumBlockSize /
                     1024ULL
              << " KB\n";

    std::cout << "Status: "
              << (status.ok() ? "OK" : "ERROR")
              << '\n';

    return status.ok() ? 0 : 1;
}