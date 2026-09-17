#include "gfs/chunkserver/mutation/mutation.hpp"

namespace gfs::chunkserver::mutation {

bool Mutation::IsValid() const noexcept {
    return chunk_handle != 0 &&
           chunk_version != 0 &&
           mutation_id != 0 &&
           FitsChunk();
}

bool Mutation::FitsChunk() const noexcept {
    if (chunk_handle == 0 ||
        offset > gfs::constants::kChunkSize) {
        return false;
    }

    return static_cast<std::uint64_t>(data.size()) <=
               gfs::constants::kChunkSize - offset;
}

}  // namespace gfs::chunkserver::mutation