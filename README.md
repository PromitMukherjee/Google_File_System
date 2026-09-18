# Google_File_System
## 1. Benchmark environment

Phase 16 measures the existing GFS reproduction in GitHub Codespaces/Linux using in-process Master and Chunkserver components and the local filesystem.

The environment is **not hardware-equivalent** to the 2003 GFS evaluation.

The benchmark captures:

- compiler
- C++ standard
- operating system
- CPU count where available
- GFS chunk size

Dataset sizes are intentionally configurable and substantially smaller than the paper's workloads.

## 2. Methodology

The benchmark suite uses:

- `std::chrono::steady_clock`
- deterministic generated data
- existing `DistributedTestHarness`
- existing `GFSClient`
- Master metadata APIs
- Chunkserver I/O
- existing `RecordAppender`
- existing recovery APIs

The Section 6 methodology is approximated rather than hardware-reproduced.

The original paper used:

- 1 master
- 2 master replicas
- 16 chunkservers
- 16 clients
- physical Ethernet
- 1 Gbps inter-switch connection

Phase 16 does not emulate that physical topology.

## 3. Read results

`sequential_read` measures data reads through `GFSClient::Read`.

The default configuration uses:

- 128 MiB file
- 4 MiB read region
- deterministic data
- configurable client counts

Paper reference:

- approximately 10 MB/s at one client
- approximately 94 MB/s aggregate at 16 clients
- 125 MB/s theoretical aggregate limit

## 4. Write results

`sequential_write` uses distinct files for each client and the existing `GFSClient::Write` path.

The default configuration uses:

- 32 MiB per file
- 1 MiB writes
- replication factor 3

Paper reference:

- approximately 6.3 MB/s at one client
- approximately 35 MB/s aggregate at 16 clients
- approximately 67 MB/s theoretical limit

## 5. Record append results

`record_append` uses:

```text
GFSClient
    |
    v
RecordAppender
    |
    v
MutationManager
    |
    v
Primary + replicas