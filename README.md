# Google File System --- GFS-CPP

> **A paper-faithful C++20 reproduction of the 2003 Google File System
> (GFS) architecture**
>
> Built from the original SOSP'03 paper, implemented incrementally from
> the repository foundation through failure injection, distributed
> testing, benchmarking, and paper comparison.

**Master • Chunkservers • Clients • Replication • Leases • Recovery •
Snapshots • GC • Rebalancing • Benchmarks**



------------------------------------------------------------------------

## 🧭 What is this project?

**GFS-CPP** is a from-scratch C++20 reproduction of the architecture
described in:

> Sanjay Ghemawat, Howard Gobioff, and Shun-Tak Leung,\
> **"The Google File System"**, SOSP 2003.

The goal is **not** to reproduce Google's private production source
code.

The goal is to make the important ideas in the paper concrete through a
readable, testable implementation:

``` text
                    ┌──────────────────────────────┐
                    │        Applications          │
                    └──────────────┬───────────────┘
                                   │
                                   ▼
                    ┌──────────────────────────────┐
                    │         GFS Client            │
                    │                              │
                    │ lookup • read • write        │
                    │ append • snapshot             │
                    └──────────────┬───────────────┘
                                   │
                    metadata/control│
                                   ▼
              ┌─────────────────────────────────────────┐
              │                 MASTER                  │
              │                                         │
              │ Namespace                               │
              │ File → Chunk mapping                    │
              │ Chunk metadata                          │
              │ Replica locations                       │
              │ Leases / primary selection              │
              │ Heartbeats / failure detection          │
              │ Re-replication / recovery               │
              │ Garbage collection / rebalancing        │
              │ Operation log / checkpoint              │
              │ Snapshot / copy-on-write                │
              └───────────────┬─────────────────────────┘
                              │ control
                ┌─────────────┼─────────────┐
                │             │             │
                ▼             ▼             ▼
          ┌──────────┐   ┌──────────┐   ┌──────────┐
          │Chunkserver│  │Chunkserver│  │Chunkserver│
          │    #1     │  │    #2     │  │    #3     │
          ├──────────┤   ├──────────┤   ├──────────┤
          │ chunks   │   │ chunks   │   │ chunks   │
          │ checksum │   │ checksum │   │ checksum │
          │ mutation │   │ mutation │   │ mutation │
          └──────────┘   └──────────┘   └──────────┘
                ▲             ▲             ▲
                └─────────────┴─────────────┘
                         data path
```

The central architectural idea is preserved:

> **The Master manages metadata and control; clients and chunkservers
> carry the actual data path.**

The original paper explicitly describes a single master, multiple
chunkservers, multiple clients, fixed-size chunks, replication, master
metadata, leases, heartbeats, and direct client↔chunkserver data
transfer. fileciteturn13file0L133-L167

------------------------------------------------------------------------

# ✨ Project goals

This project is designed around five goals:

  -----------------------------------------------------------------------
  Goal                                Meaning
  ----------------------------------- -----------------------------------
  📖 **Paper fidelity**               Implement the architectural ideas
                                      described by the 2003 paper

  🧩 **Readable source**              Keep the implementation
                                      understandable instead of hiding
                                      everything behind frameworks

  🧪 **Testability**                  Each major subsystem has focused
                                      GoogleTest coverage

  💥 **Failure realism**              Treat failures as a normal
                                      distributed-systems condition

  📊 **Measurement**                  Measure the reproduction and
                                      compare its behavior with the
                                      paper's reported results
  -----------------------------------------------------------------------

The project deliberately avoids turning GFS into a generic modern
distributed filesystem with unrelated features.

------------------------------------------------------------------------

# 🗺️ Architecture at a glance

## 1. Master

The Master is the control plane.

It maintains and coordinates:

-   namespace
-   file metadata
-   file → chunk mapping
-   chunk metadata
-   replica locations
-   leases
-   primary/secondary mutation ordering
-   chunk placement
-   heartbeats
-   failure detection
-   re-replication
-   recovery
-   garbage collection
-   rebalancing
-   operation log
-   checkpoint/recovery
-   snapshot metadata
-   copy-on-write decisions

The paper states that the Master maintains namespace, access-control
information, file-to-chunk mappings, and current chunk locations, while
also controlling leases, garbage collection, migration, and heartbeat
exchanges. fileciteturn13file0L245-L261

------------------------------------------------------------------------

## 2. Chunkservers

Chunkservers are responsible for the physical chunk data.

The implementation includes:

``` text
Chunkserver
├── Storage
│   ├── ChunkStorage
│   ├── ChunkFile
│   └── StorageManager
│
├── Checksum
│   ├── ChecksumManager
│   └── ChecksumBlock
│
├── Replication
│   ├── ReplicaReceiver
│   ├── ReplicaSender
│   └── CloneManager
│
├── Mutation
│   ├── MutationManager
│   └── Mutation
│
└── Heartbeat
    └── HeartbeatClient
```

The paper specifies 64 MB chunks and states that each replica is stored
as a plain Linux file on a chunkserver. fileciteturn13file0L141-L148
fileciteturn13file0L211-L217

------------------------------------------------------------------------

## 3. Client

The GFS client provides the application-facing interface.

``` text
GFSClient
├── MasterClient
├── ChunkLocationCache
├── Reader
├── Writer
├── RecordAppender
└── RetryPolicy
```

The client contacts the Master for metadata, but data-bearing operations
go directly to chunkservers. The original design also caches
chunk-location metadata to reduce repeated Master interaction.
fileciteturn13file0L190-L210

------------------------------------------------------------------------

# 🔬 Core GFS data flow

## Read path

``` text
Application
    │
    ▼
GFSClient
    │
    │ file + chunk index
    ▼
Master
    │
    │ chunk handle + replica locations
    ▼
GFSClient
    │
    │ chunk handle + byte range
    ▼
Chunkserver
    │
    ▼
Local chunk file
```

After the client obtains the chunk location, subsequent reads can go
directly to a replica without another Master lookup until the cached
metadata expires or the file is reopened.
fileciteturn13file0L197-L210

------------------------------------------------------------------------

## Write path

The project models the paper's primary/secondary mutation architecture:

``` text
                 ┌──────────────┐
                 │    Master    │
                 │ lease holder │
                 └──────┬───────┘
                        │
                        ▼
                    Primary
                       │
              serializes mutation
                       │
             ┌─────────┼─────────┐
             ▼         ▼         ▼
          Secondary Secondary Secondary
```

Conceptually:

1.  Client asks the Master for the lease holder and replicas.
2.  Data is pushed to replicas.
3.  Client asks the primary to perform the mutation.
4.  Primary assigns mutation order.
5.  Secondaries apply the same order.
6.  Replicas acknowledge.
7.  Primary reports the result to the client.

This follows the control/data separation described in the paper.
fileciteturn13file0L433-L450 fileciteturn13file0L483-L517

------------------------------------------------------------------------

# 🧱 Repository structure

``` text
Google_File_System/
│
├── .devcontainer/
│   └── devcontainer.json
│
├── proto/
│   ├── master.proto
│   ├── chunkserver.proto
│   └── client.proto
│
├── include/gfs/
│   ├── common/
│   ├── master/
│   │   ├── metadata/
│   │   ├── namespace/
│   │   ├── lease/
│   │   ├── heartbeat/
│   │   ├── replication/
│   │   ├── recovery/
│   │   └── garbage_collection/
│   │
│   ├── chunkserver/
│   │   ├── storage/
│   │   ├── checksum/
│   │   ├── replication/
│   │   ├── mutation/
│   │   └── heartbeat/
│   │
│   ├── client/
│   │   ├── metadata/
│   │   ├── io/
│   │   └── retry/
│   │
│   ├── protocol/
│   ├── storage/
│   ├── testing/
│   └── benchmark/
│
├── src/
│   ├── common/
│   ├── master/
│   ├── chunkserver/
│   ├── client/
│   ├── protocol/
│   ├── storage/
│   ├── testing/
│   └── benchmark/
│
├── tests/
│   ├── common_test.cpp
│   ├── master_test.cpp
│   ├── chunkserver_test.cpp
│   ├── client_test.cpp
│   ├── replication_test.cpp
│   ├── phase7_test.cpp
│   ├── phase8_test.cpp
│   ├── phase9_test.cpp
│   ├── phase10_test.cpp
│   ├── phase11_test.cpp
│   ├── phase12_test.cpp
│   ├── phase13_test.cpp
│   ├── phase14_test.cpp
│   ├── phase15_test.cpp
│   └── phase16_test.cpp
│
├── docs/
│   └── phase16_benchmark_report.md
│
├── phase16_benchmark_results.csv
├── CMakeLists.txt
├── LICENSE
└── README.md
```

------------------------------------------------------------------------

# 🚀 Development roadmap

The implementation was developed as a sequence of focused phases.

    Phase Subsystem             Main outcome
  ------- --------------------- ------------------------------------------------
        0 Environment           GitHub/Codespaces/toolchain foundation
        1 Repository            C++20 project + CMake + common types
        2 Protocol              Protobuf/gRPC foundation
        3 Master metadata       Namespace + metadata management
        4 Chunkserver storage   Persistent local chunk storage
        5 Client I/O            Client + basic read/write
        6 Replication           Replica management + placement
        7 Leases                Lease management + mutation ordering
        8 Heartbeats            Failure detection + chunkserver state
        9 Checksums             64 KB checksum blocks + corruption detection
       10 Recovery              Re-replication + stale/failed replica recovery
       11 Persistence           Operation log + checkpoint + Master recovery
       12 Record append         Atomic append semantics + retry behavior
       13 Snapshot              Snapshot + copy-on-write
       14 Maintenance           Garbage collection + rebalancing
       15 Failures              Failure injection + distributed test harness
       16 Measurement           Benchmarks + paper comparison

### 🏁 Final project milestone

**Phase 16 is the measurement layer.**

It does not introduce another filesystem subsystem. Instead, it asks:

> **How does the implemented architecture behave when measured?**

------------------------------------------------------------------------

# 🧪 Testing strategy

The project uses **GoogleTest**.

Each major phase has its own test target so that a subsystem can be
validated independently.

Examples:

``` text
gfs_common_test
gfs_master_test
gfs_chunkserver_test
gfs_client_test
gfs_replication_test

gfs_phase7_test
gfs_phase8_test
gfs_phase9_test
gfs_phase10_test
gfs_phase11_test
gfs_phase12_test
gfs_phase13_test
gfs_phase14_test
gfs_phase15_test
gfs_phase16_test
```

Phase 15 adds controlled failure injection and a distributed test
harness.

Phase 16 adds benchmark smoke tests covering:

-   benchmark arithmetic
-   benchmark execution
-   CSV export
-   environment reporting
-   report generation
-   read benchmark
-   write benchmark
-   record append benchmark
-   Master benchmark
-   recovery benchmark

------------------------------------------------------------------------

# 💥 Failure-oriented design

Failure is not treated as an unusual exception.

The implementation includes:

``` text
Heartbeat
   │
   ▼
Failure detection
   │
   ▼
Under-replication / stale state
   │
   ▼
Recovery Manager
   │
   ▼
Re-Replication Manager
   │
   ▼
ReplicaSender → ReplicaReceiver
   │
   ▼
Healthy replica set restored
```

The paper describes component failures as routine and makes monitoring,
error detection, fault tolerance, and automatic recovery integral to the
design. fileciteturn13file0L47-L55

It also describes re-replication after chunkserver failure and cloning
from an existing valid replica. fileciteturn13file0L690-L723

------------------------------------------------------------------------

# 🧮 Checksums and data integrity

The implementation includes checksum handling at the chunkserver layer.

The paper divides each chunk into **64 KB blocks**, with a 32-bit
checksum for each block. Reads verify the checksums covering the
requested range before returning data. fileciteturn13file0L875-L900

This gives the project an explicit integrity path:

``` text
Read request
     │
     ▼
Determine affected checksum blocks
     │
     ▼
Calculate / verify checksum
     │
 ┌───┴────┐
 │        │
valid   mismatch
 │        │
 ▼        ▼
data    error
         │
         ▼
      other replica
```

------------------------------------------------------------------------

# 🧬 Record append

GFS record append is intentionally different from a normal positional
write.

``` text
Normal write:
    client chooses offset

Record append:
    GFS chooses offset
```

The paper defines record append as an atomic append-at-least-once
operation whose offset is selected by GFS. It also permits padding or
duplicates in failure cases. fileciteturn13file0L371-L390

The implementation therefore tests:

-   record-size limits
-   primary offset selection
-   propagation
-   chunk-boundary padding
-   retry behavior
-   readability after append

------------------------------------------------------------------------

# 📸 Snapshot + Copy-on-Write

Snapshot creation avoids immediately copying the physical chunk data.

Conceptually:

``` text
Before snapshot

source
 ├── C1
 ├── C2
 └── C3


After snapshot

source     ──┐
             ├── C1
snapshot   ──┤
             ├── C2
             └── C3


First write to shared C2

source     ───── C2'
snapshot   ───── C2
```

The paper describes revoking/expiring leases, duplicating metadata,
sharing the original chunks, and creating a new chunk only when a shared
chunk is first modified. fileciteturn13file0L588-L618

The implementation specifically preserves chunk ordering when a
copy-on-write replacement occurs.

------------------------------------------------------------------------

# 🗑️ Garbage collection

GFS uses lazy reclamation.

Instead of immediately deleting every physical replica after a file
deletion:

``` text
Delete file
    │
    ▼
Metadata no longer references chunks
    │
    ▼
Background scan
    │
    ▼
Identify orphan chunks
    │
    ▼
Chunkservers are instructed
to remove unneeded replicas
```

The paper explains that deleted files are first hidden, later removed
from the namespace, and then orphaned chunks can be identified from the
master-maintained file-to-chunk mappings.
fileciteturn13file0L733-L756

This design also interacts naturally with snapshots: a chunk shared by a
live file and a snapshot is not orphaned until all references disappear.

------------------------------------------------------------------------

# ⚖️ Rebalancing

The Master can redistribute replicas to improve placement and disk/load
balance.

The paper describes rebalancing as a periodic activity that considers
disk-space and load distribution while avoiding suddenly overwhelming a
new chunkserver. fileciteturn13file0L724-L732

In this reproduction, rebalancing is part of the Master replication
subsystem rather than a separate storage service.

------------------------------------------------------------------------

# 💾 Operation log + checkpoint recovery

The Master does not persist every piece of runtime replica-location
state in the same way.

The paper distinguishes persistent metadata from runtime chunk-location
knowledge:

``` text
Persistent:
    Namespace
    File → chunk mapping
    Critical metadata mutations
          │
          ▼
    Operation log
          │
          ▼
      Checkpoint


Runtime / reconstructed:
    Chunk locations
          │
          ▼
    Chunkserver reports
    + heartbeats
```

The operation log provides a historical ordering of metadata mutations,
while checkpoints reduce the amount of log replay needed during
recovery. fileciteturn13file0L296-L338

This distinction is important because the paper explicitly says the
Master does **not** persistently store chunk-location information; it
learns replica locations from chunkservers.
fileciteturn13file0L276-L295

------------------------------------------------------------------------

# 📊 Phase 16 --- Benchmarking

Phase 16 introduces a dedicated benchmark layer:

``` text
include/gfs/benchmark/benchmark.hpp
             │
             ▼
src/benchmark/benchmark.cpp
             │
      ┌──────┴────────┐
      ▼               ▼
src/benchmark/    gfs_benchmark
main.cpp
      │
      ├── CSV
      │
      └── Markdown report
```

## Benchmark categories

The suite measures:

-   sequential reads
-   sequential writes
-   record append
-   chunkserver scaling
-   Master metadata operations
-   recovery / re-replication
-   replication-factor overhead

The benchmark configuration supports varying:

``` text
clients
chunkservers
replication factor
read workload
write workload
append workload
master operations
recovery workload
```

------------------------------------------------------------------------

# ⏱️ Benchmark methodology

The benchmark layer uses:

-   `std::chrono::steady_clock`
-   deterministic generated data
-   existing `GFSClient`
-   existing `DistributedTestHarness`
-   actual Master metadata paths
-   actual chunkserver storage paths
-   actual record append paths
-   actual recovery paths

It deliberately avoids machine-specific pass/fail thresholds.

### Important distinction

These numbers measure **this implementation on the current execution
environment**.

They are not a claim that this Codespaces implementation reproduces the
absolute performance of the 2003 Google hardware.

------------------------------------------------------------------------

# 📈 Current benchmark snapshot

The repository includes:

``` text
phase16_benchmark_results.csv
docs/phase16_benchmark_report.md
```

The recorded snapshot contains measurements for:

  -----------------------------------------------------------------------
  Benchmark                                       Example measured result
  ------------------------------ ----------------------------------------
  Sequential read, 1 client                                    15.45 MB/s

  Sequential read, 4 clients                         16.29 MB/s aggregate

  Sequential write, 1 client                                   16.43 MB/s

  Sequential write, 4 clients                        15.87 MB/s aggregate

  Record append, 1 client                                       2.55 MB/s

  Record append, 4 clients                                      1.00 MB/s

  Master metadata lookup                \~668,811 ops/s in the in-process
                                                                benchmark

  Recovery/re-replication              \~62,288 operations/s for the tiny
                                                      configured workload
  -----------------------------------------------------------------------

These values are **benchmark measurements from the included Codespaces
run**, not paper values.

The benchmark CSV records the workload configuration and paper reference
alongside each result.

------------------------------------------------------------------------

# 📚 Comparison with the 2003 GFS paper

The original paper's microbenchmarks used a very different environment:

``` text
1 Master
2 Master replicas
16 Chunkservers
16 Clients
dual 1.4 GHz Pentium III
2 GB RAM
two 80 GB 5400 rpm disks
100 Mbps Ethernet
1 Gbps inter-switch link
```

The paper reports:

  -----------------------------------------------------------------------
  Workload                            Paper result
  ----------------------------------- -----------------------------------
  Read, 1 client                      \~10 MB/s

  Read, 16 clients                    \~94 MB/s aggregate

  Read theoretical limit              125 MB/s

  Write, 1 client                     \~6.3 MB/s

  Write, 16 clients                   \~35 MB/s aggregate

  Write theoretical limit             67 MB/s

  Record append, 1 client             \~6.0 MB/s

  Record append, 16 clients           \~4.8 MB/s

  Real-cluster Master load            \~200--500 ops/s

  Recovery example                    \~15,000 chunks / 600 GB restored
                                      in 23.2 min at \~440 MB/s effective
                                      replication rate
  -----------------------------------------------------------------------

The paper's microbenchmark setup and read/write/append measurements are
documented in Section 6. fileciteturn13file0L948-L971
fileciteturn13file0L973-L1017

The recovery experiment is described separately in Section 6.2.5.
fileciteturn13file0L1143-L1158

### ⚠️ How to interpret this comparison

Do **not** read the comparison as:

``` text
our number > paper number
        ↓
our GFS is better
```

or:

``` text
our number < paper number
        ↓
our GFS is worse
```

That conclusion would not be scientifically meaningful because the
workloads, hardware, process model, network, storage, dataset size, and
execution environment differ.

The useful question is:

> **Does the measured behavior expose the architectural relationships
> predicted by the GFS design?**

For example:

-   replication adds write-side work
-   record append concentrates traffic around the last chunk
-   Master metadata work is separate from bulk data transfer
-   recovery depends on available replicas and transferred data
-   scaling clients changes contention patterns

------------------------------------------------------------------------

# 🧠 What the benchmark is actually teaching

The most interesting part of Phase 16 is not a single MB/s number.

It is the relationship between **architecture and performance**.

## Master involvement

``` text
More Master work
       │
       ▼
Metadata/control overhead
       │
       ▼
Should remain small compared with
bulk data transfer
```

The paper explicitly emphasizes minimizing Master involvement in reads
and writes so that the Master does not become the bottleneck.
fileciteturn13file0L168-L196

## Replication

``` text
1 replica
   │
   ▼
less write-side replication work

3 replicas
   │
   ▼
more data must reach storage replicas
   │
   ▼
higher reliability
```

The paper's write benchmark also accounts for the cost of writing each
byte to three chunkservers. fileciteturn13file0L985-L1002

## Record append

``` text
Many clients
     │
     ▼
one shared file
     │
     ▼
same last chunk
     │
     ▼
shared chunkserver bottleneck
```

The paper explicitly notes that record-append performance is limited by
the network bandwidth of the chunkservers holding the last chunk of the
shared file. fileciteturn13file0L1007-L1017

------------------------------------------------------------------------

# 🛠️ Build

Inside GitHub Codespaces:

``` bash
cmake -S . -B build
cmake --build build -j2
```

The project is configured for C++20.

Typical toolchain used during development:

``` text
GCC
C++20
CMake
Protobuf
gRPC
GoogleTest
Linux filesystem
```

------------------------------------------------------------------------

# 🧪 Run the complete test suite

``` bash
ctest --test-dir build --output-on-failure
```

For a clean rebuild:

``` bash
rm -rf build
cmake -S . -B build
cmake --build build -j2
ctest --test-dir build --output-on-failure
```

------------------------------------------------------------------------

# 📊 Run Phase 16 benchmarks

Build:

``` bash
cmake --build build -j2
```

Run the default benchmark suite:

``` bash
./build/gfs_benchmark
```

This writes:

``` text
phase16_benchmark_results.csv
docs/phase16_benchmark_report.md
```

------------------------------------------------------------------------

## ⚡ Quick benchmark mode

For a smaller run:

``` bash
./build/gfs_benchmark --quick
```

The quick configuration reduces:

``` text
clients
read workload
write workload
append workload
Master operations
recovery workload
```

This is useful for development and validation.

------------------------------------------------------------------------

## 🎛️ Custom benchmark configuration

The benchmark executable supports:

``` bash
./build/gfs_benchmark \
  --clients 1,2,4,8,16 \
  --chunkservers 3,4,8,16 \
  --replication 1,2,3 \
  --output phase16_benchmark_results.csv \
  --report docs/phase16_benchmark_report.md
```

See:

``` bash
./build/gfs_benchmark --help
```

for the current command-line interface.

------------------------------------------------------------------------

# 📦 Benchmark output format

The CSV contains fields such as:

``` text
benchmark
configuration
clients
chunkservers
replication_factor
bytes
operations
elapsed_seconds
throughput_mb_per_sec
operations_per_sec
successes
failures
paper_reference
paper_value
notes
```

This makes the results easy to analyze with:

-   Python
-   Pandas
-   Excel
-   Google Sheets
-   R
-   custom plotting scripts

------------------------------------------------------------------------

# 🔍 How to reproduce a benchmark experiment

A useful workflow is:

``` text
1. Clean build
      │
      ▼
2. Run full tests
      │
      ▼
3. Run benchmark
      │
      ▼
4. Preserve CSV
      │
      ▼
5. Inspect generated report
      │
      ▼
6. Change one workload parameter
      │
      ▼
7. Repeat
      │
      ▼
8. Compare architectural trends
```

For example:

``` bash
./build/gfs_benchmark --clients 1,2,4
```

Then:

``` bash
./build/gfs_benchmark --clients 1,2,4,8,16
```

The goal is to observe how the system changes as concurrency increases.

------------------------------------------------------------------------

# 🧩 Technology stack

  Layer              Technology
  ------------------ -----------------------------------
  Language           C++20
  Build              CMake
  RPC                gRPC
  Serialization      Protocol Buffers
  Testing            GoogleTest
  Storage            Linux local filesystem
  Concurrency        C++ standard threading primitives
  Benchmark timing   `std::chrono::steady_clock`
  Environment        GitHub Codespaces / Linux

------------------------------------------------------------------------

# 🎯 Paper-to-code mapping

  GFS paper concept     GFS-CPP subsystem
  --------------------- -------------------------------------------
  Single Master         `gfs::master::Master`
  Namespace             `NamespaceManager`
  File → chunks         metadata subsystem
  Chunk replicas        `ReplicaManager`
  Placement             `PlacementPolicy`
  Leases                `LeaseManager`
  Mutation ordering     `MutationManager`
  Heartbeats            `HeartbeatManager` / `HeartbeatClient`
  Checksums             `ChecksumManager` / `ChecksumBlock`
  Re-replication        `ReReplicationManager`
  Recovery              `RecoveryManager`
  Operation log         `OperationLog`
  Checkpoint            `Checkpoint`
  Record append         `RecordAppender`
  Snapshot              Master snapshot logic
  Copy-on-write         Master COW logic
  Garbage collection    `GarbageCollector` / `OrphanChunkManager`
  Rebalancing           `Rebalancer`
  Failure injection     `FailureInjector`
  Distributed testing   `DistributedTestHarness`
  Benchmarking          `BenchmarkSuite` / `BenchmarkRunner`

------------------------------------------------------------------------

# 📐 Important GFS design parameters

The reproduction keeps several important paper parameters visible in the
implementation.

## Chunk size

``` text
64 MB
```

The paper selected 64 MB chunks to reduce Master interaction, reduce
metadata size, and allow clients to perform many operations on a chunk.
fileciteturn13file0L211-L230

## Replication

``` text
Default conceptual model:
3 replicas
```

The paper states that three replicas were used by default, while
allowing different replication levels for different namespace regions.
fileciteturn13file0L141-L148

## Checksum block

``` text
64 KB
```

The paper specifies 64 KB checksum blocks.
fileciteturn13file0L887-L900

## Lease

``` text
Initial lease timeout:
60 seconds
```

The paper describes a 60-second initial lease timeout, with extensions
while mutations continue. fileciteturn13file0L433-L450

------------------------------------------------------------------------

# 🧠 Why the architecture looks unusual

A traditional filesystem might emphasize:

``` text
small blocks
POSIX semantics
random writes
local caching
low latency
```

GFS was designed around a different workload:

``` text
huge files
large sequential reads
large sequential writes
append-heavy workloads
many concurrent clients
frequent component failures
high aggregate throughput
```

The paper explicitly states that high sustained bandwidth is more
important than low latency for its target applications.
fileciteturn13file0L101-L122

That explains several otherwise surprising choices:

``` text
Large chunks
     ↓
Less metadata + fewer Master lookups

No client data cache
     ↓
Avoid cache-coherence complexity

Single Master
     ↓
Global metadata decisions stay simple

Leases
     ↓
Mutation authority delegated to a primary

Replication
     ↓
Fast recovery from failures

Checksums
     ↓
Detect storage corruption independently

Record append
     ↓
Efficient concurrent producer workloads

Snapshot + COW
     ↓
Cheap dataset branching

Lazy GC
     ↓
Simpler and safer reclamation
```

------------------------------------------------------------------------

# 🧪 Reproducibility notes

Benchmark results are affected by the execution environment.

The repository therefore records environment information such as:

``` text
compiler
C++ standard
operating system
CPU count
chunk size
```

The benchmark intentionally does **not** define machine-specific
throughput thresholds that cause CI/test failures.

A benchmark should answer:

> "What did this implementation achieve under this workload and
> environment?"

rather than:

> "Did this machine reach an arbitrary hard-coded MB/s target?"

------------------------------------------------------------------------

# ⚠️ What this project does NOT claim

This project does **not** claim:

-   access to Google's original GFS implementation
-   production equivalence with Google's internal GFS
-   hardware-equivalent reproduction of the 2003 experiments
-   identical networking behavior
-   identical disk behavior
-   identical scheduling behavior
-   identical cluster-scale performance
-   a replacement for modern distributed storage systems

The benchmark comparison is therefore intentionally labeled:

``` text
PAPER_REPORTED
        vs.
REPRODUCTION_MEASURED
```

rather than presenting an artificial "accuracy score".

------------------------------------------------------------------------

# 📖 Understanding the original paper through the code

A useful reading order is:

``` text
Paper Section 2
    ↓
Architecture
    ↓
Master / Chunkserver / Client

Paper Section 3
    ↓
Leases
Mutation ordering
Record append
Snapshot
    ↓
Phase 7 / 12 / 13

Paper Section 4
    ↓
Master operation
Placement
Recovery
GC
Stale replicas
    ↓
Phase 3 / 6 / 8 / 10 / 14

Paper Section 5
    ↓
Fault tolerance
Checksums
    ↓
Phase 9 / 10 / 15

Paper Section 6
    ↓
Measurements
    ↓
Phase 16
```

The original paper's design overview explicitly connects the
architecture, large chunks, metadata model, leases, consistency model,
and background Master activities. fileciteturn13file0L87-L170

------------------------------------------------------------------------

# 🏗️ Engineering philosophy

The implementation follows a few practical rules:

### 1. Source code is the executable specification

The architecture should be understandable by reading the classes and
their interactions.

### 2. Tests protect behavior

Every major subsystem is exercised through focused tests.

### 3. Failure is a first-class state

A distributed filesystem cannot be designed assuming every machine is
healthy.

### 4. Measure behavior, not marketing numbers

Benchmark results are recorded with their configuration and environment.

### 5. Keep paper facts separate from reproduction measurements

``` text
Paper:
    what Google reported in 2003

Reproduction:
    what this implementation measured now
```

------------------------------------------------------------------------

# 📊 Phase 16 files

The measurement layer is intentionally small:

``` text
include/gfs/benchmark/benchmark.hpp
        │
        ├── BenchmarkResult
        ├── BenchmarkRunner
        ├── CsvWriter
        ├── BenchmarkConfig
        └── BenchmarkSuite
                │
                ├── RunRead
                ├── RunWrite
                ├── RunRecordAppend
                ├── RunChunkserverScaling
                ├── RunMaster
                ├── RunRecovery
                └── RunReplication

src/benchmark/benchmark.cpp
src/benchmark/main.cpp
tests/phase16_test.cpp

docs/phase16_benchmark_report.md
phase16_benchmark_results.csv
```

------------------------------------------------------------------------

# 🧭 Recommended workflow for contributors

``` bash
# 1. Configure
cmake -S . -B build

# 2. Build
cmake --build build -j2

# 3. Test
ctest --test-dir build --output-on-failure

# 4. Benchmark
./build/gfs_benchmark --quick

# 5. Full benchmark when appropriate
./build/gfs_benchmark
```

After changing implementation code:

``` text
Build
  ↓
Tests
  ↓
Benchmark
  ↓
Inspect CSV
  ↓
Inspect report
```

------------------------------------------------------------------------

# 🏁 Project status

## Completed

``` text
[████████████████████████████████] Phase 0–16
```

The implementation roadmap currently ends at:

> **Phase 16 --- Benchmarks + Paper Comparison**

The benchmark layer is the final measurement stage of this reproduction
roadmap.

------------------------------------------------------------------------

# 📌 Quick reference

### Build

``` bash
cmake -S . -B build
cmake --build build -j2
```

### Test

``` bash
ctest --test-dir build --output-on-failure
```

### Run application

``` bash
./build/gfs
```

### Run benchmark

``` bash
./build/gfs_benchmark
```

### Quick benchmark

``` bash
./build/gfs_benchmark --quick
```

### Benchmark help

``` bash
./build/gfs_benchmark --help
```

------------------------------------------------------------------------

# 📚 Primary reference

**The Google File System**\
Sanjay Ghemawat, Howard Gobioff, Shun-Tak Leung\
ACM SOSP 2003

The uploaded paper is the primary behavioral/design reference for this
reproduction. Its architecture, metadata model, leases, mutation
ordering, snapshot/COW, garbage collection, checksums, fault tolerance,
and measurements are used as the conceptual reference throughout the
project. fileciteturn13file0L39-L73 fileciteturn13file0L430-L432

------------------------------------------------------------------------

```{=html}
<p align="center">
```
### 🗄️ GFS-CPP

**From a 2003 distributed-systems paper → to a complete C++20
implementation → to measurable experiments.**

```{=html}
</p>
```

