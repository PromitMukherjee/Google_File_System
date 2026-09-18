# Phase 16 — Benchmarks + Paper Comparison

## 1. Benchmark environment

compiler=GCC 13.3.0;cxx_standard=20;os=Linux;cpu_count=2;chunk_size_bytes=67108864

The reproduction uses GitHub Codespaces/Linux with in-process Master and Chunkserver components. It is not hardware-equivalent to the 2003 GFS evaluation.

## 2. Methodology

Phase 16 uses steady_clock, deterministic data, the existing DistributedTestHarness, GFSClient, Master metadata APIs, Chunkserver I/O, RecordAppender, and recovery.

Default read dataset: 32 MiB; read region: 4 MiB; write file: 8 MiB.

## 3. Results

| Benchmark | Clients | Chunkservers | Replication | Bytes | Operations | Seconds | MB/s | Ops/s | Success | Failure |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| sequential_read | 1 | 3 | 3 | 8388608 | 2 | 0.518 | 15.454 | 3.864 | 1 | 0 |
| sequential_read | 2 | 3 | 3 | 16777216 | 4 | 1.013 | 15.802 | 3.951 | 2 | 0 |
| sequential_read | 4 | 4 | 3 | 33554432 | 8 | 1.964 | 16.292 | 4.073 | 4 | 0 |
| sequential_write | 1 | 3 | 3 | 8388608 | 8 | 0.487 | 16.432 | 16.432 | 1 | 0 |
| sequential_write | 2 | 3 | 3 | 16777216 | 16 | 1.110 | 14.409 | 14.409 | 2 | 0 |
| sequential_write | 4 | 4 | 3 | 33554432 | 32 | 2.017 | 15.867 | 15.867 | 4 | 0 |
| record_append | 1 | 3 | 3 | 8192 | 2 | 0.003 | 2.547 | 652.119 | 1 | 0 |
| record_append | 2 | 3 | 3 | 16384 | 4 | 0.009 | 1.794 | 459.271 | 2 | 0 |
| record_append | 4 | 4 | 3 | 32768 | 8 | 0.031 | 0.996 | 254.947 | 4 | 0 |
| chunkserver_scaling_read | 1 | 3 | 3 | 8388608 | 2 | 0.515 | 15.541 | 3.885 | 1 | 0 |
| master_metadata_lookup | 1 | 1 | 1 | 0 | 100 | 0.000 | 0.000 | 668811.322 | 1 | 0 |
| recovery_rereplication | 1 | 3 | 2 | 2097152 | 2 | 0.000 | 62287.832 | 62287.832 | 2 | 0 |
| replication_overhead | 1 | 3 | 1 | 8388608 | 8 | 0.510 | 15.681 | 15.681 | 1 | 0 |
| replication_overhead | 1 | 3 | 2 | 8388608 | 8 | 0.537 | 14.904 | 14.904 | 1 | 0 |
| replication_overhead | 1 | 3 | 3 | 8388608 | 8 | 0.490 | 16.339 | 16.339 | 1 | 0 |

## 4. Paper comparison

Paper values are labeled `PAPER_REPORTED`; benchmark measurements are `REPRODUCTION_MEASURED`. No accuracy score is calculated.

- Reads: approximately 10 MB/s for one client, 94 MB/s aggregate for 16 clients, and 125 MB/s theoretical limit.
- Writes: approximately 6.3 MB/s for one client, 35 MB/s aggregate for 16 clients, and 67 MB/s theoretical limit.
- Record append: approximately 6.0 MB/s for one client and 4.8 MB/s for 16 clients.
- Master load: approximately 200–500 operations/s in the reported real clusters.
- Recovery example: approximately 15,000 chunks / 600 GB, 23.2 minutes and 440 MB/s effective replication rate.

## 5. Architectural interpretation

The measurements expose relationships described in Section 6: master metadata operations are separated from bulk data I/O, replication increases write-side work, record append concentrates traffic on the shared file's last chunk, and recovery cost depends on data volume and available replicas.

## 6. Limitations

- The datasets are much smaller than the paper's 320 GB read set and 1 GB/client microbenchmarks.
- There is no physical network simulator.
- Master and Chunkservers run in-process.
- Timing depends on the Codespaces host.
- No throughput threshold causes a test failure.

## 7. Conclusion

Phase 16 provides repeatable measurements and a labeled comparison with the GFS Section 6 methodology and reported values. The results study the reproduction's architectural behavior rather than claiming hardware-equivalent performance.
