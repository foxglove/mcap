# MCAP chunk-size study

**Question:** what chunk size should an MCAP writer default to, and what are we
actually trading off when we change it?

This study sweeps the C++ MCAP writer's chunk size across four message-size
classes, two compression formats, and several read access patterns, and models
read cost for both local and remote (object-store) storage. C++ is the writer
behind ROS 2 `rosbag2` MCAP recordings, so it is the most representative
implementation for a robotics recorder default. The corpus model is adapted
from [foxglove/mcap#1611](https://github.com/foxglove/mcap/pull/1611).

All numbers below come from `results/raw.tsv` (regenerate with
`./run.sh && python3 analyze.py`). Each point is the median of 3 write / 5 read
iterations over ~256 MiB of uncompressed payload per corpus, on a 4-core Linux
VM with the file in page cache.

## TL;DR

- **Compression ratio is essentially flat from 256 KiB to 32 MiB** for every
  content type. zstd's window already captures the available redundancy inside
  a small chunk; making chunks bigger buys almost nothing. The common "bigger
  chunks compress better" intuition does **not** hold in this range.
- **Write throughput is also flat** across chunk size.
- **Bigger chunks have real, monotonic costs:** reader peak memory grows ~10×
  (≈7 MiB → ≈65 MiB from 1 MiB → 32 MiB), and single-message / random reads get
  linearly more expensive because the reader must fetch and decompress a whole
  chunk to extract one message.
- **The one place bigger chunks help is bulk/streaming reads over high-latency
  remote storage**, where fewer, larger ranged GETs cut round-trip overhead.
  This benefit is real (2–4×) but is mostly realized by ~4–8 MiB and saturates
  after that.
- **Recommended default: 4 MiB.** It is the knee of every curve: it captures
  all of the compression ratio and (within run-to-run noise) the full write
  throughput, cuts remote streaming
  latency 39–62% vs 1 MiB, and costs only +3.6 MiB reader RSS and +6 ms
  single-message read latency vs 1 MiB. Going to 8 MiB buys less additional
  streaming gain while memory (+64%) and random-read latency (+33%) climb
  faster; **≥16 MiB is not recommended.** Use 1 MiB instead only for
  memory-constrained/embedded readers or pure random-access workloads, and 8 MiB
  only for deployments that are overwhelmingly bulk/streaming reads from
  high-latency remote storage.

## Current defaults across languages

The MCAP spec does not mandate a chunk size. The defaults fall into three
clusters, and the 768 KiB–1 MiB cluster traces back to ROS 1 `rosbag` (see
[History](#history-of-the-chunk-size-defaults) below):

| Language | Default chunk size | Bytes | Source |
| --- | --- | --- | --- |
| C++ | 768 KiB | `1024 * 768` | `cpp/mcap/include/mcap/types.hpp` (`DefaultChunkSize`) |
| Rust | 768 KiB | `1024 * 768` | `rust/mcap/src/write.rs` |
| Go | 1 MiB | `1024 * 1024` | `go/mcap/writer.go` |
| Python | 1 MiB | `1024 * 1024` | `python/mcap/mcap/writer.py` |
| TypeScript | 1 MiB | `1024 * 1024` | `typescript/core/src/McapWriter.ts` |
| Swift | 10 MiB | `10 * 1024 * 1024` | `swift/mcap/MCAPWriter.swift` |

The CLIs that re-chunk files use their own, larger defaults: the Go CLI uses
4 MiB (`filter`/`compress`/`decompress`/`recover`/`sort`), and the Rust CLI uses
4 MiB (filter/merge/recover) and 8 MiB (convert).

Note that "chunk size" is a target for the *uncompressed* chunk and is treated
as a soft ceiling: a chunk is closed once adding the next message would exceed
it, and a single message larger than the target is isolated in its own chunk
(see history below). So the on-disk compressed chunk is smaller than the target
by roughly the compression ratio.

## History of the chunk-size defaults

There has never been a benchmark or analysis establishing an "optimal" chunk
size — this study is the first. The existing defaults trace back to ROS 1 and
were otherwise chosen for consistency with neighboring implementations or to
avoid a data-loss footgun:

- **768 KiB comes from ROS 1 `rosbag`.** MCAP's first C++ writer
  ([#50](https://github.com/foxglove/mcap/pull/50), Jan 2022, built around the
  `ros1` profile) defined `constexpr uint64_t DefaultChunkSize = 1024 * 768`
  — 786,432 bytes, byte-for-byte identical to ROS 1 `rosbag`'s default
  `chunk_threshold_(768 * 1024)  // 768KB chunks` (see `ros_comm`
  `tools/rosbag/src/bag.cpp`; the Python `rosbag.Bag` default is likewise
  `chunk_threshold=786432`). So the 768 KiB value was inherited from the ROS 1
  bag format rather than chosen from first principles. Rust later adopted the
  same 768 KiB explicitly to match C++ (#777), and the Go/Python/TypeScript
  1 MiB defaults are a round-number variant of the same ballpark.
- **TS 1 MiB default ([#254](https://github.com/foxglove/mcap/pull/254), 2022).**
  The closest thing to a discussion of the value itself. A reviewer noted
  *"1MB feels small in this day and age (especially if images are involved),"*
  and the author responded that *"1MB is in line with defaults for Go and Python
  writers currently (C++ defaults to 768k)."* So 1 MiB was chosen for
  cross-language consistency, and the "too small for images" concern was
  acknowledged but not acted on.
- **Rust auto-chunking ([#754](https://github.com/foxglove/mcap/pull/754), 2022).**
  Added size-based chunk breaking. The original Rust writer author noted that
  the "right" criterion is workload-dependent (Anduril used uncompressed size
  and/or elapsed time depending on context).
- **Rust 768 KiB default ([#777](https://github.com/foxglove/mcap/pull/777), 2023).**
  Changed the Rust default from `None` (unbounded — *"a footgun [that] can lead
  to a situation where a user loses all of their MCAP data if they never break
  chunks"*) to `Some(1024 * 768)`. The motivation was safety, not performance,
  and it matched C++ (768 KiB) rather than the 1 MiB used by Go/Python/TS.
- **Soft-ceiling semantics ([#1291](https://github.com/foxglove/mcap/pull/1291), 2025).**
  Changed chunk size from a floor (close *after* exceeding) to a soft ceiling
  (close *before* exceeding), and made oversized messages get their own chunk.
  This is why, for the point-cloud corpus below, chunk sizes smaller than the
  ~1.5 MB message are degenerate.

The result is the three-way split above (768 KiB / 1 MiB / 10 MiB) with no
empirical basis — which is what motivated this study.

## Setup

| Dimension | Values |
| --- | --- |
| Chunk size (target uncompressed) | 256 KiB, 768 KiB, 1 MiB, 4 MiB, 8 MiB, 16 MiB, 32 MiB |
| Message class | `small` (~100 B telemetry), `jpeg` (~150 KB incompressible image), `pointcloud` (~1.5 MB semi-compressible), `mixed` (5-channel robot recording, rate-driven so all channels run concurrently over one duration) |
| Compression | zstd (primary), lz4 (sensitivity) |
| Read patterns | `full`, `point` (1 message), `range` (1% window), `streaming` (15% window), `topic` (mixed only) |

Read cost for remote storage is modeled analytically from the chunk index — one
ranged GET per chunk overlapping the query window plus one GET for the
summary/index — so it reflects the format, not the C++ reader's record-by-record
I/O granularity:

```
modeled_latency = (chunks_touched + 1) * RTT
                + (compressed_bytes_of_those_chunks + summary_bytes) / bandwidth
                + local_decode_time
```

Profiles: **local NVMe** (~50 µs, 2 GB/s), **regional object store** (20 ms,
300 MB/s), **high-latency remote** (100 ms, 80 MB/s).

## Environment

The benchmark was run inside a cloud Linux VM (KVM full virtualization), so the
CPU model string is masked/generic and absolute throughput/latency numbers are
specific to this machine — the *trends and ratios* are what matter, not the
absolute values. Details determined from `lscpu`, `/proc/meminfo`, `uname`, and
package metadata:

| Component | Value |
| --- | --- |
| CPU | Intel Xeon (generic model string under KVM); flags include AVX-512 + AMX/`avx512_fp16`/`avx_vnni`/`bf16`, i.e. a 4th-gen Xeon Scalable ("Sapphire Rapids")-class core |
| vCPUs | 4 (1 socket × 4 cores × 1 thread) |
| Caches (as exposed) | L1d 192 KiB, L1i 128 KiB, L2 8 MiB, L3 320 MiB |
| Virtualization | KVM, full |
| Memory | ~16 GB (`MemTotal` 16,402,092 kB) |
| OS | Ubuntu 24.04.4 LTS |
| Kernel | Linux 6.1.147, x86_64 |
| Storage | overlay filesystem (252 GB volume); benchmark files in page cache during reads |
| Compiler | g++ 13.3.0 (Ubuntu 13.3.0), `-O2` (see `Makefile`) |
| Compression libs | libzstd 1.5.5, liblz4 1.9.4 |

The benchmark is single-threaded for the timed write/read loops; the 4 vCPUs
mainly help the OS and the harness around the measured sections.

## 1. Compression ratio is flat across chunk size

![Compression ratio vs chunk size](results/fig1_compression_ratio.png)

| class | 256K | 768K | 1M | 4M | 8M | 16M | 32M |
| --- | --- | --- | --- | --- | --- | --- | --- |
| small | 1.35 | 1.37 | 1.37 | 1.37 | 1.37 | 1.37 | 1.37 |
| jpeg | 1.00 | 1.00 | 1.00 | 1.00 | 1.00 | 1.00 | 1.00 |
| pointcloud | 2.15 | 2.15 | 2.15 | 2.15 | 2.15 | 2.15 | 2.15 |
| mixed | 1.87 | 1.87 | 1.87 | 1.86 | 1.86 | 1.86 | 1.86 |

Beyond 256 KiB the ratio is flat (and the `mixed` corpus actually gets slightly
*worse* at large chunks, as incompressible camera data shares a zstd stream with
compressible telemetry). This is the single most important result: **there is no
compression argument for chunks larger than ~1 MiB.** The reason is that zstd's
match window already spans far more than a 256 KiB chunk of these payloads, so
enlarging the chunk adds no new redundancy to exploit — and point clouds carry
little cross-scan redundancy to begin with.

## 2. Write throughput is flat across chunk size

![Write throughput vs chunk size](results/fig2_write_throughput.png)

Payload write throughput is within a few percent across the whole sweep
(~150 MB/s for the compressible corpora, ~340 MB/s for the incompressible image
corpus, which skips real compression work). There is no write-speed argument for
large chunks either.

## 3. Bigger chunks cost reader memory

![Reader peak memory vs chunk size](results/fig3_reader_rss.png)

Peak reader RSS tracks chunk size almost linearly, because the reader buffers a
whole (de)compressed chunk:

| class | 1M | 4M | 8M | 16M | 32M |
| --- | --- | --- | --- | --- | --- |
| pointcloud | 7.1 MiB | 9.7 MiB | 17.9 MiB | 34.6 MiB | 64.8 MiB |
| mixed | 7.4 MiB | 11.0 MiB | 17.9 MiB | 35.4 MiB | 69.5 MiB |

Going from 1 MiB to 8 MiB roughly triples reader memory; 32 MiB is ~10×. This
matters for memory-constrained robots and for servers running many concurrent
readers.

## 4. Bigger chunks hurt selective / random reads

![Read amplification vs chunk size](results/fig4_read_amplification.png)

Read amplification = bytes the reader must fetch ÷ bytes actually wanted. For a
**point read** (one message) it grows linearly with chunk size — for small
telemetry it reaches ~10⁵× at 32 MiB (fetching ~14 MiB of chunk to return
100 bytes). `range` reads show the same trend more mildly; `streaming` and
`full` reads stay near-optimal at every chunk size.

Single message fetched per point read (zstd):

| class | 256K | 1M | 8M | 32M |
| --- | --- | --- | --- | --- |
| small | 106 KiB | 445 KiB | 3.5 MiB | 13.9 MiB |
| pointcloud | 679 KiB | 679 KiB | 3.3 MiB | 14.6 MiB |

(For point clouds, chunks below the ~1.5 MB message size are degenerate — each
message already occupies its own chunk, so 256K/768K/1M behave identically. The
chunk-size knob only gains leverage once a chunk holds several messages.)

## 5. The remote crossover: where bigger chunks help

![Point vs streaming read latency](results/fig5_remote_crossover.png)

This is the heart of the local-vs-remote question. On **local NVMe**, latency is
dominated by decompression, so chunk size barely matters (and large chunks
slightly hurt streaming by decompressing wasted bytes). On **remote** profiles,
the per-request round trip dominates, and the two access patterns diverge:

- **Point / random reads get worse** with bigger chunks — you pay to transfer a
  whole large chunk for one message. Regional single-message latency:
  ~46 ms at ≤1 MiB → ~70 ms at 8 MiB → ~135 ms at 32 MiB (point cloud).

  ![Point read latency, regional](results/fig6_remote_point_read.png)

- **Streaming / bulk reads get better** with bigger chunks — fewer round trips.
  Point-cloud `streaming` (15% window) latency:

  | profile | 1M | 4M | 8M | 16M | 32M |
  | --- | --- | --- | --- | --- | --- |
  | regional (20 ms) | 662 ms | 406 ms | 258 ms | 273 ms | 242 ms |
  | high-latency (100 ms) | 3074 ms | 1785 ms | 1009 ms | 954 ms | 763 ms |

  The streaming win is substantial (~2.5–3× from 1 MiB to 8 MiB) but **most of
  it is captured by 4–8 MiB**; going from 8 MiB to 32 MiB adds little while
  quadrupling the point-read penalty and reader memory.

So the intuition that "8 MB trades off well" is correct **specifically for
streaming visualization / point-cloud playback served from remote storage** —
that is exactly the regime where larger chunks pay off, and 8 MiB sits near the
knee of the streaming curve. It is not a good default for workloads that do
random access, run locally, or are memory-sensitive.

## 6. zstd vs lz4

![zstd vs lz4](results/fig7_zstd_vs_lz4.png)

Orthogonal to chunk size: zstd compresses better (point cloud 2.15× vs lz4
1.87×) but decodes ~2× slower (~260 ms vs ~115 ms full scan). Both are flat
across chunk size, so the chunk-size conclusions hold regardless of compression
choice.

## Recommendation

**The recommended default is 4 MiB.** The supporting numbers (zstd):

| 1 MiB → 4 MiB → 8 MiB | 1 MiB | 4 MiB | 8 MiB |
| --- | --- | --- | --- |
| compression ratio (point cloud) | 2.15 | 2.15 | 2.15 |
| write throughput (mixed) | 164 MB/s | 156 MB/s | 157 MB/s |
| reader peak RSS (mixed, full) | 7.4 MiB | 11.0 MiB | 17.9 MiB |
| point read latency, regional (point cloud) | 46 ms | 52 ms | 69 ms |
| streaming latency, regional (mixed) | 1068 ms | 402 ms | 295 ms |
| streaming latency, high-latency remote (point cloud) | 3074 ms | 1785 ms | 1008 ms |

4 MiB is where the streaming benefit is largely realized (−40 to −62% vs 1 MiB)
while random-read latency and reader memory are still close to the 1 MiB
baseline. Past 4 MiB the streaming gains shrink while the random-read and memory
costs accelerate.

Use a different value only for a known-narrow workload:

| Workload | Chunk size |
| --- | --- |
| **General-purpose default** | **4 MiB** |
| Memory-constrained / embedded readers, or pure random-access (e.g. heavy scrubbing) | 1 MiB |
| Overwhelmingly bulk/streaming reads from high-latency remote storage | 8 MiB |
| Any workload | do not exceed 8 MiB; ≥16 MiB only hurts |

Relating this to the current per-language defaults (C++/Rust 768 KiB; Go,
Python, TypeScript 1 MiB; Swift 10 MiB): the 768 KiB–1 MiB cluster is *safe and
conservative* — lowest memory and random-read latency — but it leaves
substantial remote-streaming throughput on the table (e.g. ~2.6× slower mixed
streaming on a regional object store than 4 MiB). Swift's 10 MiB is too high: it
pays ~2.4× the reader memory and materially higher random-read latency for no
compression or write benefit. The data-optimal single default for all six
libraries is **4 MiB**.

**Proposed action:** this study and PR change **no** library default — it exists
to document the tradeoffs and give the defaults an empirical basis. If
maintainers want to act on it, the natural follow-ups (each its own PR) are
(1) raise the C++/Rust/Go/Python/TypeScript defaults to 4 MiB and lower Swift's
10 MiB to 4 MiB, unifying on the data-optimal value, or (2) if a more
conservative change is preferred, at minimum lower Swift's 10 MiB. These are
intentionally left out of this PR.

## Caveats

- Results reflect the C++ implementation; absolute throughput/RSS are
  C++-specific, but the format-level conclusions (flat ratio, read amplification,
  remote crossover) generalize across languages.
- The remote model is analytic (idealized one-GET-per-chunk reader); real
  clients add request overhead and may coalesce or parallelize differently.
- The model's compute term is the measured local decode wall time, taken with
  the file in page cache, so it folds local read I/O into "decode." This is
  negligible for the decode-bound patterns studied here but slightly
  double-counts I/O for trivially small reads. The supplementary
  `raw_fetched`/`raw_reads` columns in `raw.tsv` record the C++ reader's actual
  record-granular I/O; the model intentionally ignores them so it reflects the
  format rather than that reader's I/O granularity.
- At 32 MiB the ~256 MiB corpora hold only ~8 chunks, so the largest-chunk
  selective-read points are coarser; this does not affect the ratio, write, or
  memory conclusions.
- Synthetic payloads approximate real compressibility (quantized fixed-point
  telemetry, random bytes for already-compressed images, structured floats for
  point clouds); real data will shift absolute ratios but not the chunk-size
  trends.
