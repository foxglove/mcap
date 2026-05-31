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
- **Recommended default: 1–4 MiB.** It captures all of the compression ratio
  and write throughput, keeps reader memory and random-read latency low, and
  already captures most of the remote-streaming round-trip benefit. **8 MiB is a
  defensible upper bound for streaming-heavy, point-cloud-heavy, remote-served
  workloads**, but is a poor universal default. **≥16 MiB is not recommended.**

## Current defaults across languages

The MCAP spec does not mandate a chunk size, and each implementation picked its
own writer default independently. They fall into three clusters:

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
size — this study is the first. The existing defaults were chosen for
consistency with neighboring implementations or to avoid a data-loss footgun:

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
| Message class | `small` (~100 B telemetry), `jpeg` (~150 KB incompressible image), `pointcloud` (~1.5 MB semi-compressible), `mixed` (5-channel robot recording) |
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

## 1. Compression ratio is flat across chunk size

![Compression ratio vs chunk size](results/fig1_compression_ratio.png)

| class | 256K | 768K | 1M | 4M | 8M | 16M | 32M |
| --- | --- | --- | --- | --- | --- | --- | --- |
| small | 1.35 | 1.37 | 1.37 | 1.37 | 1.37 | 1.37 | 1.37 |
| jpeg | 1.00 | 1.00 | 1.00 | 1.00 | 1.00 | 1.00 | 1.00 |
| pointcloud | 2.15 | 2.15 | 2.15 | 2.15 | 2.15 | 2.15 | 2.15 |
| mixed | 1.95 | 1.95 | 1.95 | 1.94 | 1.93 | 1.93 | 1.93 |

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
| pointcloud | 7.2 MiB | 9.9 MiB | 18.0 MiB | 34.6 MiB | 64.9 MiB |
| mixed | 7.3 MiB | 11.0 MiB | 21.7 MiB | 43.1 MiB | 69.7 MiB |

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

| Workload | Suggested chunk size |
| --- | --- |
| **General default** | **1–4 MiB** |
| Random access / point queries, or memory-constrained | 256 KiB – 1 MiB |
| Bulk/streaming reads from remote object storage (e.g. cloud-served point-cloud playback) | 4–8 MiB |
| Anything | avoid ≥16 MiB as a default |

Relating this back to the current per-language defaults (C++/Rust 768 KiB; Go,
Python, TypeScript 1 MiB; Swift 10 MiB): the 768 KiB–1 MiB cluster is already in
the sweet spot. Swift's 10 MiB is on the high side — fine for streaming-heavy
remote use, but it pays ~3× reader memory and materially higher random-read
latency for no compression or write-speed benefit. If the libraries were to
standardize on one value, **1 MiB (already shared by Go/Python/TypeScript) is
the best-supported choice**, with an option to raise it for remote-streaming
deployments.

## Caveats

- Results reflect the C++ implementation; absolute throughput/RSS are
  C++-specific, but the format-level conclusions (flat ratio, read amplification,
  remote crossover) generalize across languages.
- The remote model is analytic (idealized one-GET-per-chunk reader); real
  clients add request overhead and may coalesce or parallelize differently.
- At 32 MiB the ~256 MiB corpora hold only ~8 chunks, so the largest-chunk
  selective-read points are coarser; this does not affect the ratio, write, or
  memory conclusions.
- Synthetic payloads approximate real compressibility (quantized fixed-point
  telemetry, random bytes for already-compressed images, structured floats for
  point clouds); real data will shift absolute ratios but not the chunk-size
  trends.
