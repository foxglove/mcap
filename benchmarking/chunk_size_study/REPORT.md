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
- **Storage overhead from indexes is set by message count, not chunk size.** The
  message index costs ~16 uncompressed bytes/message regardless of chunking, so
  it is ~22% of the file for tiny high-rate topics but **<0.03% for point
  clouds** — even in the degenerate one-cloud-per-chunk case. Chunk size barely
  moves total file size.
- **Bigger chunks have real, monotonic costs:** reader peak memory grows ~10×
  (≈7 MiB → ≈65 MiB from 1 MiB → 32 MiB), and single-message / random reads get
  linearly more expensive because the reader must fetch and decompress a whole
  chunk to extract one message.
- **Bigger chunks do not robustly help remote reads.** Full scans are
  chunk-size-independent; random/point access is strictly *worse* with big
  chunks (you fetch a whole chunk per message); and the apparent streaming
  benefit only appears for a naive reader that issues one GET per chunk — a
  coalescing/buffering reader is flat-to-faster with *small* chunks (see §5).
- **Recommended default: 1 MiB.** Every metric that varies with chunk size is
  either flat (compression ratio, write throughput) or favors smaller chunks
  (reader memory, random-read latency, remote read cost with a proper reader).
  1 MiB captures all the compression ratio with negligible per-chunk overhead.
  **≥8 MiB only adds cost; ≥16 MiB clearly hurts.** This matches the existing
  Go/Python/TypeScript defaults.

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

## 5. Remote reads: random access favors small chunks; streaming is reader-dependent

It is tempting to argue that large chunks help reads from high-latency remote
storage. That is only true for one specific (and avoidable) reader design.
Breaking it down by access pattern:

**Full scan — chunk size is irrelevant.** You transfer the whole data section
either way, and any reader streams it in large sequential fetches independent of
chunk boundaries. No round-trip or transfer difference.

**Random / point / scattered access — smaller chunks are genuinely better, for
any reader.** To read one message you must fetch and decompress the entire chunk
containing it. That cost is reader-independent and grows linearly with chunk
size:

![Point read latency, regional](results/fig6_remote_point_read.png)

Bytes fetched for one point-cloud message: 0.66 MiB at ≤1 MiB chunks → 3.3 MiB
at 8 MiB → 14.6 MiB at 32 MiB. Regional single-message latency: ~46 ms at
≤1 MiB → ~70 ms at 8 MiB → ~135 ms at 32 MiB. This is the real, robust remote
effect of chunk size, and it points to **smaller** chunks.

**Streaming a time window — depends entirely on the reader, and the "benefit" is
an artifact.** A time-range query selects a *contiguous run* of chunks. How that
maps to round trips depends on the reader:

![Streaming: naive vs coalescing reader](results/fig8_reader_model.png)

- A **naive reader that issues one ranged GET per chunk** does fewer round trips
  with larger chunks, so its windowed-read latency falls as chunks grow (662 ms
  → 257 ms regional, 1 MiB → 8 MiB). This is the only place large chunks "win",
  and it is the curve the earlier draft of this report leaned on.
- A **coalescing/buffering reader that fetches the contiguous span in one ranged
  GET** does *not* care about chunk count. Its latency is flat — and actually
  rises slightly with bigger chunks, because the window snaps to coarser chunk
  boundaries and transfers more overhang (17.9 MiB at 1 MiB chunks → 29 MiB at
  32 MiB). Crucially, the coalescing reader is **much faster than the naive
  reader at small chunks** (142 ms vs 662 ms regional at 1 MiB).

In other words, large chunks only speed up streaming for a reader that is
leaving performance on the table; the correct fix is to coalesce requests, not
to inflate chunk size. With a good reader, smaller chunks are as fast or faster
for streaming *and* much faster for random access.

(The earlier `fig5_remote_crossover.png` plots the naive-reader model for point
vs streaming; it is retained for reference but the naive streaming curve should
be read with the above caveat.)

So the remote picture, contrary to the intuition that "8 MB trades off well for
streaming", actually favors **smaller** chunks: random access strictly prefers
them, full scans are indifferent, and streaming only prefers large chunks under
a naive non-coalescing reader.

## 6. zstd vs lz4

![zstd vs lz4](results/fig7_zstd_vs_lz4.png)

Orthogonal to chunk size: zstd compresses better (point cloud 2.15× vs lz4
1.87×) but decodes ~2× slower (~260 ms vs ~115 ms full scan). Both are flat
across chunk size, so the chunk-size conclusions hold regardless of compression
choice.

## 7. Storage overhead: chunk and message indexes

Beyond the compressed payload, an indexed MCAP file carries structural overhead
that lives *outside* the compressed chunks and is therefore stored uncompressed:

- **Message Index record** — one per channel per chunk, written right after each
  chunk. ~15 bytes of framing plus a **16-byte entry (log_time + offset) per
  message**.
- **Chunk Index record** — one per chunk, in the summary section. ~85–95 bytes
  each (timestamps, offsets, sizes, per-channel index offsets, compression name).
- **Chunk framing** — each chunk record's own header (~50–57 bytes) wrapping the
  compressed `records` blob.

Measured breakdown (zstd, ~24 MB payload per corpus), bytes:

| corpus | chunk | msgs | chunks | compressed payload | chunk framing | message index | summary (chunk index etc.) | overhead % of file |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| point cloud | 256 KiB | 16 | 17 | 11,125,438 | 897 | 496 | 1,766 | **0.028%** |
| point cloud | 8 MiB | 16 | 4 | 11,126,607 | 212 | 316 | 649 | 0.011% |
| small telemetry | 256 KiB | 240,000 | 120 | 13,931,523 | 6,360 | **3,841,800** | 10,731 | **21.7%** |
| small telemetry | 1 MiB | 240,000 | 30 | 13,588,578 | 1,590 | 3,840,450 | 2,901 | 22.0% |
| mixed | 1 MiB | 521 | 29 | 12,966,434 | 1,521 | 9,446 | 3,675 | 0.11% |

### Answering the degenerate point-cloud case

When each point cloud is its own chunk, the *per-cloud* extra cost is roughly:

```
~53 B chunk framing  +  ~31 B message index record  +  ~90 B chunk index record  ≈  ~175 B/cloud
```

all uncompressed. On a 0.5–8 MB cloud that is **0.002%–0.035%** — negligible.
The measurement above (16 clouds in 17 chunks) totals 3,159 bytes of overhead on
an 11.1 MB file: **0.028%**. So index/framing overhead is a non-issue for point
clouds, in the degenerate case or otherwise.

### What actually drives index overhead

- **Message count is the dominant factor**, because the message index costs a
  fixed **~16 bytes per message** and is *not compressed*. Note it is essentially
  **independent of chunk size** — the small-telemetry overhead is ~21.7% at
  256 KiB and ~22.0% at 1 MiB. This is why tiny high-rate topics (IMU, TF) carry
  large relative overhead while a handful of big point clouds carry almost none.
- **Chunk count** drives the chunk-index records (summary) and chunk framing
  (~140–150 B per chunk combined). Smaller chunks → more chunks → more of this,
  but it is an order of magnitude smaller than the message-index cost and stays
  well under ~0.1% except for pathologically tiny chunks. (Halving from 1 MiB to
  256 KiB on the small corpus raised summary+framing from ~4.5 KB to ~17 KB —
  still ~0.1% of the file.)
- **Active channels per chunk** adds one message-index record (~15 B framing +
  its entries) per channel per chunk.
- **Compression** does not touch the indexes (they live outside the chunk), so
  it cannot reduce this overhead; only the per-message record headers inside the
  chunk are compressed.
- **Writer options** are the real lever: `SkipMessageIndexing` removes the
  per-message 16-byte cost entirely (at the price of fast random access), and
  `SkipStatistics` / `SkipRepeatedSchemas` trim the summary.

Net for the chunk-size question: chunk size barely affects total storage. The
index cost is set by how many messages you log, not how you group them — so
there is no storage argument for large chunks (and only a negligible one against
very small chunks).

## 8. Cross-language read/write correlation

The study above is C++-only. To confirm the C++ read/write numbers are
*representative* and not an artifact of one implementation, a separate
cross-language micro-benchmark (`crosslang/`) writes and reads the same
fixed-payload corpus with the C++, Rust, Go, Python and TypeScript libraries
from this repo (4 MiB chunks; a single reused payload generated outside the
timed loop, so this isolates library/codec throughput rather than payload
generation). zstd is run for the four languages with a native zstd codec;
TypeScript is run uncompressed because `@mcap/core` ships no zstd *compressor*,
and an uncompressed pass is run for all five so TypeScript can be compared.

![Cross-language throughput](crosslang/results/fig_crosslang_throughput.png)

Large messages (50 KB), the regime relevant to point-cloud/image robotics data:

| language | write MB/s | read MB/s | read/write | (uncompressed) write | read |
| --- | --- | --- | --- | --- | --- |
| C++ | 1741 | 10531 | 6.0× | 718 | 7365 |
| Rust | 1686 | 7777 | 4.6× | 2469 | 7962 |
| Go | 2977 | 6171 | 2.1× | 2082 | 3782 |
| Python | 1079 | 4697 | 4.4× | 709 | 3206 |
| TypeScript | — | — | — | 405 | 780 |

Takeaways:

- **Reads are several× faster than writes in every language** — the same
  qualitative relationship the C++ study reports.
- **C++ is not an outlier**: for large messages it tracks Rust and Go within
  ~2×, and the three compiled languages cluster together (multi-GB/s reads,
  1–3 GB/s writes). Python and TypeScript are slower in absolute terms but show
  the same read-faster-than-write pattern.
- So the C++ read/write behavior the study relies on (reads cheap, writes the
  bottleneck, both far above typical sensor data rates) **correlates across
  languages**, and the chunk-size conclusions — which are about the *format*
  (compression ratio, bytes fetched, index overhead), not C++ specifics — carry
  over.

Caveats for this comparison: timings are single-threaded medians of 3 runs on
the same VM; the reused payload is highly compressible, so the zstd numbers
measure library/codec overhead rather than realistic compression CPU; the
small-message regime (`crosslang/results/xl_summary.md`) is dominated by
per-message overhead and is noisier; TypeScript writes are uncompressed only.

## Recommendation

**The recommended default is 1 MiB.** Every metric that actually varies with
chunk size either is flat or favors smaller chunks, once the remote-streaming
"benefit" of large chunks is correctly attributed to naive readers (see §5). The
supporting numbers (zstd):

| metric | 1 MiB | 4 MiB | 8 MiB | direction |
| --- | --- | --- | --- | --- |
| compression ratio (point cloud) | 2.15 | 2.15 | 2.15 | flat |
| write throughput (mixed) | 164 MB/s | 156 MB/s | 157 MB/s | flat |
| reader peak RSS (mixed, full) | 7.4 MiB | 11.0 MiB | 17.9 MiB | smaller better |
| point read latency, regional (point cloud) | 46 ms | 52 ms | 69 ms | smaller better |
| streaming, regional, **coalescing reader** (point cloud) | 142 ms | 146 ms | 157 ms | smaller better |
| streaming, regional, **naive reader** (point cloud) | 662 ms | 406 ms | 257 ms | larger better* |

\* The only column favoring large chunks assumes a reader that issues one GET
per chunk; a coalescing reader (the row above) removes that effect entirely and
is far faster at 1 MiB. So large chunks do not robustly help any access pattern.

1 MiB sits at the point where compression ratio has fully saturated and
per-chunk overhead (chunk-index records, decompression setup) is already
negligible, while keeping reader memory low and random/remote reads cheap.
Smaller still (256 KiB) is fine for compression and random access but starts to
add chunk-count overhead and is below ROS 1's historical value; 1 MiB is the
natural, well-supported choice.

Use a different value only for a known-narrow workload:

| Workload | Chunk size |
| --- | --- |
| **General-purpose default** | **1 MiB** |
| Heavy random access / scrubbing, or memory-constrained readers | 256 KiB – 1 MiB |
| Bulk reads behind a naive (non-coalescing) high-latency remote reader | up to 4 MiB |
| Any workload | do not exceed 8 MiB; ≥16 MiB only hurts |

Relating this to the current per-language defaults (C++/Rust 768 KiB; Go,
Python, TypeScript 1 MiB; Swift 10 MiB): the **768 KiB–1 MiB cluster is already
right** — it has all the compression ratio, lowest memory, and best random/remote
read behavior. Swift's 10 MiB is the outlier: it pays ~2.4× the reader memory and
materially higher random-read latency for no compression, write, or (with a
proper reader) streaming benefit.

**Proposed action:** this study and PR change **no** library default — it exists
to document the tradeoffs and give the defaults an empirical basis. The main
finding is that the existing 768 KiB–1 MiB defaults are well chosen and need no
change. The one defensible follow-up (its own PR) is to **lower Swift's 10 MiB
toward 1 MiB** to bring it in line; unifying the others on 1 MiB would be a
tidy-up but is not performance-critical. These are intentionally left out of this
PR.

## Caveats

- Results reflect the C++ implementation; absolute throughput/RSS are
  C++-specific, but the format-level conclusions (flat ratio, read amplification,
  remote behavior) generalize across languages.
- The remote model is analytic. It is reported for both a naive reader (one GET
  per chunk) and a coalescing reader (one ranged GET per contiguous span); real
  clients sit between these but a competent remote reader coalesces, so the
  coalescing model is the one to design against.
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
