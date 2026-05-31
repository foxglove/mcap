# MCAP chunk-size study (C++)

A focused benchmark that sweeps the MCAP writer's **chunk size** to quantify how
it trades off compression ratio, write throughput, memory, and — most
importantly — **read performance for local vs. remote storage across different
access patterns**.

It is C++-only on purpose: C++ is the writer used for ROS 2 `rosbag2` MCAP
recordings, so it is the most representative implementation for the question of
what default chunk size a robotics recorder should use. The corpus model (the
mixed 5-channel robot recording) is adapted from the cross-language benchmark
proposed in [foxglove/mcap#1611](https://github.com/foxglove/mcap/pull/1611).

See [`REPORT.md`](./REPORT.md) for the full write-up, figures, and the
recommendation.

## What it varies

- **Chunk size** (target uncompressed): 256 KiB, 768 KiB, 1 MiB, 4 MiB, 8 MiB,
  16 MiB, 32 MiB.
- **Message-size class**, each with realistic content/compressibility:
  - `small` — ~100 B fixed-point telemetry (IMU/odom/tf-like), compressible.
  - `jpeg` — ~150 KB already-compressed image, effectively incompressible.
  - `pointcloud` — ~1.5 MB `PointCloud2`-like float data, semi-compressible.
  - `mixed` — a 5-channel robot recording combining all of the above.
- **Compression**: `zstd` (primary) and `lz4` (sensitivity).
- **Read access pattern**: `full` scan, `point` (single message), `range` (1%
  time window), `streaming` (15% window), and `topic` (one topic, mixed only).

## What it measures

Per write: compressed file size (→ compression ratio), write wall time
(→ throughput), peak RSS.

Per read: messages returned, **chunks touched** and **compressed bytes** for the
chunks overlapping the query window (derived from the chunk index, so it is
independent of the reader's I/O granularity), summary/index size, local decode
wall time, and peak RSS. From these, `analyze.py` computes an analytic
remote-storage latency model:

```
modeled_read_latency = (chunks_touched + 1) * RTT
                     + (chunk_fetched_bytes + summary_bytes) / bandwidth
                     + local_decode_time
```

i.e. an idealized object-store reader issuing one ranged GET per overlapping
chunk plus one GET for the index. (`raw_fetched`/`raw_reads` from the actual
`CountingReadable` wrapper are also recorded in `raw.tsv` as supplementary data,
but the model deliberately uses the index-derived figures so it does not depend
on the C++ reader's record-by-record I/O granularity.)

## Running

```sh
sudo apt-get install -y libzstd-dev liblz4-dev   # build deps
pip3 install matplotlib                           # analysis dep

make                 # builds ./bench against ../../cpp/mcap/include
./run.sh             # full sweep -> results/raw.tsv (a few minutes)
python3 analyze.py   # -> results/*.png and results/summary.md
```

Configure via env vars, e.g. `TARGET_BYTES`, `CHUNKS`, `CLASSES`, `COMP_LIST`,
`ITERS_W`, `ITERS_R` (see `run.sh`).
