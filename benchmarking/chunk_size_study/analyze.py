#!/usr/bin/env python3
"""Aggregate chunk-size sweep results and render the report figures.

Reads results/raw.tsv (produced by run.sh), computes medians over iterations
and derived metrics (compression ratio, throughput, read amplification, and an
analytic remote-storage latency model), and writes PNG figures plus a
summary.md fragment into results/.
"""
import csv
import os
import statistics
from collections import defaultdict

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt

HERE = os.path.dirname(os.path.abspath(__file__))
RESULTS = os.path.join(HERE, "results")
RAW = os.path.join(RESULTS, "raw.tsv")

CHUNKS = [262144, 786432, 1048576, 4194304, 8388608, 16777216, 33554432]
CLASSES = ["small", "jpeg", "pointcloud", "mixed"]
CLASS_LABEL = {
    "small": "small telemetry (~100 B)",
    "jpeg": "compressed image (~150 KB)",
    "pointcloud": "point cloud (~1.5 MB)",
    "mixed": "mixed robot recording",
}
PRIMARY_COMP = "zstd"

# Analytic storage profiles: (round-trip latency seconds, bandwidth bytes/sec).
PROFILES = {
    "local NVMe": (0.00005, 2.0e9),
    "regional object store (20 ms, 300 MB/s)": (0.020, 300e6),
    "high-latency remote (100 ms, 80 MB/s)": (0.100, 80e6),
}


def human(n):
    n = int(n)
    if n >= 1 << 20 and n % (1 << 20) == 0:
        return f"{n >> 20}M"
    if n >= 1 << 10:
        return f"{n // 1024}K"
    return str(n)


def load():
    writes = defaultdict(list)  # (comp,cls,chunk) -> rows
    reads = defaultdict(list)  # (comp,cls,chunk,pattern) -> rows
    with open(RAW) as fh:
        r = csv.DictReader(fh, delimiter="\t")
        for row in r:
            comp = row["comp"]
            cls = row["class"]
            chunk = int(row["chunk_bytes"])
            if row["op"] == "write":
                writes[(comp, cls, chunk)].append(row)
            else:
                reads[(comp, cls, chunk, row["pattern"])].append(row)
    return writes, reads


def med(rows, key, cast=float):
    return statistics.median(cast(x[key]) for x in rows)


def style():
    plt.rcParams.update({"figure.dpi": 130, "font.size": 11, "axes.grid": True, "grid.alpha": 0.3})


def xaxis(ax):
    ax.set_xscale("log", base=2)
    ax.set_xticks(CHUNKS)
    ax.set_xticklabels([human(c) for c in CHUNKS])
    ax.set_xlabel("chunk size (target uncompressed)")


def fig_ratio(writes):
    fig, ax = plt.subplots(figsize=(8, 5))
    for cls in CLASSES:
        xs, ys = [], []
        for c in CHUNKS:
            rows = writes.get((PRIMARY_COMP, cls, c))
            if not rows:
                continue
            payload = med(rows, "payload_bytes")
            fsize = med(rows, "file_size")
            xs.append(c)
            ys.append(payload / fsize)
        if xs:
            ax.plot(xs, ys, "o-", label=CLASS_LABEL[cls])
    xaxis(ax)
    ax.set_ylabel("compression ratio (payload / file size)")
    ax.set_title(f"Compression ratio vs chunk size ({PRIMARY_COMP})")
    ax.legend()
    fig.tight_layout()
    fig.savefig(os.path.join(RESULTS, "fig1_compression_ratio.png"))
    plt.close(fig)


def fig_write_tput(writes):
    fig, ax = plt.subplots(figsize=(8, 5))
    for cls in CLASSES:
        xs, ys = [], []
        for c in CHUNKS:
            rows = writes.get((PRIMARY_COMP, cls, c))
            if not rows:
                continue
            payload = med(rows, "payload_bytes")
            wall = med(rows, "wall")
            xs.append(c)
            ys.append(payload / wall / 1e6)
        if xs:
            ax.plot(xs, ys, "o-", label=CLASS_LABEL[cls])
    xaxis(ax)
    ax.set_ylabel("write throughput (MB/s payload)")
    ax.set_title(f"Write throughput vs chunk size ({PRIMARY_COMP})")
    ax.legend()
    fig.tight_layout()
    fig.savefig(os.path.join(RESULTS, "fig2_write_throughput.png"))
    plt.close(fig)


def fig_rss(reads):
    fig, ax = plt.subplots(figsize=(8, 5))
    for cls in CLASSES:
        xs, ys = [], []
        for c in CHUNKS:
            rows = reads.get((PRIMARY_COMP, cls, c, "full"))
            if not rows:
                continue
            xs.append(c)
            ys.append(med(rows, "rss_kb") / 1024.0)
        if xs:
            ax.plot(xs, ys, "o-", label=CLASS_LABEL[cls])
    xaxis(ax)
    ax.set_ylabel("reader peak RSS (MiB)")
    ax.set_title(f"Reader peak memory vs chunk size (full scan, {PRIMARY_COMP})")
    ax.legend()
    fig.tight_layout()
    fig.savefig(os.path.join(RESULTS, "fig3_reader_rss.png"))
    plt.close(fig)


def fig_amplification(reads):
    patterns = ["point", "range", "streaming", "full"]
    fig, axes = plt.subplots(1, len(patterns), figsize=(17, 4.3), sharey=True)
    for ax, pat in zip(axes, patterns):
        for cls in CLASSES:
            xs, ys = [], []
            for c in CHUNKS:
                rows = reads.get((PRIMARY_COMP, cls, c, pat))
                if not rows:
                    continue
                fetched = med(rows, "chunk_fetched_bytes")
                payload = med(rows, "payload_bytes")  # bytes actually wanted
                if payload <= 0:
                    continue
                xs.append(c)
                ys.append(fetched / payload)
            if xs:
                ax.plot(xs, ys, "o-", label=CLASS_LABEL[cls])
        xaxis(ax)
        ax.set_yscale("log")
        ax.set_title(f"{pat} read")
    axes[0].set_ylabel("read amplification\n(bytes fetched / bytes wanted)")
    axes[-1].legend(fontsize=8)
    fig.suptitle(f"Read amplification vs chunk size ({PRIMARY_COMP})")
    fig.tight_layout()
    fig.savefig(os.path.join(RESULTS, "fig4_read_amplification.png"))
    plt.close(fig)


def modeled_time(rows, rtt, bw):
    # Idealized object-store reader: one ranged GET per chunk that overlaps the
    # query window, plus one GET for the summary/index section. Transfer volume
    # is the compressed bytes of those chunks plus the index. Compute is the
    # measured local decode/iterate wall time.
    n_gets = med(rows, "chunks_touched") + 1
    fetched = med(rows, "chunk_fetched_bytes") + med(rows, "summary_bytes")
    compute = med(rows, "wall")
    return n_gets * rtt + fetched / bw + compute


def fig_crossover(reads):
    # Money chart: point vs streaming read latency vs chunk size, per profile,
    # for the point-cloud corpus (where large chunks help streaming most).
    cls = "pointcloud"
    fig, axes = plt.subplots(1, len(PROFILES), figsize=(17, 4.6), sharey=False)
    for ax, (pname, (rtt, bw)) in zip(axes, PROFILES.items()):
        for pat, mk in [("point", "o-"), ("streaming", "s-")]:
            xs, ys = [], []
            for c in CHUNKS:
                rows = reads.get((PRIMARY_COMP, cls, c, pat))
                if not rows:
                    continue
                xs.append(c)
                ys.append(modeled_time(rows, rtt, bw) * 1000.0)
            if xs:
                ax.plot(xs, ys, mk, label=pat)
        xaxis(ax)
        ax.set_yscale("log")
        ax.set_title(pname, fontsize=9)
        ax.legend(fontsize=9)
    axes[0].set_ylabel("modeled read latency (ms)")
    fig.suptitle(f"Point vs streaming read latency vs chunk size — {CLASS_LABEL[cls]} ({PRIMARY_COMP})")
    fig.tight_layout()
    fig.savefig(os.path.join(RESULTS, "fig5_remote_crossover.png"))
    plt.close(fig)


def fig_remote_point(reads):
    # Single-message (point) read latency across classes on the regional profile.
    rtt, bw = PROFILES["regional object store (20 ms, 300 MB/s)"]
    fig, ax = plt.subplots(figsize=(8, 5))
    for cls in CLASSES:
        xs, ys = [], []
        for c in CHUNKS:
            rows = reads.get((PRIMARY_COMP, cls, c, "point"))
            if not rows:
                continue
            xs.append(c)
            ys.append(modeled_time(rows, rtt, bw) * 1000.0)
        if xs:
            ax.plot(xs, ys, "o-", label=CLASS_LABEL[cls])
    xaxis(ax)
    ax.set_ylabel("modeled single-message read latency (ms)")
    ax.set_title("Point read latency vs chunk size — regional object store")
    ax.legend()
    fig.tight_layout()
    fig.savefig(os.path.join(RESULTS, "fig6_remote_point_read.png"))
    plt.close(fig)


def fig_comp_compare(writes, reads):
    # zstd vs lz4: ratio and full-read compute time for the point-cloud corpus.
    cls = "pointcloud"
    fig, (a1, a2) = plt.subplots(1, 2, figsize=(13, 4.6))
    for comp in ("zstd", "lz4"):
        xs, ratio, rd = [], [], []
        for c in CHUNKS:
            w = writes.get((comp, cls, c))
            r = reads.get((comp, cls, c, "full"))
            if not w or not r:
                continue
            xs.append(c)
            ratio.append(med(w, "payload_bytes") / med(w, "file_size"))
            rd.append(med(r, "wall") * 1000.0)
        if xs:
            a1.plot(xs, ratio, "o-", label=comp)
            a2.plot(xs, rd, "o-", label=comp)
    for ax in (a1, a2):
        xaxis(ax)
        ax.legend()
    a1.set_ylabel("compression ratio")
    a1.set_title("Ratio: zstd vs lz4 (point cloud)")
    a2.set_ylabel("full-scan decode time (ms)")
    a2.set_title("Full-scan decode time: zstd vs lz4")
    fig.tight_layout()
    fig.savefig(os.path.join(RESULTS, "fig7_zstd_vs_lz4.png"))
    plt.close(fig)


def write_summary(writes, reads):
    lines = []
    lines.append("## Compression ratio (zstd, payload / file size)\n")
    lines.append("| class | " + " | ".join(human(c) for c in CHUNKS) + " |")
    lines.append("| --- " * (len(CHUNKS) + 1) + "|")
    for cls in CLASSES:
        cells = []
        for c in CHUNKS:
            rows = writes.get((PRIMARY_COMP, cls, c))
            cells.append(f"{med(rows,'payload_bytes')/med(rows,'file_size'):.2f}" if rows else "-")
        lines.append(f"| {cls} | " + " | ".join(cells) + " |")
    lines.append("")

    lines.append("## Point-read bytes fetched (zstd, single message)\n")
    lines.append("| class | " + " | ".join(human(c) for c in CHUNKS) + " |")
    lines.append("| --- " * (len(CHUNKS) + 1) + "|")
    for cls in CLASSES:
        cells = []
        for c in CHUNKS:
            rows = reads.get((PRIMARY_COMP, cls, c, "point"))
            if rows:
                kb = med(rows, "chunk_fetched_bytes") / 1024.0
                cells.append(f"{kb:.0f} KiB" if kb < 1024 else f"{kb/1024:.1f} MiB")
            else:
                cells.append("-")
        lines.append(f"| {cls} | " + " | ".join(cells) + " |")
    lines.append("")

    with open(os.path.join(RESULTS, "summary.md"), "w") as fh:
        fh.write("\n".join(lines))


def main():
    style()
    writes, reads = load()
    fig_ratio(writes)
    fig_write_tput(writes)
    fig_rss(reads)
    fig_amplification(reads)
    fig_crossover(reads)
    fig_remote_point(reads)
    fig_comp_compare(writes, reads)
    write_summary(writes, reads)
    print("Wrote figures and summary.md to", RESULTS)


if __name__ == "__main__":
    main()
