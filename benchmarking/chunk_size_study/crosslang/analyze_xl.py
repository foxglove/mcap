#!/usr/bin/env python3
"""Aggregate the cross-language read/write results into a figure + table."""
import csv
import os
import statistics
from collections import defaultdict

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt

HERE = os.path.dirname(os.path.abspath(__file__))
RAW = os.path.join(HERE, "results", "xl_raw.tsv")
OUT = os.path.join(HERE, "results")

LANGS = ["cpp", "rust", "go", "python", "typescript"]
LANG_LABEL = {"cpp": "C++", "rust": "Rust", "go": "Go", "python": "Python", "typescript": "TypeScript"}


def load():
    agg = defaultdict(list)
    payload = {}
    for r in csv.DictReader(open(RAW), delimiter="\t"):
        k = (r["workload"], r["comp"], r["lang"], r["op"])
        agg[k].append(float(r["wall"]))
        payload[k] = int(r["payload_bytes"])
    return agg, payload


def mbps(agg, payload, k):
    if k not in agg:
        return None
    w = statistics.median(agg[k])
    return payload[k] / w / 1e6 if w > 0 else None


def bars(ax, agg, payload, workload, comp, langs):
    xs = list(range(len(langs)))
    wr = [mbps(agg, payload, (workload, comp, l, "write")) or 0 for l in langs]
    rd = [mbps(agg, payload, (workload, comp, l, "read")) or 0 for l in langs]
    ax.bar([x - 0.2 for x in xs], wr, 0.4, label="write")
    ax.bar([x + 0.2 for x in xs], rd, 0.4, label="read")
    ax.set_xticks(xs)
    ax.set_xticklabels([LANG_LABEL[l] for l in langs], rotation=20)
    ax.set_yscale("log")
    ax.set_ylabel("throughput (MB/s payload, log scale)")
    ax.set_title(f"{workload} messages, {comp}")
    ax.legend()
    for x, v in zip(xs, wr):
        if v:
            ax.text(x - 0.2, v, f"{v:.0f}", ha="center", va="bottom", fontsize=7)
    for x, v in zip(xs, rd):
        if v:
            ax.text(x + 0.2, v, f"{v:.0f}", ha="center", va="bottom", fontsize=7)


def main():
    plt.rcParams.update({"figure.dpi": 130, "font.size": 10, "axes.grid": True, "grid.alpha": 0.3})
    agg, payload = load()

    fig, (a1, a2) = plt.subplots(1, 2, figsize=(13, 5), sharey=True)
    bars(a1, agg, payload, "large", "zstd", ["cpp", "rust", "go", "python"])
    bars(a2, agg, payload, "large", "none", LANGS)
    fig.suptitle("Cross-language MCAP read/write throughput (50 KB messages, 4 MiB chunks)")
    fig.tight_layout()
    fig.savefig(os.path.join(OUT, "fig_crosslang_throughput.png"))
    plt.close(fig)

    lines = []
    for wl in ["small", "large"]:
        for comp in ["zstd", "none"]:
            lines.append(f"### {wl} messages, {comp}\n")
            lines.append("| language | write MB/s | read MB/s | read/write |")
            lines.append("| --- | --- | --- | --- |")
            for l in LANGS:
                w = mbps(agg, payload, (wl, comp, l, "write"))
                r = mbps(agg, payload, (wl, comp, l, "read"))
                if w is None:
                    continue
                ratio = f"{r / w:.1f}×" if (r and w) else "-"
                lines.append(f"| {LANG_LABEL[l]} | {w:.0f} | {r:.0f} | {ratio} |")
            lines.append("")
    with open(os.path.join(OUT, "xl_summary.md"), "w") as fh:
        fh.write("\n".join(lines))
    print("wrote fig_crosslang_throughput.png and xl_summary.md")


if __name__ == "__main__":
    main()
