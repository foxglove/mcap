#!/usr/bin/env python3
"""MCAP read benchmark for Python."""

import os
import resource
import sys
import time


def main():
    if len(sys.argv) < 2:
        print(
            f"Usage: {sys.argv[0]} <input_file> [mode] [num_messages] [payload_size] [filter]",
            file=sys.stderr,
        )
        print(
            "  filter: topic | timerange | topic_timerange (default: no filter)",
            file=sys.stderr,
        )
        return 1

    filename = sys.argv[1]
    mode = sys.argv[2] if len(sys.argv) >= 3 else "unknown"
    num_messages_str = sys.argv[3] if len(sys.argv) >= 4 else "0"
    payload_size_str = sys.argv[4] if len(sys.argv) >= 5 else "0"
    filter_mode = sys.argv[5] if len(sys.argv) >= 6 else ""

    filter_kwargs = {}
    if filter_mode == "topic":
        filter_kwargs["topics"] = ["/imu"]
    elif filter_mode == "timerange":
        filter_kwargs["start_time"] = 3000000000
        filter_kwargs["end_time"] = 5000000000
    elif filter_mode == "topic_timerange":
        filter_kwargs["topics"] = ["/lidar"]
        filter_kwargs["start_time"] = 4000000000
        filter_kwargs["end_time"] = 6000000000

    from mcap.reader import make_reader

    # Time file open + reader creation + message iteration
    t_start = time.perf_counter_ns()

    with open(filename, "rb") as f:
        reader = make_reader(f)
        for _schema, _channel, message in reader.iter_messages(**filter_kwargs):
            # Touch the data to prevent dead-code elimination
            if len(message.data) == 0:
                print("Empty message", file=sys.stderr)

    t_end = time.perf_counter_ns()

    elapsed_ns = t_end - t_start
    wall_sec = elapsed_ns / 1e9
    file_size = os.path.getsize(filename)

    peak_rss_kb = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss

    # TSV output: op lang mode num_msgs payload_size file_size elapsed_ns wall_sec peak_rss_kb
    print(
        f"read\tpython\t{mode}\t{num_messages_str}\t{payload_size_str}\t{file_size}\t{elapsed_ns}\t{wall_sec:.6f}\t{peak_rss_kb}"
    )

    return 0


if __name__ == "__main__":
    sys.exit(main())
