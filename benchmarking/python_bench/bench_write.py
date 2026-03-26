#!/usr/bin/env python3
"""MCAP write benchmark for Python."""

import os
import resource
import sys
import time


def make_payload(size, varied):
    if varied:
        return bytes([(i * 137 + 43) & 0xFF for i in range(size)])
    return b"\x42" * size


def main():
    if len(sys.argv) != 6:
        print(
            f"Usage: {sys.argv[0]} <output_file> <mode> <num_messages> <payload_size> <uniform|varied>",
            file=sys.stderr,
        )
        return 1

    varied_fill = sys.argv[5] == "varied"

    filename = sys.argv[1]
    mode = sys.argv[2]
    mixed = sys.argv[4] == "mixed"

    if mixed:
        num_messages = 3750
        payload_size_str = "mixed"
    else:
        num_messages = int(sys.argv[3])
        payload_size_str = sys.argv[4]

    from mcap.writer import Writer, CompressionType

    with open(filename, "wb") as f:
        if mode == "unchunked":
            writer = Writer(f, use_chunking=False)
        elif mode == "chunked":
            writer = Writer(f, compression=CompressionType.NONE, chunk_size=786432)
        elif mode == "zstd":
            writer = Writer(f, compression=CompressionType.ZSTD, chunk_size=786432)
        elif mode == "lz4":
            writer = Writer(f, compression=CompressionType.LZ4, chunk_size=786432)
        else:
            print(f"Unknown mode: {mode}", file=sys.stderr)
            return 1

        writer.start(profile="bench", library="py-bench")

        if mixed:
            # Channel definitions: (topic, schema_name, base_payload_size, period_ns, count)
            channel_defs = [
                ("/imu", "IMU", 96, 5_000_000, 2000),
                ("/odom", "Odometry", 296, 20_000_000, 500),
                ("/tf", "TFMessage", None, 10_000_000, 1000),
                ("/lidar", "PointCloud2", 230_400, 100_000_000, 100),
                ("/camera/compressed", "CompressedImage", 524_288, 66_666_667, 150),
            ]

            tf_payload_cycle = [80, 160, 320, 800, 1600]

            schema_ids = []
            channel_ids = []
            for topic, schema_name, _, _, _ in channel_defs:
                sid = writer.register_schema(
                    name=schema_name,
                    encoding="jsonschema",
                    data=b'{"type":"object"}',
                )
                cid = writer.register_channel(
                    topic=topic,
                    message_encoding="json",
                    schema_id=sid,
                )
                schema_ids.append(sid)
                channel_ids.append(cid)

            # Pre-generate the message schedule sorted by (timestamp, channel_index)
            schedule = []
            for ch_idx, (_, _, _, period_ns, count) in enumerate(channel_defs):
                for msg_i in range(count):
                    ts = msg_i * period_ns
                    schedule.append((ts, ch_idx, msg_i))
            schedule.sort(key=lambda x: (x[0], x[1]))

            # Pre-allocate payload buffers
            payload_cache = {}
            for size in [96, 296, 230_400, 524_288] + tf_payload_cycle:
                if size not in payload_cache:
                    payload_cache[size] = make_payload(size, varied_fill)

            # Per-channel sequence counters
            seq = [0] * len(channel_defs)

            # Time the message-writing loop + finish
            t_start = time.perf_counter_ns()

            for ts, ch_idx, msg_i in schedule:
                if ch_idx == 2:  # /tf
                    payload = payload_cache[tf_payload_cycle[msg_i % len(tf_payload_cycle)]]
                else:
                    payload = payload_cache[channel_defs[ch_idx][2]]
                writer.add_message(
                    channel_id=channel_ids[ch_idx],
                    sequence=seq[ch_idx],
                    log_time=ts,
                    publish_time=ts,
                    data=payload,
                )
                seq[ch_idx] += 1

            writer.finish()
        else:
            payload_size = int(sys.argv[4])
            payload = make_payload(payload_size, varied_fill)

            schema_id = writer.register_schema(
                name="BenchMsg",
                encoding="jsonschema",
                data=b'{"type":"object"}',
            )

            channel_id = writer.register_channel(
                topic="/bench",
                message_encoding="json",
                schema_id=schema_id,
            )

            # Time the message-writing loop + finish
            t_start = time.perf_counter_ns()

            for i in range(num_messages):
                log_time = i * 1000
                writer.add_message(
                    channel_id=channel_id,
                    sequence=i,
                    log_time=log_time,
                    publish_time=log_time,
                    data=payload,
                )

            writer.finish()

    t_end = time.perf_counter_ns()

    elapsed_ns = t_end - t_start
    wall_sec = elapsed_ns / 1e9
    file_size = os.path.getsize(filename)

    peak_rss_kb = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss

    # TSV output: op lang mode num_msgs payload_size file_size elapsed_ns wall_sec peak_rss_kb
    print(
        f"write\tpython\t{mode}\t{num_messages}\t{payload_size_str}\t{file_size}\t{elapsed_ns}\t{wall_sec:.6f}\t{peak_rss_kb}"
    )

    return 0


if __name__ == "__main__":
    sys.exit(main())
