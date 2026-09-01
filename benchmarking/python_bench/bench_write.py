#!/usr/bin/env python3
"""MCAP write benchmark for Python."""

import os
import resource
import sys
import time
import zlib

# Shared payload blob parameters; must match gen_blob.py and the other
# language benches. Message i's payload is the window of the blob starting
# at (i * BLOB_STRIDE) % BLOB_WINDOW_SPAN, so all implementations feed
# identical bytes to their writers.
BLOB_SIZE = 16777216
BLOB_MAX_PAYLOAD = 524288
BLOB_WINDOW_SPAN = BLOB_SIZE - BLOB_MAX_PAYLOAD
BLOB_STRIDE = 7919


def payload_offset(msg_index):
    return (msg_index * BLOB_STRIDE) % BLOB_WINDOW_SPAN


def load_blob(path):
    with open(path, "rb") as f:
        blob = f.read()
    if len(blob) != BLOB_SIZE:
        print(f"Blob file {path} is not exactly {BLOB_SIZE} bytes", file=sys.stderr)
        sys.exit(1)
    return memoryview(blob)


def peak_rss_kb() -> int:
    """Peak RSS in KB. ru_maxrss is KB on Linux but bytes on macOS."""
    rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    if sys.platform == "darwin":
        rss //= 1024
    return rss


def main():
    if len(sys.argv) != 6:
        print(
            f"Usage: {sys.argv[0]} <output_file> <mode> <num_messages> <payload_size> <blob_file>",
            file=sys.stderr,
        )
        return 1

    blob = load_blob(sys.argv[5])

    filename = sys.argv[1]
    mode = sys.argv[2]
    mixed = sys.argv[4] == "mixed"

    if mixed:
        num_messages = 3750
        payload_size_str = "mixed"
    else:
        num_messages = int(sys.argv[3])
        payload_size_str = sys.argv[4]
        if int(sys.argv[4]) > BLOB_MAX_PAYLOAD:
            print(f"payload_size must be <= {BLOB_MAX_PAYLOAD}", file=sys.stderr)
            return 1

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

            # Not timed: CRC of the payload stream for cross-language verification
            payload_crc = 0
            for i, (ts, ch_idx, msg_i) in enumerate(schedule):
                if ch_idx == 2:  # /tf
                    size = tf_payload_cycle[msg_i % len(tf_payload_cycle)]
                else:
                    size = channel_defs[ch_idx][2]
                off = payload_offset(i)
                payload_crc = zlib.crc32(blob[off : off + size], payload_crc)

            # Per-channel sequence counters
            seq = [0] * len(channel_defs)

            # Time the message-writing loop + finish + flush to the OS
            t_start = time.perf_counter_ns()

            for i, (ts, ch_idx, msg_i) in enumerate(schedule):
                if ch_idx == 2:  # /tf
                    size = tf_payload_cycle[msg_i % len(tf_payload_cycle)]
                else:
                    size = channel_defs[ch_idx][2]
                off = payload_offset(i)
                payload = blob[off : off + size]
                writer.add_message(
                    channel_id=channel_ids[ch_idx],
                    sequence=seq[ch_idx],
                    log_time=ts,
                    publish_time=ts,
                    data=payload,
                )
                seq[ch_idx] += 1

            writer.finish()
            f.flush()
            t_end = time.perf_counter_ns()
        else:
            payload_size = int(sys.argv[4])

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

            # Not timed: CRC of the payload stream for cross-language verification
            payload_crc = 0
            for i in range(num_messages):
                off = payload_offset(i)
                payload_crc = zlib.crc32(blob[off : off + payload_size], payload_crc)

            # Time the message-writing loop + finish + flush to the OS
            t_start = time.perf_counter_ns()

            for i in range(num_messages):
                log_time = i * 1000
                off = payload_offset(i)
                writer.add_message(
                    channel_id=channel_id,
                    sequence=i,
                    log_time=log_time,
                    publish_time=log_time,
                    data=blob[off : off + payload_size],
                )

            writer.finish()
            f.flush()
            t_end = time.perf_counter_ns()

    elapsed_ns = t_end - t_start
    wall_sec = elapsed_ns / 1e9
    file_size = os.path.getsize(filename)

    rss_kb = peak_rss_kb()

    # TSV output: op lang mode num_msgs payload_size file_size elapsed_ns wall_sec peak_rss_kb payload_crc32
    print(
        f"write\tpython\t{mode}\t{num_messages}\t{payload_size_str}\t{file_size}\t{elapsed_ns}\t{wall_sec:.6f}\t{rss_kb}\t{payload_crc}"
    )

    return 0


if __name__ == "__main__":
    sys.exit(main())
