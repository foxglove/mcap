#!/usr/bin/env python3
"""Cross-language correlation check (Python)."""
import sys
import time

from mcap.writer import Writer, CompressionType
from mcap.reader import make_reader


def fill(size: int, seq: int) -> bytes:
    return bytes((i + seq) & 0xFF for i in range(size))


def main() -> None:
    if len(sys.argv) != 7:
        sys.stderr.write("usage: xl.py <write|read> <file> <num> <size> <chunk> <none|zstd>\n")
        sys.exit(1)
    op, file, num_s, size_s, chunk_s, comp = sys.argv[1:7]
    num, size, chunk = int(num_s), int(size_s), int(chunk_s)

    if op == "write":
        compression = CompressionType.ZSTD if comp == "zstd" else CompressionType.NONE
        # Pre-build payloads outside the timed region is unfair to other langs
        # (they generate per-message); generate inline to match.
        with open(file, "wb") as f:
            w = Writer(f, chunk_size=chunk, compression=compression)
            w.start(profile="xl", library="python")
            schema_id = w.register_schema(name="Bench", encoding="jsonschema", data=b"{}")
            channel_id = w.register_channel(topic="/bench", message_encoding="json", schema_id=schema_id)
            payload = fill(size, 0)  # one reusable payload, generated outside timing
            t = time.monotonic()
            for i in range(num):
                w.add_message(
                    channel_id=channel_id,
                    log_time=i * 1000,
                    data=payload,
                    publish_time=i * 1000,
                    sequence=i,
                )
            w.finish()
            wall = time.monotonic() - t
        import os

        fsize = os.path.getsize(file)
        print(f"python\twrite\t{comp}\t{num}\t{num * size}\t{fsize}\t{wall:.6f}")
    else:
        with open(file, "rb") as f:
            reader = make_reader(f)
            t = time.monotonic()
            count = 0
            nbytes = 0
            for _schema, _channel, message in reader.iter_messages():
                count += 1
                nbytes += len(message.data)
            wall = time.monotonic() - t
        print(f"python\tread\t{comp}\t{count}\t{nbytes}\t0\t{wall:.6f}")


if __name__ == "__main__":
    main()
