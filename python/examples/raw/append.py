"""
Example demonstrating Writer.open_append() to append data to an existing MCAP file.

Shows:
- Creating an initial MCAP file with schema, channel, and messages
- Reopening with Writer.open_append() to add more data
- Adding messages to existing channels
- Registering new channels during append
- Adding attachments and metadata during append
"""

import json
import os

from mcap.reader import SeekingReader, make_reader
from mcap.writer import Writer


def summarize_mcap(path: str, label: str) -> None:
    """Open an MCAP file and print a summary of its contents."""
    with open(path, "rb") as f:
        reader = SeekingReader(f)
        summary = reader.get_summary()

    print(f"\n=== {label} ===")
    print(f"  Schemas: {len(summary.schemas)}")
    print(f"  Channels: {len(summary.channels)}")
    print(f"  Messages: {summary.statistics.message_count}")
    print(f"  Attachments: {summary.statistics.attachment_count}")
    print(f"  Metadata: {summary.statistics.metadata_count}")


MCAP_FILE = "demo.mcap"

# --- Step 1: Create initial MCAP file ---

writer = Writer(MCAP_FILE)
writer.start(library="example-append")

schema_id = writer.register_schema(
    name="sensor_reading",
    encoding="jsonschema",
    data=json.dumps(
        {
            "type": "object",
            "properties": {
                "sensor_id": {"type": "string"},
                "value": {"type": "number"},
                "unit": {"type": "string"},
            },
        }
    ).encode(),
)

temp_channel_id = writer.register_channel(
    schema_id=schema_id,
    topic="temperature",
    message_encoding="json",
)

# Add initial temperature readings
for i, value in enumerate([23.5, 24.1]):
    writer.add_message(
        channel_id=temp_channel_id,
        log_time=i,
        publish_time=i,
        data=json.dumps(
            {
                "sensor_id": "temp-001",
                "value": value,
                "unit": "celsius",
            }
        ).encode(),
    )

writer.finish()

summarize_mcap(MCAP_FILE, "After initial write")


# --- Step 2: Reopen and append ---

append_writer = Writer.open_append(MCAP_FILE)

# Add more temperature readings to existing channel
for i, value in enumerate([24.8, 25.2], start=2):
    append_writer.add_message(
        channel_id=temp_channel_id,  # reuse existing channel ID
        log_time=i,
        publish_time=i,
        data=json.dumps(
            {
                "sensor_id": "temp-001",
                "value": value,
                "unit": "celsius",
            }
        ).encode(),
    )

# Register new channel on existing schema
humidity_channel_id = append_writer.register_channel(
    schema_id=schema_id,  # reuse existing schema
    topic="humidity",
    message_encoding="json",
)

# Add humidity readings
for i, value in enumerate([45.0, 46.5], start=4):
    append_writer.add_message(
        channel_id=humidity_channel_id,
        log_time=i,
        publish_time=i,
        data=json.dumps(
            {
                "sensor_id": "humid-001",
                "value": value,
                "unit": "percent",
            }
        ).encode(),
    )

# Add attachment
append_writer.add_attachment(
    name="calibration.txt",
    log_time=0,
    create_time=0,
    media_type="text/plain",
    data=b"Calibrated on 2024-01-15\nOffset: 0.1",
)

# Add metadata
append_writer.add_metadata(
    name="session_info",
    data={"location": "Lab A", "operator": "Alice"},
)

append_writer.finish()

summarize_mcap(MCAP_FILE, "After append")


# --- Step 3: Read all messages ---

print("\n=== All messages ===")
with open(MCAP_FILE, "rb") as f:
    reader = make_reader(f)
    for schema, channel, message in reader.iter_messages():
        print(f"  {channel.topic}: {message.data.decode()}")


# Cleanup
os.remove(MCAP_FILE)
