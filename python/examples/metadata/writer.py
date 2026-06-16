"""Write an MCAP file with a metadata record carrying a structured record.

MCAP metadata records are a lightweight, message-independent way to attach
named key-value information to a file: run configuration, calibration, or, as
here, a robot's intent / capability manifest. Metadata values are strings, so a
structured (nested) record is stored as a single JSON-encoded string value.

Usage:

    python writer.py output.mcap
"""
import json
import sys
from time import time_ns

from mcap.writer import Writer

with open(sys.argv[1], "wb") as stream:
    writer = Writer(stream)
    writer.start()

    # A structured record to attach to the file. Because metadata values must
    # be strings, nested data is JSON-encoded into a single field; flat string
    # fields can be stored directly.
    intent_record = {
        "robot_id": "demo-1",
        "capabilities": ["move_to", "grasp"],
        "max_velocity_m_s": 1.5,
    }
    writer.add_metadata(
        "robot_intent",
        {
            "schema": "example/robot-intent@1",
            "created_ns": str(time_ns()),
            "record": json.dumps(intent_record),
        },
    )

    writer.finish()
