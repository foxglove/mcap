"""Read metadata records back from an MCAP file.

Iterates the file's metadata records and prints each one, decoding any
JSON-encoded value (see writer.py) back into structured data.

Usage:

    python reader.py input.mcap
"""
import json
import sys

from mcap.reader import make_reader

with open(sys.argv[1], "rb") as f:
    reader = make_reader(f)
    for metadata in reader.iter_metadata():
        print(f"metadata '{metadata.name}':")
        for key, value in metadata.metadata.items():
            try:
                decoded = json.loads(value)
            except (json.JSONDecodeError, TypeError):
                decoded = value
            print(f"  {key} = {decoded}")
