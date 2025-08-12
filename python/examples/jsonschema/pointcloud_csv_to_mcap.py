""" Example script for converting pointcloud CSV data to an MCAP viewable in Foxglove.

This script uses the public CSV dataset "Sydney Urban Objects Dataset",
released by the Australian Centre for Field Robotics at the University of Sydney, NSW, Australia.
The original dataset can be downloaded from here:
https://www.acfr.usyd.edu.au/papers/SydneyUrbanObjectsDataset.shtml

usage:
  curl https://www.acfr.usyd.edu.au/papers/data/sydney-urban-objects-dataset.tar.gz | tar -xz
  pip install mcap
  python3 pointcloud_csv_to_mcap.py sydney-urban-objects-dataset/objects/4wd.0.2299.csv -o out.mcap
"""

import argparse
import base64
import csv
import datetime
import json
import struct
import typing
from pathlib import Path

# tutorial-mcap-imports-start
from mcap.well_known import MessageEncoding, SchemaEncoding
from mcap.writer import Writer

# tutorial-mcap-imports-end


# tutorial-csv-decode-start
def point_reader(csv_path: typing.Union[str, Path]):
    with open(csv_path, "r") as f:
        for time_string, i, _, x, y, z, _, _, _ in csv.reader(f):
            timestamp = datetime.datetime.strptime(time_string, "%Y%m%dT%H%M%S.%f")
            yield (timestamp, float(i), float(x), float(y), float(z))
            # tutorial-csv-decode-end


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("csv", help="The input CSV to read")
    parser.add_argument(
        "--output", "-o", default="out.mcap", help="The MCAP output path to write"
    )
    args = parser.parse_args()

    pointcloud: typing.Dict[str, typing.Any]
    # tutorial-point-layout-start
    float32 = 7  # as defined in the schema
    pointcloud = {
        "point_stride": (4 + 4 + 4 + 4),  # four bytes per float
        "fields": [
            {"name": "x", "offset": 0, "type": float32},
            {"name": "y", "offset": 4, "type": float32},
            {"name": "z", "offset": 8, "type": float32},
            {"name": "i", "offset": 12, "type": float32},
        ],
    }
    # tutorial-point-layout-end

    # tutorial-pack-points-start
    points = bytearray()
    base_timestamp = None
    for point_timestamp, intensity, x, y, z in point_reader(args.csv):
        if base_timestamp is None:
            base_timestamp = point_timestamp
        points.extend(struct.pack("<ffff", x, y, z, intensity))
    assert base_timestamp is not None, "found no points in input csv"
    pointcloud["data"] = base64.b64encode(points).decode("utf-8")
    # tutorial-pack-points-end

    # tutorial-pose-frame-id-start
    pointcloud["pose"] = {
        "position": {"x": 0, "y": 0, "z": 0},
        "orientation": {"x": 0, "y": 0, "z": 0, "w": 1},
    }
    pointcloud["frame_id"] = "lidar"
    # tutorial-pose-frame-id-end

    # tutorial-write-header-start
    with open(args.output, "wb") as f:
        writer = Writer(f)
        writer.start()
        # tutorial-write-header-end

        # tutorial-write-channel-start
        with open(Path(__file__).parent / "PointCloud.json", "rb") as f:
            schema = f.read()
        schema_id = writer.register_schema(
            name="foxglove.PointCloud",
            encoding=SchemaEncoding.JSONSchema,
            data=schema,
        )
        channel_id = writer.register_channel(
            topic="pointcloud",
            message_encoding=MessageEncoding.JSON,
            schema_id=schema_id,
        )
        # tutorial-write-channel-end
        # tutorial-write-message-start
        for i in range(10):
            frame_timestamp = base_timestamp + datetime.timedelta(seconds=(i / 10.0))
            pointcloud["timestamp"] = {
                "sec": int(frame_timestamp.timestamp()),
                "nsec": frame_timestamp.microsecond * 1000,
            }
            writer.add_message(
                channel_id,
                log_time=int(frame_timestamp.timestamp() * 1e9),
                data=json.dumps(pointcloud).encode("utf-8"),
                publish_time=int(frame_timestamp.timestamp() * 1e9),
            )
        # tutorial-write-message-end
        # tutorial-finish-start
        writer.finish()
        # tutorial-finish-end


if __name__ == "__main__":
    main()
