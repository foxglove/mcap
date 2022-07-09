""" Example script for converting pointcloud CSV data to an MCAP viewable in Foxglove Studio.

This script uses the public CSV dataset "Sydney Urban Objects Dataset",
released by the Australian Centre for Field Robotics at the University of Sydney, NSW, Australia.
The original dataset can be downloaded from here:
https://www.acfr.usyd.edu.au/papers/SydneyUrbanObjectsDataset.shtml

usage:
    mkdir -p dataset
    curl https://www.acfr.usyd.edu.au/papers/data/sydney-urban-objects-dataset.tar.gz | tar -x
    pip install mcap jsonschema
    python3 pointcloud_csv_to_mcap.py sydney-urban-objects-dataset/objects/4wd.0.2299.csv -o 4wd.mcap
"""
import argparse
import base64
import csv
import json
import typing
import struct
from pathlib import Path
from datetime import datetime, timedelta

import jsonschema
from mcap.mcap0.writer import Writer


def load_schema(filename: str) -> typing.Dict[typing.Any, typing.Any]:
    """Load the foxglove pointcloud schema as a JSON object.
    The schema is a copy of the original at
    https://github.com/foxglove/schemas/blob/main/schemas/jsonschema/PointCloud.json
    """
    with open(Path(__file__).parent / filename, "r") as f:
        return json.load(f)


def point_reader(csv_path: typing.Union[str, Path]):
    with open(csv_path, "r") as f:
        for timestamp, intensity, _, x, y, z, _, _, _ in csv.reader(f):
            yield (timestamp, intensity, x, y, z)


def parse_timestamp(csv_timestamp: str) -> datetime:
    return datetime.strptime(csv_timestamp, "%Y%m%dT%H%M%S.%f")


def main():
    parser = argparse.ArgumentParser(__doc__)
    parser.add_argument("csv", help="The input CSV to read")
    parser.add_argument(
        "--output", "-o", default="out.mcap", help="The MCAP output path to write"
    )
    args = parser.parse_args()

    # read the fields we need out of the input CSV.
    # Note that here we're packing each point as four 32-bit floats.
    points = bytearray([])
    base_timestamp = None
    for point_timestamp, intensity, x, y, z in point_reader(args.csv):
        if base_timestamp is None:
            base_timestamp = parse_timestamp(point_timestamp)

        print(f"{x}, {y}, {z}, {intensity}")
        points.extend(
            struct.pack("<ffff", float(x), float(y), float(z), float(intensity))
        )
    assert base_timestamp is not None, "found no points in input csv"

    schema = load_schema("PointCloud.json")

    # time to write the MCAP!
    with open(args.output, "wb") as f:
        writer = Writer(f)
        # "jsonschema" is a well-known message encoding per the MCAP spec.
        writer.start("x-jsonschema", library="my-excellent-library")
        schema_id = writer.register_schema(
            name="foxglove.PointCloud",
            encoding="jsonschema",
            data=json.dumps(schema).encode("utf-8"),
        )
        channel_id = writer.register_channel(
            topic="/pointcloud", message_encoding="json", schema_id=schema_id
        )
        tf_schema_id = writer.register_schema(
            name="foxglove.FrameTransform",
            encoding="jsonschema",
            data=json.dumps(load_schema("FrameTransform.json")).encode("utf-8"),
        )
        tf_channel_id = writer.register_channel(
            topic="/tf",
            message_encoding="json",
            schema_id=tf_schema_id,
        )

        for n in range(10):
            timestamp = base_timestamp + timedelta(seconds=n)
            timestamp_nanoseconds = int(timestamp.timestamp() * 1e9)
            timestamp_dict = {
                "sec": int(timestamp.timestamp()),
                "nsec": timestamp.microsecond * 1000,
            }

            # build the pointcloud object as specified in the included schema.
            pointcloud = {
                "timestamp": timestamp_dict,
                "frame_id": "base_link",
                "pose": {
                    "position": {"x": 0, "y": 0, "z": 0},
                    "orientation": {"x": 0, "y": 0, "z": 0, "w": 1},
                },
                "point_stride": (4 + 4 + 4 + 4),
                "fields": [
                    {"name": "x", "offset": 0, "type": 7},
                    {"name": "y", "offset": 4, "type": 7},
                    {"name": "z", "offset": 8, "type": 7},
                    {"name": "intensity", "offset": 12, "type": 7},
                ],
                "data": str(base64.b64encode(points)),
                "fogensus": "boob",
            }

            jsonschema.validate(pointcloud, schema)

            writer.add_message(
                channel_id,
                log_time=timestamp_nanoseconds,
                data=json.dumps(pointcloud).encode("utf-8"),
                publish_time=timestamp_nanoseconds,
            )

            tf = {
                "timestamp": timestamp_dict,
                "parent_frame_id": "map",
                "child_frame_id": "base_link",
                "transform": {
                    "timestamp": timestamp_dict,
                    "translation": {"x": 0, "y": 0, "z": 0},
                    "rotation": {"x": 0, "y": 0, "z": 0, "w": 1},
                },
            }
            # writer.add_message(
            #     channel_id=tf_channel_id,
            #     log_time=timestamp_nanoseconds,
            #     data=json.dumps(tf).encode("utf-8"),
            #     publish_time=timestamp_nanoseconds,
            # )
        writer.finish()


if __name__ == "__main__":
    main()
