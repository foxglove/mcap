# This example writes a single point cloud message.
from io import BytesIO
from random import random
import struct
import sys
import time

from foxglove_schemas_protobuf.PackedElementField_pb2 import PackedElementField
from foxglove_schemas_protobuf.PointCloud_pb2 import PointCloud
from foxglove_schemas_protobuf.Pose_pb2 import Pose
from foxglove_schemas_protobuf.Quaternion_pb2 import Quaternion
from foxglove_schemas_protobuf.Vector3_pb2 import Vector3

from google.protobuf.timestamp_pb2 import Timestamp

from mcap_protobuf.writer import Writer


def main():
    if len(sys.argv) != 2:
        print(f"Usage: {sys.argv[0]} <output.mcap>")
        sys.exit(1)

    with open(sys.argv[1], "wb") as f, Writer(f) as writer:
        fields = [
            PackedElementField(name="x", offset=0, type=PackedElementField.FLOAT32),
            PackedElementField(name="y", offset=4, type=PackedElementField.FLOAT32),
            PackedElementField(name="z", offset=8, type=PackedElementField.FLOAT32),
            PackedElementField(name="intensity", offset=12, type=PackedElementField.FLOAT32),
        ]
        pose = Pose(
            position=Vector3(x=0, y=0, z=0),
            orientation=Quaternion(w=1, x=0, y=0, z=0),
        )

        num_points = 100
        data = BytesIO()
        scale = 2
        for _ in range(num_points):
            data.write(
                struct.pack(
                    "<ffff",
                    scale * random(),
                    scale * random(),
                    scale * random(),
                    random(),
                )
            )

        message = PointCloud(
            frame_id="example",
            pose=pose,
            timestamp=Timestamp(seconds=int(time.time()), nanos=0),
            point_stride=16,
            fields=fields,
            data=data.getvalue(),
        )
        writer.write_message(
            topic="/point_cloud",
            log_time=time.time_ns(),
            message=message,
            publish_time=time.time_ns(),
        )


if __name__ == "__main__":
    main()
