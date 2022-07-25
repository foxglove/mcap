# This example writes a single point cloud message.
from io import BytesIO
from random import random
import struct
import time
import sys

from mcap_protobuf.writer import Writer

from google.protobuf.timestamp_pb2 import Timestamp

from foxglove.PointCloud_pb2 import PointCloud
from foxglove.PackedElementField_pb2 import PackedElementField
from foxglove.Pose_pb2 import Pose
from foxglove.Vector3_pb2 import Vector3
from foxglove.Quaternion_pb2 import Quaternion


def main():
    with open(sys.argv[1], "wb") as f, Writer(f) as writer:
        fields = [
            PackedElementField(name="x", offset=0, type=7),
            PackedElementField(name="y", offset=4, type=7),
            PackedElementField(name="z", offset=8, type=7),
            PackedElementField(name="intensity", offset=12, type=7),
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
