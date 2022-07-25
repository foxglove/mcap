# This example writes a single point cloud message.
from io import BytesIO
from random import random
import struct
import time
import sys

from mcap.mcap0.writer import Writer as McapWriter
from mcap_protobuf.schema import register_schema

from google.protobuf.timestamp_pb2 import Timestamp

from foxglove.PointCloud_pb2 import PointCloud
from foxglove.PackedElementField_pb2 import PackedElementField
from foxglove.Pose_pb2 import Pose
from foxglove.Vector3_pb2 import Vector3
from foxglove.Quaternion_pb2 import Quaternion


def main():
    output = open(sys.argv[1], "w+b")
    mcap_writer = McapWriter(output)
    mcap_writer.start(profile="protobuf", library="test")

    cloud_schema_id = register_schema(writer=mcap_writer, message_class=PointCloud)

    cloud_channel_id = mcap_writer.register_channel(
        topic="/point_cloud",
        message_encoding="protobuf",
        schema_id=cloud_schema_id,
    )

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
    for i in range(num_points):
        data.write(
            struct.pack(
                "<ffff", scale * random(), scale * random(), scale * random(), random()
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
    mcap_writer.add_message(
        channel_id=cloud_channel_id,
        log_time=time.time_ns(),
        data=message.SerializeToString(),  # type: ignore
        publish_time=time.time_ns(),
    )

    mcap_writer.finish()
    output.close()


if __name__ == "__main__":
    main()
