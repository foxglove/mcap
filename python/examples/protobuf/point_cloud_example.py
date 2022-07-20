# This example writes a single point cloud message.
from io import BytesIO
from random import random
import struct
import time

from mcap.mcap0.writer import Writer as McapWriter
from mcap_protobuf.schema import register_schema

from ros.builtins_pb2 import Time
from ros.sensor_msgs.PointCloud2_pb2 import PointCloud2
from ros.sensor_msgs.PointField_pb2 import PointField
from ros.std_msgs.Header_pb2 import Header

output = open("point_cloud.mcap", "w+b")
mcap_writer = McapWriter(output)
mcap_writer.start(profile="protobuf", library="test")

cloud_schema_id = register_schema(writer=mcap_writer, message_class=PointCloud2)

cloud_channel_id = mcap_writer.register_channel(
    topic="/point_cloud",
    message_encoding="protobuf",
    schema_id=cloud_schema_id,
)

header = Header(seq=0, stamp=Time(sec=int(time.time()), nsec=0), frame_id="example")
fields = [
    PointField(name="x", offset=0, datatype=7, count=1),
    PointField(name="y", offset=4, datatype=7, count=1),
    PointField(name="z", offset=8, datatype=7, count=1),
    PointField(name="intensity", offset=12, datatype=7, count=1),
]

num_points = 100
data = BytesIO()
scale = 2
for i in range(num_points):
    data.write(
        struct.pack(
            "<ffff", scale * random(), scale * random(), scale * random(), random()
        )
    )

message = PointCloud2(
    header=header,
    width=num_points,
    height=1,
    point_step=16,
    row_step=100 * 16,
    fields=fields,
    data=data.getvalue(),
    is_bigendian=False,
    is_dense=True,
)
mcap_writer.add_message(
    channel_id=cloud_schema_id,
    log_time=time.time_ns(),
    data=message.SerializeToString(),  # type: ignore
    publish_time=time.time_ns(),
)

mcap_writer.finish()
output.close()
