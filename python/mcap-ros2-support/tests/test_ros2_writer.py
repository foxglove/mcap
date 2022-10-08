from io import BytesIO

from mcap_ros2.reader import read_ros2_messages
from mcap_ros2.writer import Writer as Ros2Writer


def test_write_messages():
    output = BytesIO()
    ros_writer = Ros2Writer(output=output)
    schema = ros_writer.register_msgdef("test_msgs/TestData", "string a\nint32 b")
    for i in range(0, 10):
        ros_writer.write_message(
            topic="/test",
            schema=schema,
            message={"a": f"string message {i}", "b": i},
            log_time=i,
            publish_time=i,
            sequence=i,
        )
    ros_writer.finish()

    output.seek(0)
    for index, msg in enumerate(read_ros2_messages(output)):
        print(msg.ros_msg)
        assert msg.channel.topic == "/test"
        assert msg.schema.name == "test_msgs/TestData"
        assert msg.ros_msg.a == f"string message {index}"
        assert msg.ros_msg.b == index
        assert msg.log_time_ns == index
        assert msg.publish_time_ns == index
        assert msg.sequence_count == index
