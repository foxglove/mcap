from io import BytesIO

from mcap_ros1.reader import read_ros1_messages
from mcap_ros1.writer import Writer as Ros1Writer
from std_msgs.msg import String  # type: ignore


def test_write_messages():
    output = BytesIO()
    ros_writer = Ros1Writer(output=output)
    for i in range(0, 10):
        ros_writer.write_message("/chatter", String(data=f"string message {i}"), i)
    ros_writer.finish()

    output.seek(0)
    for index, msg in enumerate(read_ros1_messages(output)):
        assert msg.topic == "/chatter"
        assert msg.ros_msg.data == f"string message {index}"
        assert msg.log_time_ns == index
