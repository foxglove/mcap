from io import BytesIO

from mcap_ros1.decoder import decode_ros1_messages
from mcap.mcap0.reader import make_reader
from mcap_ros1.writer import Writer as Ros1Writer
from std_msgs.msg import String  # type: ignore


def test_write_messages():
    output = BytesIO()
    ros_writer = Ros1Writer(output=output)
    for i in range(0, 10):
        ros_writer.write_message("/chatter", String(data=f"string message {i}"), i)
    ros_writer.finish()

    output.seek(0)
    reader = make_reader(output)
    message_iterator = decode_ros1_messages(reader.iter_messages())
    for index, (topic, message, timestamp) in enumerate(message_iterator):
        assert topic == "/chatter"
        assert message.data == f"string message {index}"
        assert timestamp == index
