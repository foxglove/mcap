from io import BytesIO

from mcap_ros1.decoder import Decoder as Ros1Decoder
from mcap_ros1.writer import Writer as Ros1Writer
from std_msgs.msg import String  # type: ignore


def test_write_messages():
    output = BytesIO()
    ros_writer = Ros1Writer(output=output)
    for i in range(0, 10):
        ros_writer.write_message("chatter", String(data=f"string message {i}"))
    ros_writer.finish()

    output.seek(0)
    decoder = Ros1Decoder(source=output)
    for index, (topic, _record, message) in enumerate(decoder.messages):
        assert topic == "chatter"
        assert message.data == f"string message {index}"
