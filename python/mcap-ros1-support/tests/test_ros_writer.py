from io import BytesIO

from mcap_ros1.decoder import DecoderFactory
from mcap_ros1.writer import Writer as Ros1Writer
from std_msgs.msg import String  # type: ignore

from mcap.reader import make_reader


def read_ros1_messages(stream: BytesIO):
    reader = make_reader(stream, decoder_factories=[DecoderFactory()])
    return reader.iter_decoded_messages()


def test_write_messages():
    output = BytesIO()
    ros_writer = Ros1Writer(output=output)
    for i in range(0, 10):
        ros_writer.write_message("/chatter", String(data=f"string message {i}"), i)
    ros_writer.finish()

    output.seek(0)
    for index, msg in enumerate(read_ros1_messages(output)):
        assert msg.channel.topic == "/chatter"
        assert msg.decoded_message.data == f"string message {index}"
        assert msg.message.log_time == index


def test_get_underlying_writer():
    output = BytesIO()
    ros_writer = Ros1Writer(output=output)
    underlying_writer = ros_writer.writer
    assert underlying_writer
    assert ros_writer != underlying_writer
