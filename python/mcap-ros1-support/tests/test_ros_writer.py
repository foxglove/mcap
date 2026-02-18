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


def test_write_metadata():
    output = BytesIO()
    ros_writer = Ros1Writer(output=output)
    ros_writer.add_metadata("test_metadata", {"key": "value"})
    ros_writer.finish()

    output.seek(0)
    reader = make_reader(output, decoder_factories=[DecoderFactory()])
    for metadata in reader.iter_metadata():
        assert metadata.name == "test_metadata"
        assert metadata.metadata == {"key": "value"}


def test_write_attachment():
    output = BytesIO()
    ros_writer = Ros1Writer(output=output)
    ros_writer.add_attachment(10, 10, "test_attachment", "text/plain", b"test_data")
    ros_writer.finish()

    output.seek(0)
    reader = make_reader(output, decoder_factories=[DecoderFactory()])
    for attachment in reader.iter_attachments():
        assert attachment.name == "test_attachment"
        assert attachment.media_type == "text/plain"
        assert attachment.data == b"test_data"
