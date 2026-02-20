from py_mcap_demo.reader import read_messages
from py_mcap_demo.writer import write_to
from std_msgs.msg import String

from mcap.reader import make_reader


def test_write_then_read(tmp_path):
    mcap_path = tmp_path / "ros2_bags"
    write_to(str(mcap_path))

    (filepath,) = tuple(mcap_path.glob("*.mcap"))

    mcap_reader_count = 0
    with open(filepath, "rb") as f:
        reader = make_reader(f)
        for schema, channel, _ in reader.iter_messages():
            assert schema.encoding == "ros2msg"
            assert schema.name == "std_msgs/msg/String"
            assert channel.message_encoding == "cdr"
            assert channel.topic == "/chatter"
            mcap_reader_count += 1

    assert mcap_reader_count == 10

    rosbag_reader_count = 0
    for topic, msg, timestamp in read_messages(str(mcap_path)):
        assert topic == "/chatter"
        assert isinstance(msg, String)
        assert msg.data.startswith("Chatter")
        rosbag_reader_count += 1

    assert rosbag_reader_count == 10
