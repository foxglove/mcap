import sys

from mcap.mcap0.reader import make_reader
from mcap_ros1.decoder import decode_ros1_messages

with open(sys.argv[1], "rb") as f:
    reader = make_reader(f)
    ros1_messages = decode_ros1_messages(reader.iter_messages())
    for topic, message, log_time in ros1_messages:
        print(f"{topic} [log_time] ({type(message).__name__}): {message.data}")
