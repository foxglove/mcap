import sys

from mcap_ros1.decoder import Decoder

with open(sys.argv[1], "rb") as f:
    ros_reader = Decoder(f)
    for topic, underlying_record, message in ros_reader.messages:
        print(f"{topic}: ({type(message).__name__}): {message.data}")
