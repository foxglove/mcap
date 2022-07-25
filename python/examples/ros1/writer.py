import sys

from mcap_ros1.writer import Writer
from std_msgs.msg import String


with open(sys.argv[1], "wb") as f:
    ros_writer = Writer(f)
    for i in range(0, 10):
        ros_writer.write_message("/chatter", String(data=f"string message {i}"))
    ros_writer.finish()
