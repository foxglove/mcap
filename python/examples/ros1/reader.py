import sys

from mcap_ros1.reader import read_ros1_messages


def main():
    for msg in read_ros1_messages(sys.argv[1]):
        print(
            f"{msg.topic} [{msg.log_time}] ({type(msg.ros_msg).__name__}): {msg.ros_msg.data}"
        )


if __name__ == "__main__":
    main()
