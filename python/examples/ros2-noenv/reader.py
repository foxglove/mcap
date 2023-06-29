import sys

from mcap_ros2.decoder import DecoderFactory

from mcap.reader import make_reader


def main():
    with open(sys.argv[1], "rb") as f:
        reader = make_reader(f, decoder_factories=[DecoderFactory()])
        for schema, channel, message, ros_msg in reader.iter_decoded_messages():
            print(f"{channel.topic} {schema.name} [{message.log_time}]: {ros_msg}")


if __name__ == "__main__":
    main()
