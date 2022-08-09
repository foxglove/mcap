import sys

from mcap.mcap0.reader import make_reader
from mcap_protobuf.decoder import decode_proto_messages


def main():
    with open(sys.argv[1], "rb") as f:
        mcap_iterator = make_reader(f).iter_messages()
        for topic, message, timestamp in decode_proto_messages(mcap_iterator):
            print(f"{topic} [{timestamp}]: {message}")


if __name__ == "__main__":
    main()
