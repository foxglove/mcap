import sys

from mcap.mcap0.stream_reader import StreamReader
from mcap_protobuf.decoder import Decoder


def main():
    reader = StreamReader(sys.argv[1])
    decoder = Decoder(reader)
    for topic, message in decoder.messages:
        print(f"{topic}: {message}")


if __name__ == "__main__":
    main()
