import sys

from mcap.reader import make_reader
from mcap_protobuf.decoder import DecoderFactory


def main():
    with open(sys.argv[1], "rb") as infile:
        reader = make_reader(infile, decoder_factories=[DecoderFactory()])
        for schema, channel, message, proto_msg in reader.iter_decoded_messages():
            print(f"{channel.topic} {schema.name} [{message.log_time}]: {proto_msg}")


if __name__ == "__main__":
    main()
