import sys

from mcap_protobuf.decoder import DecoderFactory

from mcap.reader import make_reader


def main():
    with open(sys.argv[1], "rb") as f:
        reader = make_reader(f, decoder_factories=[DecoderFactory()])
        for schema, channel, message, proto_msg in reader.iter_decoded_messages():
            print(f"{channel.topic} {schema.name} [{message.log_time}]: {proto_msg}")


if __name__ == "__main__":
    main()
