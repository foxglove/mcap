import sys

from mcap.reader import make_reader

with open(sys.argv[1], "rb") as f:
    reader = make_reader(f)
    for schema, channel, message in reader.iter_messages(topics=["/diagnostics"]):
        print(f"{channel.topic} ({schema.name}): {message.data}")
