import json
import sys

from mcap.mcap0.records import MessageIndex
from mcap.mcap0.serialization import stringify_record
from mcap.mcap0.stream_reader import StreamReader


def main():
    reader = StreamReader(open(sys.argv[1], "rb"))
    records = [
        stringify_record(r) for r in reader.records if not isinstance(r, MessageIndex)
    ]
    print(json.dumps({"records": records}, indent=2))


if __name__ == "__main__":
    main()
