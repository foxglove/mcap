import sys

from mcap.mcap0.records import DataEnd
from mcap.mcap0.serialization import stringify_record
from mcap.mcap0.stream_reader import StreamReader


def main():
    reader = StreamReader(open(sys.argv[1], "rb"))
    records = [r for r in reader.records() if not isinstance(r, DataEnd)]
    for r in records:
        print(stringify_record(r))


if __name__ == "__main__":
    main()
