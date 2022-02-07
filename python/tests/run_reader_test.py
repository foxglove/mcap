import sys

from mcap.mcap0.stream_reader import StreamReader
from mcap.mcap0.records import DataEnd


def main():
    reader = StreamReader(open(sys.argv[1], "rb"))
    records = [r for r in reader.records() if not isinstance(r, DataEnd)]
    for r in records:
        print(r.stringify())


if __name__ == "__main__":
    main()
