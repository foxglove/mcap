import sys
from pathlib import Path

from mcap.Mcap0StreamReader import Mcap0StreamReader
from mcap.Records import DataEnd


def main():
    text = Path(sys.argv[1]).read_text().splitlines()
    print(text)
    writer = Mcap0StreamWriter(open(sys.argv[1], "rb"))
    writer = Mcap0StreamWriter(open(sys.argv[1], "rb"))
    records = [r for r in reader.records() if not isinstance(r, DataEnd)]
    for r in records:
        print(r.stringify())


if __name__ == "__main__":
    main()
