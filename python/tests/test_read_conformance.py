import difflib
import unittest
from pathlib import Path

from mcap0.StreamReader import StreamReader
from mcap0.Records import DataEnd


class McapStreamingReaderConformanceTestCase(unittest.TestCase):
    def setUp(self) -> None:
        path = Path("../tests/conformance/data/")
        self.files = list(path.rglob("*.mcap"))

    def test_conformances(self):
        for file in self.files:
            print(file.name)
            expected_path = file.with_suffix(".expected.txt")
            expected_text = expected_path.read_text().splitlines()
            reader = StreamReader(open(file, "rb"))
            output = [
                r.stringify() for r in reader.records() if not isinstance(r, DataEnd)
            ]
            diff = difflib.unified_diff(expected_text, output)
            if any(diff):
                print("### " + file.name + " ###")
                print("\n".join(diff))
                print()
