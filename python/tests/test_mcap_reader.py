import unittest

from mcap0.StreamReader import StreamReader


class McapReaderTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self.reader = StreamReader(open("../testdata/mcap/demo.mcap", "rb"))

    def test_read(self):
        for record in self.reader.records():
            print(record.__class__)
