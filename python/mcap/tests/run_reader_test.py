import json
import sys
from typing import Any, Dict, List, Set, Tuple, Union

from mcap.records import MessageIndex
from mcap.serialization import stringify_record
from mcap.stream_reader import StreamReader
from mcap.reader import SeekingReader


def main():
    if sys.argv[2] == "streamed":
        reader = StreamReader(open(sys.argv[1], "rb"))
        records = [
            stringify_record(r)
            for r in reader.records
            if not isinstance(r, MessageIndex)
        ]
        print(json.dumps({"records": records}, indent=2))
    else:
        reader = SeekingReader(open(sys.argv[1], "rb"))
        result: Dict[str, List[Dict[str, Union[str, List[Tuple[str, Any]]]]]] = {
            "schemas": [],
            "channels": [],
            "messages": [],
            "statistics": [],
        }
        known_schemas: Set[int] = set()
        known_channels: Set[int] = set()
        for schema, channel, message in reader.iter_messages():
            if schema.id not in known_schemas:
                result["schemas"].append(stringify_record(schema))
                known_schemas.add(schema.id)

            if channel.id not in known_channels:
                result["channels"].append(stringify_record(channel))
                known_channels.add(channel.id)

            result["messages"].append(stringify_record(message))

        summary = reader.get_summary()
        if summary is not None and summary.statistics is not None:
            result["statistics"].append(stringify_record(summary.statistics))

        print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
