from typing import Any

from mcap.opcode import Opcode


class McapError(Exception):
    pass


class InvalidMagic(McapError):
    def __init__(self, bad_magic: Any):
        super().__init__(f"not a valid MCAP file, invalid magic: {bad_magic}")


class DecoderNotFoundError(McapError):
    pass


class EndOfFile(McapError):
    pass


class InvalidRecordLength(McapError):
    def __init__(self, opcode: Opcode, length: int, limit: int):
        super().__init__(
            f"{opcode.name} record has invalid length {length}, limit is set to {limit}",
        )
