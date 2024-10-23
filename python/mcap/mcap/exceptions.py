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


class RecordLengthLimitExceeded(McapError):
    def __init__(self, opcode: int, length: int, limit: int):
        opcode_name = f"unknown (opcode {opcode})"
        try:
            opcode_name = Opcode(opcode).name
        except ValueError:
            # unknown opcode will trigger a ValueError
            pass
        super().__init__(
            f"{opcode_name} record has length {length} that exceeds limit {limit}",
        )
