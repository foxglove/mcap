class McapError(Exception):
    pass


class InvalidMagic(McapError):
    def __init__(self, bad_magic):
        super().__init__(f"not a valid MCAP file, invalid magic: {bad_magic}")


class EndOfFile(McapError):
    pass
