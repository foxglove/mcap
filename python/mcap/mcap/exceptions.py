class McapError(Exception):
    pass


class InvalidMagic(McapError):
    def __init__(self):
        super().__init__("not a valid MCAP file")


class EndOfFile(McapError):
    pass
