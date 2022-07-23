from enum import IntEnum, unique


@unique
class Opcode(IntEnum):
    ATTACHMENT = 0x09
    ATTACHMENT_INDEX = 0x0A
    CHANNEL = 0x04
    CHUNK = 0x06
    CHUNK_INDEX = 0x08
    DATA_END = 0x0F
    FOOTER = 0x02
    HEADER = 0x01
    MESSAGE = 0x05
    MESSAGE_INDEX = 0x07
    METADATA = 0x0C
    METADATA_INDEX = 0x0D
    SCHEMA = 0x03
    STATISTICS = 0x0B
    SUMMARY_OFFSET = 0x0E
