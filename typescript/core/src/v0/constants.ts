/** Array.from("\x89MCAP0\r\n", (c) => c.charCodeAt(0)) */
export const MCAP0_MAGIC = Object.freeze([137, 77, 67, 65, 80, 48, 13, 10]);

export const DETECT_VERSION_BYTES_REQUIRED = MCAP0_MAGIC.length;

export enum Opcode {
  MIN = 0x01,
  HEADER = 0x01,
  FOOTER = 0x02,
  SCHEMA = 0x03,
  CHANNEL = 0x04,
  MESSAGE = 0x05,
  CHUNK = 0x06,
  MESSAGE_INDEX = 0x07,
  CHUNK_INDEX = 0x08,
  ATTACHMENT = 0x09,
  ATTACHMENT_INDEX = 0x0a,
  STATISTICS = 0x0b,
  METADATA = 0x0c,
  METADATA_INDEX = 0x0d,
  SUMMARY_OFFSET = 0x0e,
  DATA_END = 0x0f,
  MAX = 0x0f,
}

export function isKnownOpcode(opcode: number): opcode is Opcode {
  return opcode >= Opcode.MIN && opcode <= Opcode.MAX;
}
