/** Array.from("\x89MCAP0\r\n", (c) => c.charCodeAt(0)) */
export const MCAP0_MAGIC = Object.freeze([137, 77, 67, 65, 80, 48, 13, 10]);

export enum Opcode {
  MIN = 0x01,
  HEADER = 0x01,
  FOOTER = 0x02,
  CHANNEL_INFO = 0x03,
  MESSAGE = 0x04,
  CHUNK = 0x05,
  MESSAGE_INDEX = 0x06,
  CHUNK_INDEX = 0x07,
  ATTACHMENT = 0x08,
  ATTACHMENT_INDEX = 0x09,
  STATISTICS = 0x0a,
  METADATA = 0x0b,
  METADATA_INDEX = 0x0c,
  SUMMARY_OFFSET = 0x0d,
  DATA_END = 0x0e,
  MAX = 0x0e,
}

export function isKnownOpcode(opcode: number): opcode is Opcode {
  return opcode >= Opcode.MIN && opcode <= Opcode.MAX;
}
