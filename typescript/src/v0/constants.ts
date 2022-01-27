/** Array.from("\x89MCAP0\r\n", (c) => c.charCodeAt(0)) */
export const MCAP0_MAGIC = Object.freeze([137, 77, 67, 65, 80, 48, 13, 10]);

export enum Opcode {
  MIN = 0x00,
  HEADER = 0x00,
  FOOTER = 0x7f,
  CHANNEL_INFO = 0x01,
  MESSAGE = 0x02,
  CHUNK = 0x03,
  MESSAGE_INDEX = 0x04,
  CHUNK_INDEX = 0x05,
  ATTACHMENT = 0x06,
  ATTACHMENT_INDEX = 0x07,
  STATISTICS = 0x08,
  METADATA = 0x09,
  SUMMARY_OFFSET = 0x0a,

  // max opcode excluding footer
  MAX = 0x0a,
}

export function isKnownOpcode(opcode: number): opcode is Opcode {
  return opcode >= Opcode.MIN && opcode <= Opcode.MAX && opcode !== Opcode.FOOTER;
}
