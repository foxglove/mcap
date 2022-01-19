export enum RecordType {
  MIN = 0x01,
  CHANNEL_INFO = 0x01,
  MESSAGE = 0x02,
  CHUNK = 0x03,
  INDEX_DATA = 0x04,
  CHUNK_INFO = 0x05,
  FOOTER = 0x06,
  MAX = 0x06,
}

/** Array.from("\x89MCAP\r\n\n", (c) => c.charCodeAt(0)) */
export const MCAP_MAGIC = Object.freeze([137, 77, 67, 65, 80, 13, 10, 10]);
