import { crc32 } from "@foxglove/crc";

import Reader from "./Reader.ts";
import { MCAP_MAGIC, Opcode } from "./constants.ts";
import type { McapMagic, TypedMcapRecord } from "./types.ts";

/**
 * Parse a MCAP magic string at `startOffset` in `view`.
 */
export function parseMagic(reader: Reader): McapMagic | undefined {
  if (reader.bytesRemaining() < MCAP_MAGIC.length) {
    return undefined;
  }
  const magic = reader.u8ArrayBorrow(MCAP_MAGIC.length);
  if (!MCAP_MAGIC.every((val, i) => val === magic[i])) {
    throw new Error(
      `Expected MCAP magic '${MCAP_MAGIC.map((val) => val.toString(16).padStart(2, "0")).join(
        " ",
      )}', found '${Array.from(magic, (_, i) => magic[i]!.toString(16).padStart(2, "0")).join(
        " ",
      )}'`,
    );
  }
  return { specVersion: "0" };
}

/**
 * Parse a MCAP record from the given reader
 */
// NOTE: internal function in the hot path, (de)structuring  args would be wasteful, acceptable perf/clarity tradeoff
// eslint-disable-next-line @foxglove/no-boolean-parameters
export function parseRecord(reader: Reader, validateCrcs = false): TypedMcapRecord | undefined {
  const RECORD_HEADER_SIZE = 1 /*opcode*/ + 8; /*record content length*/
  if (reader.bytesRemaining() < RECORD_HEADER_SIZE) {
    return undefined;
  }
  const start = reader.offset;
  const opcode = reader.uint8();
  const recordLength = reader.uint64();

  if (recordLength > Number.MAX_SAFE_INTEGER) {
    throw new Error(`Record content length ${recordLength} is too large`);
  }

  const recordLengthNum = Number(recordLength);

  if (reader.bytesRemaining() < recordLengthNum) {
    reader.offset = start; // Rewind to the start of the record
    return undefined;
  }

  let result: TypedMcapRecord;
  switch (opcode as Opcode) {
    case Opcode.HEADER:
      result = parseHeader(reader, recordLengthNum);
      break;
    case Opcode.FOOTER:
      result = parseFooter(reader, recordLengthNum);
      break;
    case Opcode.SCHEMA:
      result = parseSchema(reader, recordLengthNum);
      break;
    case Opcode.CHANNEL:
      result = parseChannel(reader, recordLengthNum);
      break;
    case Opcode.MESSAGE:
      result = parseMessage(reader, recordLengthNum);
      break;
    case Opcode.CHUNK:
      result = parseChunk(reader, recordLengthNum);
      break;
    case Opcode.MESSAGE_INDEX:
      result = parseMessageIndex(reader, recordLengthNum);
      break;
    case Opcode.CHUNK_INDEX:
      result = parseChunkIndex(reader, recordLengthNum);
      break;
    case Opcode.ATTACHMENT:
      result = parseAttachment(reader, recordLengthNum, validateCrcs);
      break;
    case Opcode.ATTACHMENT_INDEX:
      result = parseAttachmentIndex(reader, recordLengthNum);
      break;
    case Opcode.STATISTICS:
      result = parseStatistics(reader, recordLengthNum);
      break;
    case Opcode.METADATA:
      result = parseMetadata(reader, recordLengthNum);
      break;
    case Opcode.METADATA_INDEX:
      result = parseMetadataIndex(reader, recordLengthNum);
      break;
    case Opcode.SUMMARY_OFFSET:
      result = parseSummaryOffset(reader, recordLengthNum);
      break;
    case Opcode.DATA_END:
      result = parseDataEnd(reader, recordLengthNum);
      break;
    default:
      result = parseUnknown(reader, recordLengthNum, opcode);
      break;
  }

  // NOTE: a bit redundant, but ensures we've advanced by the full record length
  // TODO: simplify this when we explore monomorphic paths
  reader.offset = start + RECORD_HEADER_SIZE + recordLengthNum;

  return result;
}

function parseUnknown(reader: Reader, recordLength: number, opcode: number): TypedMcapRecord {
  const data = reader.u8ArrayBorrow(recordLength);
  return {
    type: "Unknown",
    opcode,
    data,
  };
}

function parseHeader(reader: Reader, recordLength: number): TypedMcapRecord {
  const startOffset = reader.offset;
  const profile = reader.string();
  const library = reader.string();
  reader.offset = startOffset + recordLength;
  return { type: "Header", profile, library };
}

function parseFooter(reader: Reader, recordLength: number): TypedMcapRecord {
  const startOffset = reader.offset;
  const summaryStart = reader.uint64();
  const summaryOffsetStart = reader.uint64();
  const summaryCrc = reader.uint32();
  reader.offset = startOffset + recordLength;
  return {
    type: "Footer",
    summaryStart,
    summaryOffsetStart,
    summaryCrc,
  };
}

function parseSchema(reader: Reader, recordLength: number): TypedMcapRecord {
  const start = reader.offset;
  const id = reader.uint16();
  const name = reader.string();
  const encoding = reader.string();
  const dataLen = reader.uint32();
  const end = reader.offset;
  if (recordLength - (end - start) < dataLen) {
    throw new Error(`Schema data length ${dataLen} exceeds bounds of record`);
  }
  const data = reader.u8ArrayCopy(dataLen);
  reader.offset = start + recordLength;

  return {
    type: "Schema",
    id,
    encoding,
    name,
    data,
  };
}

function parseChannel(reader: Reader, recordLength: number): TypedMcapRecord {
  const startOffset = reader.offset;
  const channelId = reader.uint16();
  const schemaId = reader.uint16();
  const topicName = reader.string();
  const messageEncoding = reader.string();
  const metadata = reader.map(
    (r) => r.string(),
    (r) => r.string(),
  );
  reader.offset = startOffset + recordLength;

  return {
    type: "Channel",
    id: channelId,
    schemaId,
    topic: topicName,
    messageEncoding,
    metadata,
  };
}

function parseMessage(reader: Reader, recordLength: number): TypedMcapRecord {
  const MESSAGE_PREFIX_SIZE = 2 + 4 + 8 + 8; // channelId, sequence, logTime, publishTime
  const channelId = reader.uint16();
  const sequence = reader.uint32();
  const logTime = reader.uint64();
  const publishTime = reader.uint64();
  const data = reader.u8ArrayCopy(recordLength - MESSAGE_PREFIX_SIZE);
  return {
    type: "Message",
    channelId,
    sequence,
    logTime,
    publishTime,
    data,
  };
}

function parseChunk(reader: Reader, recordLength: number): TypedMcapRecord {
  const start = reader.offset;
  const startTime = reader.uint64();
  const endTime = reader.uint64();
  const uncompressedSize = reader.uint64();
  const uncompressedCrc = reader.uint32();
  const compression = reader.string();
  const recordsByteLength = Number(reader.uint64());
  const end = reader.offset;
  const prefixSize = end - start;
  if (recordsByteLength + prefixSize > recordLength) {
    throw new Error("Chunk records length exceeds remaining record size");
  }
  const records = reader.u8ArrayCopy(recordsByteLength);
  reader.offset = start + recordLength;
  return {
    type: "Chunk",
    messageStartTime: startTime,
    messageEndTime: endTime,
    compression,
    uncompressedSize,
    uncompressedCrc,
    records,
  };
}

function parseMessageIndex(reader: Reader, recordLength: number): TypedMcapRecord {
  const startOffset = reader.offset;
  const channelId = reader.uint16();
  const records = reader.keyValuePairs(
    (r) => r.uint64(),
    (r) => r.uint64(),
  );
  reader.offset = startOffset + recordLength;
  return {
    type: "MessageIndex",
    channelId,
    records,
  };
}

function parseChunkIndex(reader: Reader, recordLength: number): TypedMcapRecord {
  const startOffset = reader.offset;
  const messageStartTime = reader.uint64();
  const messageEndTime = reader.uint64();
  const chunkStartOffset = reader.uint64();
  const chunkLength = reader.uint64();
  const messageIndexOffsets = reader.map(
    (r) => r.uint16(),
    (r) => r.uint64(),
  );
  const messageIndexLength = reader.uint64();
  const compression = reader.string();
  const compressedSize = reader.uint64();
  const uncompressedSize = reader.uint64();
  reader.offset = startOffset + recordLength;
  return {
    type: "ChunkIndex",
    messageStartTime,
    messageEndTime,
    chunkStartOffset,
    chunkLength,
    messageIndexOffsets,
    messageIndexLength,
    compression,
    compressedSize,
    uncompressedSize,
  };
}

function parseAttachment(
  reader: Reader,
  recordLength: number,
  // NOTE: internal function in the hot path, (de)structuring  args would be wasteful, acceptable perf/clarity tradeoff
  // eslint-disable-next-line @foxglove/no-boolean-parameters
  validateCrcs: boolean,
): TypedMcapRecord {
  const startOffset = reader.offset;
  const logTime = reader.uint64();
  const createTime = reader.uint64();
  const name = reader.string();
  const mediaType = reader.string();
  const dataLen = reader.uint64();
  // NOTE: probably not necessary, but just in case
  if (BigInt(reader.offset) + dataLen > Number.MAX_SAFE_INTEGER) {
    throw new Error(`Attachment too large: ${dataLen}`);
  }
  if (reader.offset + Number(dataLen) + 4 /*crc*/ > startOffset + recordLength) {
    throw new Error(`Attachment data length ${dataLen} exceeds bounds of record`);
  }
  const data = reader.u8ArrayCopy(Number(dataLen));
  const crcLength = reader.offset - startOffset;
  const expectedCrc = reader.uint32();
  if (validateCrcs && expectedCrc !== 0) {
    reader.offset = startOffset;
    const fullData = reader.u8ArrayBorrow(crcLength);
    const actualCrc = crc32(fullData);
    reader.offset = startOffset + crcLength + 4;
    if (actualCrc !== expectedCrc) {
      throw new Error(`Attachment CRC32 mismatch: expected ${expectedCrc}, actual ${actualCrc}`);
    }
  }
  reader.offset = startOffset + recordLength;

  return {
    type: "Attachment",
    logTime,
    createTime,
    name,
    mediaType,
    data,
  };
}

function parseAttachmentIndex(reader: Reader, recordLength: number): TypedMcapRecord {
  const startOffset = reader.offset;
  const offset = reader.uint64();
  const length = reader.uint64();
  const logTime = reader.uint64();
  const createTime = reader.uint64();
  const dataSize = reader.uint64();
  const name = reader.string();
  const mediaType = reader.string();
  reader.offset = startOffset + recordLength;

  return {
    type: "AttachmentIndex",
    offset,
    length,
    logTime,
    createTime,
    dataSize,
    name,
    mediaType,
  };
}

function parseStatistics(reader: Reader, recordLength: number): TypedMcapRecord {
  const startOffset = reader.offset;
  const messageCount = reader.uint64();
  const schemaCount = reader.uint16();
  const channelCount = reader.uint32();
  const attachmentCount = reader.uint32();
  const metadataCount = reader.uint32();
  const chunkCount = reader.uint32();
  const messageStartTime = reader.uint64();
  const messageEndTime = reader.uint64();
  const channelMessageCounts = reader.map(
    (r) => r.uint16(),
    (r) => r.uint64(),
  );
  reader.offset = startOffset + recordLength;

  return {
    type: "Statistics",
    messageCount,
    schemaCount,
    channelCount,
    attachmentCount,
    metadataCount,
    chunkCount,
    messageStartTime,
    messageEndTime,
    channelMessageCounts,
  };
}

function parseMetadata(reader: Reader, recordLength: number): TypedMcapRecord {
  const startOffset = reader.offset;
  const name = reader.string();
  const metadata = reader.map(
    (r) => r.string(),
    (r) => r.string(),
  );
  reader.offset = startOffset + recordLength;
  return { type: "Metadata", metadata, name };
}

function parseMetadataIndex(reader: Reader, recordLength: number): TypedMcapRecord {
  const startOffset = reader.offset;
  const offset = reader.uint64();
  const length = reader.uint64();
  const name = reader.string();
  reader.offset = startOffset + recordLength;

  return {
    type: "MetadataIndex",
    offset,
    length,
    name,
  };
}

function parseSummaryOffset(reader: Reader, recordLength: number): TypedMcapRecord {
  const startOffset = reader.offset;
  const groupOpcode = reader.uint8();
  const groupStart = reader.uint64();
  const groupLength = reader.uint64();
  reader.offset = startOffset + recordLength;

  return {
    type: "SummaryOffset",
    groupOpcode,
    groupStart,
    groupLength,
  };
}

function parseDataEnd(reader: Reader, recordLength: number): TypedMcapRecord {
  const startOffset = reader.offset;
  const dataSectionCrc = reader.uint32();
  reader.offset = startOffset + recordLength;
  return {
    type: "DataEnd",
    dataSectionCrc,
  };
}
