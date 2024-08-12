import { crc32 } from "@foxglove/crc";

import Reader from "./Reader";
import { MCAP_MAGIC, Opcode } from "./constants";
import { McapMagic, TypedMcapRecord } from "./types";

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
// eslint-disable-next-line @foxglove/no-boolean-parameters
export function parseRecord(reader: Reader, validateCrcs = false): TypedMcapRecord | undefined {
  const RECORD_HEADER_SIZE = 1 /*opcode*/ + 8; /*record content length*/
  if (reader.bytesRemaining() < RECORD_HEADER_SIZE) {
    return undefined;
  }
  const opcode = reader.uint8();
  const recordLength = reader.uint64();

  if (recordLength > Number.MAX_SAFE_INTEGER) {
    throw new Error(`Record content length ${recordLength} is too large`);
  }

  const recordLengthNum = Number(recordLength);

  if (reader.bytesRemaining() < recordLengthNum) {
    reader.rewind(RECORD_HEADER_SIZE);
    return undefined;
  }

  switch (opcode as Opcode) {
    case Opcode.HEADER:
      return parseHeader(reader);
    case Opcode.FOOTER:
      return parseFooter(reader);
    case Opcode.SCHEMA:
      return parseSchema(reader, recordLengthNum);
    case Opcode.CHANNEL:
      return parseChannel(reader);
    case Opcode.MESSAGE:
      return parseMessage(reader, recordLengthNum);
    case Opcode.CHUNK:
      return parseChunk(reader, recordLengthNum);
    case Opcode.MESSAGE_INDEX:
      return parseMessageIndex(reader);
    case Opcode.CHUNK_INDEX:
      return parseChunkIndex(reader);
    case Opcode.ATTACHMENT:
      return parseAttachment(reader, recordLengthNum, validateCrcs);
    case Opcode.ATTACHMENT_INDEX:
      return parseAttachmentIndex(reader);
    case Opcode.STATISTICS:
      return parseStatistics(reader);
    case Opcode.METADATA:
      return parseMetadata(reader);
    case Opcode.METADATA_INDEX:
      return parseMetadataIndex(reader);
    case Opcode.SUMMARY_OFFSET:
      return parseSummaryOffset(reader);
    case Opcode.DATA_END:
      return parseDataEnd(reader);
    default:
      return parseUnknown(reader, recordLengthNum, opcode);
  }
}

function parseUnknown(reader: Reader, recordLength: number, opcode: number): TypedMcapRecord {
  const data = reader.u8ArrayBorrow(recordLength);
  return {
    type: "Unknown",
    opcode,
    data,
  };
}

function parseHeader(reader: Reader): TypedMcapRecord {
  const profile = reader.string();
  const library = reader.string();
  return { type: "Header", profile, library };
}

function parseFooter(reader: Reader): TypedMcapRecord {
  const summaryStart = reader.uint64();
  const summaryOffsetStart = reader.uint64();
  const summaryCrc = reader.uint32();
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

  return {
    type: "Schema",
    id,
    encoding,
    name,
    data,
  };
}

function parseChannel(reader: Reader): TypedMcapRecord {
  const channelId = reader.uint16();
  const schemaId = reader.uint16();
  const topicName = reader.string();
  const messageEncoding = reader.string();
  const metadata = reader.map(
    (r) => r.string(),
    (r) => r.string(),
  );

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
  const channelId = reader.uint16();
  const sequence = reader.uint32();
  const logTime = reader.uint64();
  const publishTime = reader.uint64();
  const data = reader.u8ArrayCopy(recordLength - 22 /*channelId, sequence, logTime, publishTime*/);
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
  const recordByteLength = Number(reader.uint64());
  const end = reader.offset;
  if (recordLength - (end - start) < recordByteLength) {
    throw new Error("Chunk records length exceeds remaining record size");
  }
  const records = reader.u8ArrayCopy(recordByteLength);
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

function parseMessageIndex(reader: Reader): TypedMcapRecord {
  const channelId = reader.uint16();
  const records = reader.keyValuePairs(
    (r) => r.uint64(),
    (r) => r.uint64(),
  );
  return {
    type: "MessageIndex",
    channelId,
    records,
  };
}

function parseChunkIndex(reader: Reader): TypedMcapRecord {
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
  // eslint-disable-next-line @foxglove/no-boolean-parameters
  validateCrcs: boolean,
): TypedMcapRecord {
  const start = reader.offset;
  const logTime = reader.uint64();
  const createTime = reader.uint64();
  const name = reader.string();
  const mediaType = reader.string();
  const dataLen = reader.uint64();
  const end = reader.offset;
  // NOTE: probably not necessary, but just in case
  if (BigInt(reader.offset) + dataLen > Number.MAX_SAFE_INTEGER) {
    throw new Error(`Attachment too large: ${dataLen}`);
  }
  if (recordLength - (end - start) < Number(dataLen) + 4 /*crc*/) {
    throw new Error(`Attachment data length ${dataLen} exceeds bounds of record`);
  }
  const data = reader.u8ArrayCopy(Number(dataLen));
  const crcEnd = reader.offset;
  const expectedCrc = reader.uint32();
  if (validateCrcs && expectedCrc !== 0) {
    reader.offset = start;
    const fullData = reader.u8ArrayBorrow(recordLength - 4);
    const actualCrc = crc32(fullData);
    reader.offset = crcEnd + 4;
    if (actualCrc !== expectedCrc) {
      throw new Error(`Attachment CRC32 mismatch: expected ${expectedCrc}, actual ${actualCrc}`);
    }
  }

  return {
    type: "Attachment",
    logTime,
    createTime,
    name,
    mediaType,
    data,
  };
}

function parseAttachmentIndex(reader: Reader): TypedMcapRecord {
  const offset = reader.uint64();
  const length = reader.uint64();
  const logTime = reader.uint64();
  const createTime = reader.uint64();
  const dataSize = reader.uint64();
  const name = reader.string();
  const mediaType = reader.string();

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

function parseStatistics(reader: Reader): TypedMcapRecord {
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

function parseMetadata(reader: Reader): TypedMcapRecord {
  const name = reader.string();
  const metadata = reader.map(
    (r) => r.string(),
    (r) => r.string(),
  );
  return { type: "Metadata", metadata, name };
}

function parseMetadataIndex(reader: Reader): TypedMcapRecord {
  const offset = reader.uint64();
  const length = reader.uint64();
  const name = reader.string();

  return {
    type: "MetadataIndex",
    offset,
    length,
    name,
  };
}

function parseSummaryOffset(reader: Reader): TypedMcapRecord {
  const groupOpcode = reader.uint8();
  const groupStart = reader.uint64();
  const groupLength = reader.uint64();

  return {
    type: "SummaryOffset",
    groupOpcode,
    groupStart,
    groupLength,
  };
}

function parseDataEnd(reader: Reader): TypedMcapRecord {
  const dataSectionCrc = reader.uint32();
  return {
    type: "DataEnd",
    dataSectionCrc,
  };
}
