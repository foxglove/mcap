import { crc32 } from "@foxglove/crc";

import Reader from "./Reader";
import { isKnownOpcode, MCAP_MAGIC, Opcode } from "./constants";
import { McapMagic, TypedMcapRecord } from "./types";

/**
 * Parse a MCAP magic string at `startOffset` in `view`.
 */
export function parseMagic(
  view: DataView,
  startOffset: number,
): { magic: McapMagic; usedBytes: number } | { magic?: undefined; usedBytes: 0 } {
  if (startOffset + MCAP_MAGIC.length > view.byteLength) {
    return { usedBytes: 0 };
  }
  if (!MCAP_MAGIC.every((val, i) => val === view.getUint8(startOffset + i))) {
    throw new Error(
      `Expected MCAP magic '${MCAP_MAGIC.map((val) => val.toString(16).padStart(2, "0")).join(
        " ",
      )}', found '${Array.from(MCAP_MAGIC, (_, i) =>
        view
          .getUint8(startOffset + i)
          .toString(16)
          .padStart(2, "0"),
      ).join(" ")}'`,
    );
  }
  return {
    magic: { specVersion: "0" },
    usedBytes: MCAP_MAGIC.length,
  };
}

/**
 * Parse a MCAP record beginning at `startOffset` in `view`.
 */
export function parseRecord({
  view,
  startOffset,
  validateCrcs,
}: {
  view: DataView;
  startOffset: number;
  validateCrcs: boolean;
}): { record: TypedMcapRecord; usedBytes: number } | { record?: undefined; usedBytes: 0 } {
  if (startOffset + /*opcode*/ 1 + /*record content length*/ 8 >= view.byteLength) {
    return { usedBytes: 0 };
  }
  const headerReader = new Reader(view, startOffset);

  const opcode = headerReader.uint8();

  const recordLength = headerReader.uint64();
  if (recordLength > Number.MAX_SAFE_INTEGER) {
    throw new Error(`Record content length ${recordLength} is too large`);
  }
  const recordLengthNum = Number(recordLength);
  const recordEndOffset = headerReader.offset + recordLengthNum;
  if (recordEndOffset > view.byteLength) {
    return { usedBytes: 0 };
  }

  if (!isKnownOpcode(opcode)) {
    const record: TypedMcapRecord = {
      type: "Unknown",
      opcode,
      data: new Uint8Array(view.buffer, view.byteOffset + headerReader.offset, recordLengthNum),
    };
    return { record, usedBytes: recordEndOffset - startOffset };
  }

  const recordView = new DataView(
    view.buffer,
    view.byteOffset + headerReader.offset,
    recordLengthNum,
  );
  const reader = new Reader(recordView);

  switch (opcode) {
    case Opcode.HEADER: {
      const profile = reader.string();
      const library = reader.string();
      const record: TypedMcapRecord = { type: "Header", profile, library };
      return { record, usedBytes: recordEndOffset - startOffset };
    }

    case Opcode.FOOTER: {
      const summaryStart = reader.uint64();
      const summaryOffsetStart = reader.uint64();
      const summaryCrc = reader.uint32();
      const record: TypedMcapRecord = {
        type: "Footer",
        summaryStart,
        summaryOffsetStart,
        summaryCrc,
      };
      return { record, usedBytes: recordEndOffset - startOffset };
    }

    case Opcode.SCHEMA: {
      const id = reader.uint16();
      const name = reader.string();
      const encoding = reader.string();
      const dataLen = reader.uint32();
      if (reader.offset + dataLen > recordView.byteLength) {
        throw new Error(`Schema data length ${dataLen} exceeds bounds of record`);
      }
      const data = new Uint8Array(
        recordView.buffer.slice(
          recordView.byteOffset + reader.offset,
          recordView.byteOffset + reader.offset + dataLen,
        ),
      );
      reader.offset += dataLen;

      const record: TypedMcapRecord = {
        type: "Schema",
        id,
        encoding,
        name,
        data,
      };

      return { record, usedBytes: recordEndOffset - startOffset };
    }

    case Opcode.CHANNEL: {
      const channelId = reader.uint16();
      const schemaId = reader.uint16();
      const topicName = reader.string();
      const messageEncoding = reader.string();
      const metadata = reader.map(
        (r) => r.string(),
        (r) => r.string(),
      );

      const record: TypedMcapRecord = {
        type: "Channel",
        id: channelId,
        schemaId,
        topic: topicName,
        messageEncoding,
        metadata,
      };

      return { record, usedBytes: recordEndOffset - startOffset };
    }

    case Opcode.MESSAGE: {
      const channelId = reader.uint16();
      const sequence = reader.uint32();
      const logTime = reader.uint64();
      const publishTime = reader.uint64();
      const data = new Uint8Array(
        recordView.buffer.slice(
          recordView.byteOffset + reader.offset,
          recordView.byteOffset + recordView.byteLength,
        ),
      );
      const record: TypedMcapRecord = {
        type: "Message",
        channelId,
        sequence,
        logTime,
        publishTime,
        data,
      };
      return { record, usedBytes: recordEndOffset - startOffset };
    }

    case Opcode.CHUNK: {
      const startTime = reader.uint64();
      const endTime = reader.uint64();
      const uncompressedSize = reader.uint64();
      const uncompressedCrc = reader.uint32();
      const compression = reader.string();
      const recordByteLength = Number(reader.uint64());
      if (recordByteLength + reader.offset > recordView.byteLength) {
        throw new Error("Chunk records length exceeds remaining record size");
      }
      const records = new Uint8Array(
        recordView.buffer.slice(
          recordView.byteOffset + reader.offset,
          recordView.byteOffset + reader.offset + recordByteLength,
        ),
      );
      const record: TypedMcapRecord = {
        type: "Chunk",
        messageStartTime: startTime,
        messageEndTime: endTime,
        compression,
        uncompressedSize,
        uncompressedCrc,
        records,
      };
      return { record, usedBytes: recordEndOffset - startOffset };
    }

    case Opcode.MESSAGE_INDEX: {
      const channelId = reader.uint16();
      const records = reader.keyValuePairs(
        (r) => r.uint64(),
        (r) => r.uint64(),
      );
      const record: TypedMcapRecord = {
        type: "MessageIndex",
        channelId,
        records,
      };
      return { record, usedBytes: recordEndOffset - startOffset };
    }
    case Opcode.CHUNK_INDEX: {
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
      const record: TypedMcapRecord = {
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
      return { record, usedBytes: recordEndOffset - startOffset };
    }
    case Opcode.ATTACHMENT: {
      const logTime = reader.uint64();
      const createTime = reader.uint64();
      const name = reader.string();
      const mediaType = reader.string();
      const dataLen = reader.uint64();
      if (BigInt(recordView.byteOffset + reader.offset) + dataLen > Number.MAX_SAFE_INTEGER) {
        throw new Error(`Attachment too large: ${dataLen}`);
      }
      if (reader.offset + Number(dataLen) + 4 /*crc*/ > recordView.byteLength) {
        throw new Error(`Attachment data length ${dataLen} exceeds bounds of record`);
      }
      const data = new Uint8Array(
        recordView.buffer.slice(
          recordView.byteOffset + reader.offset,
          recordView.byteOffset + reader.offset + Number(dataLen),
        ),
      );
      reader.offset += Number(dataLen);
      const crcLength = reader.offset;
      const expectedCrc = reader.uint32();
      if (validateCrcs && expectedCrc !== 0) {
        const actualCrc = crc32(new DataView(recordView.buffer, recordView.byteOffset, crcLength));
        if (actualCrc !== expectedCrc) {
          throw new Error(
            `Attachment CRC32 mismatch: expected ${expectedCrc}, actual ${actualCrc}`,
          );
        }
      }

      const record: TypedMcapRecord = {
        type: "Attachment",
        logTime,
        createTime,
        name,
        mediaType,
        data,
      };
      return { record, usedBytes: recordEndOffset - startOffset };
    }
    case Opcode.ATTACHMENT_INDEX: {
      const offset = reader.uint64();
      const length = reader.uint64();
      const logTime = reader.uint64();
      const createTime = reader.uint64();
      const dataSize = reader.uint64();
      const name = reader.string();
      const mediaType = reader.string();

      const record: TypedMcapRecord = {
        type: "AttachmentIndex",
        offset,
        length,
        logTime,
        createTime,
        dataSize,
        name,
        mediaType,
      };
      return { record, usedBytes: recordEndOffset - startOffset };
    }
    case Opcode.STATISTICS: {
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

      const record: TypedMcapRecord = {
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
      return { record, usedBytes: recordEndOffset - startOffset };
    }
    case Opcode.METADATA: {
      const name = reader.string();
      const metadata = reader.map(
        (r) => r.string(),
        (r) => r.string(),
      );
      const record: TypedMcapRecord = { type: "Metadata", metadata, name };
      return { record, usedBytes: recordEndOffset - startOffset };
    }
    case Opcode.METADATA_INDEX: {
      const offset = reader.uint64();
      const length = reader.uint64();
      const name = reader.string();

      const record: TypedMcapRecord = {
        type: "MetadataIndex",
        offset,
        length,
        name,
      };
      return { record, usedBytes: recordEndOffset - startOffset };
    }
    case Opcode.SUMMARY_OFFSET: {
      const groupOpcode = reader.uint8();
      const groupStart = reader.uint64();
      const groupLength = reader.uint64();

      const record: TypedMcapRecord = {
        type: "SummaryOffset",
        groupOpcode,
        groupStart,
        groupLength,
      };
      return { record, usedBytes: recordEndOffset - startOffset };
    }
    case Opcode.DATA_END: {
      const dataSectionCrc = reader.uint32();
      const record: TypedMcapRecord = {
        type: "DataEnd",
        dataSectionCrc,
      };
      return { record, usedBytes: recordEndOffset - startOffset };
    }
  }
}
