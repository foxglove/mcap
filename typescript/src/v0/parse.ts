import { crc32 } from "@foxglove/crc";

import Reader from "./Reader";
import { isKnownOpcode, MCAP0_MAGIC, Opcode } from "./constants";
import { McapMagic, TypedMcapRecord } from "./types";

/**
 * Parse a MCAP magic string at `startOffset` in `view`.
 */
export function parseMagic(
  view: DataView,
  startOffset: number,
): { magic: McapMagic; usedBytes: number } | { magic?: undefined; usedBytes: 0 } {
  if (startOffset + MCAP0_MAGIC.length > view.byteLength) {
    return { usedBytes: 0 };
  }
  if (!MCAP0_MAGIC.every((val, i) => val === view.getUint8(startOffset + i))) {
    throw new Error(
      `Expected MCAP magic '${MCAP0_MAGIC.map((val) => val.toString(16).padStart(2, "0")).join(
        " ",
      )}', found '${Array.from(MCAP0_MAGIC, (_, i) =>
        view.getUint8(i).toString(16).padStart(2, "0"),
      ).join(" ")}'`,
    );
  }
  return {
    magic: { specVersion: "0" },
    usedBytes: MCAP0_MAGIC.length,
  };
}

/**
 * Parse a MCAP record beginning at `startOffset` in `view`.
 *
 * @param channelInfosById Used to track ChannelInfo objects across calls to `parseRecord` and
 * associate them with newly parsed Message records.
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
  const recordEndOffset = headerReader.offset + Number(recordLength);
  if (recordEndOffset > view.byteLength) {
    return { usedBytes: 0 };
  }

  if (!isKnownOpcode(opcode)) {
    const record: TypedMcapRecord = {
      type: "Unknown",
      opcode,
      data: new Uint8Array(
        view.buffer,
        view.byteOffset + headerReader.offset,
        Number(recordLength),
      ),
    };
    return { record, usedBytes: recordEndOffset - startOffset };
  }

  const recordView = new DataView(
    view.buffer,
    view.byteOffset + headerReader.offset,
    Number(recordLength),
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
      const schemaId = reader.uint16();
      const schemaEncoding = reader.string();
      const schema = reader.string();
      const schemaName = reader.string();

      const record: TypedMcapRecord = {
        type: "Schema",
        id: schemaId,
        schemaEncoding,
        schemaName,
        schema,
      };

      return { record, usedBytes: recordEndOffset - startOffset };
    }

    case Opcode.CHANNEL_INFO: {
      const channelId = reader.uint16();
      const topicName = reader.string();
      const messageEncoding = reader.string();
      const schemaId = reader.uint16();
      const metadata = reader.keyValuePairs(
        (r) => r.string(),
        (r) => r.string(),
      );

      const record: TypedMcapRecord = {
        type: "ChannelInfo",
        id: channelId,
        topic: topicName,
        messageEncoding,
        schemaId,
        metadata,
      };

      return { record, usedBytes: recordEndOffset - startOffset };
    }

    case Opcode.MESSAGE: {
      const channelId = reader.uint16();
      const sequence = reader.uint32();
      const publishTime = reader.uint64();
      const logTime = reader.uint64();
      const messageData = new Uint8Array(
        recordView.buffer.slice(
          recordView.byteOffset + reader.offset,
          recordView.byteOffset + recordView.byteLength,
        ),
      );
      const record: TypedMcapRecord = {
        type: "Message",
        channelId,
        sequence,
        publishTime,
        logTime,
        messageData,
      };
      return { record, usedBytes: recordEndOffset - startOffset };
    }

    case Opcode.CHUNK: {
      const startTime = reader.uint64();
      const endTime = reader.uint64();
      const uncompressedSize = reader.uint64();
      const uncompressedCrc = reader.uint32();
      const compression = reader.string();
      const records = new Uint8Array(
        recordView.buffer.slice(
          recordView.byteOffset + reader.offset,
          recordView.byteOffset + recordView.byteLength,
        ),
      );
      const record: TypedMcapRecord = {
        type: "Chunk",
        startTime,
        endTime,
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
      const startTime = reader.uint64();
      const endTime = reader.uint64();
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
        startTime,
        endTime,
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
      const name = reader.string();
      const createdAt = reader.uint64();
      const logTime = reader.uint64();
      const contentType = reader.string();
      const dataLen = reader.uint64();
      if (BigInt(recordView.byteOffset + reader.offset) + dataLen > Number.MAX_SAFE_INTEGER) {
        throw new Error(`Attachment too large: ${dataLen}`);
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
        name,
        createdAt,
        logTime,
        contentType,
        data,
      };
      return { record, usedBytes: recordEndOffset - startOffset };
    }
    case Opcode.ATTACHMENT_INDEX: {
      const offset = reader.uint64();
      const length = reader.uint64();
      const logTime = reader.uint64();
      const dataSize = reader.uint64();
      const name = reader.string();
      const contentType = reader.string();

      const record: TypedMcapRecord = {
        type: "AttachmentIndex",
        offset,
        length,
        logTime,
        dataSize,
        name,
        contentType,
      };
      return { record, usedBytes: recordEndOffset - startOffset };
    }
    case Opcode.STATISTICS: {
      const messageCount = reader.uint64();
      const channelCount = reader.uint32();
      const attachmentCount = reader.uint32();
      const chunkCount = reader.uint32();
      const channelMessageCounts = reader.map(
        (r) => r.uint16(),
        (r) => r.uint64(),
      );

      const record: TypedMcapRecord = {
        type: "Statistics",
        messageCount,
        channelCount,
        attachmentCount,
        chunkCount,
        channelMessageCounts,
      };
      return { record, usedBytes: recordEndOffset - startOffset };
    }
    case Opcode.METADATA: {
      const name = reader.string();
      const metadata = reader.keyValuePairs(
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
