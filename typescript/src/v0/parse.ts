import { crc32 } from "@foxglove/crc";
import { isEqual } from "lodash";

import Reader from "../common/Reader";
import { isKnownOpcode, MCAP0_MAGIC, Opcode } from "./constants";
import { McapMagic, McapRecord, ChannelInfo, UnknownRecord } from "./types";

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
    magic: { type: "Magic", specVersion: "0" },
    usedBytes: MCAP0_MAGIC.length,
  };
}

/**
 * Parse a MCAP record beginning at `startOffset` in `view`.
 *
 * @param channelInfosById Used to track ChannelInfo objects across calls to `parseRecord` and
 * associate them with newly parsed Message records.
 * @param channelInfosSeenInThisChunk Used to validate that messages are preceded by a corresponding
 * ChannelInfo within the same chunk.
 */
export function parseRecord({
  view,
  startOffset,
  channelInfosById,
  channelInfosSeenInThisChunk,
  validateCrcs,
}: {
  view: DataView;
  startOffset: number;
  channelInfosById: Map<number, ChannelInfo>;
  channelInfosSeenInThisChunk: Set<number>;
  validateCrcs: boolean;
}): { record: McapRecord; usedBytes: number } | { record?: undefined; usedBytes: 0 } {
  if (startOffset + 5 >= view.byteLength) {
    return { usedBytes: 0 };
  }
  const headerReader = new Reader(view, startOffset);

  const opcode = headerReader.uint8();

  const recordLength = headerReader.uint64();
  if (recordLength > Number.MAX_SAFE_INTEGER) {
    throw new Error(`Record length ${recordLength} is too large`);
  }
  const recordEndOffset = headerReader.offset + Number(recordLength);
  if (recordEndOffset > view.byteLength) {
    return { usedBytes: 0 };
  }

  if (!isKnownOpcode(opcode)) {
    const record: UnknownRecord = {
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
      const metadata = reader.keyValuePairs(
        (r) => r.string(),
        (r) => r.string(),
      );
      const record: McapRecord = { type: "Header", profile, library, metadata };
      return { record, usedBytes: recordEndOffset - startOffset };
    }

    case Opcode.FOOTER: {
      const indexOffset = reader.uint64();
      const indexCrc = reader.uint32();
      const record: McapRecord = { type: "Footer", indexOffset, indexCrc };
      return { record, usedBytes: recordEndOffset - startOffset };
    }

    case Opcode.CHANNEL_INFO: {
      const channelId = reader.uint16();
      const topicName = reader.string();
      const encoding = reader.string();
      const schemaName = reader.string();
      const schema = reader.string();
      const userData = reader.keyValuePairs(
        (r) => r.string(),
        (r) => r.string(),
      );
      const crcLength = reader.offset;
      const expectedCrc = reader.uint32();
      if (validateCrcs && expectedCrc !== 0) {
        const actualCrc = crc32(new DataView(recordView.buffer, recordView.byteOffset, crcLength));
        if (actualCrc !== expectedCrc) {
          throw new Error(
            `Channel Info CRC32 mismatch: expected ${expectedCrc}, actual ${actualCrc}`,
          );
        }
      }

      const record: McapRecord = {
        type: "ChannelInfo",
        channelId,
        topicName,
        encoding,
        schemaName,
        schema,
        userData,
      };
      channelInfosSeenInThisChunk.add(channelId);
      const existingInfo = channelInfosById.get(channelId);
      if (existingInfo) {
        if (!isEqual(existingInfo, record)) {
          throw new Error(`differing channel infos for ${record.channelId}`);
        }
        return {
          record: existingInfo,
          usedBytes: recordEndOffset - startOffset,
        };
      } else {
        channelInfosById.set(channelId, record);
        return { record, usedBytes: recordEndOffset - startOffset };
      }
    }

    case Opcode.MESSAGE: {
      const channelId = reader.uint16();
      const channelInfo = channelInfosById.get(channelId);
      if (!channelInfo) {
        throw new Error(`Encountered message on channel ${channelId} without prior channel info`);
      }
      if (!channelInfosSeenInThisChunk.has(channelId)) {
        throw new Error(
          `Encountered message on channel ${channelId} without prior channel info in this chunk; channel info must be repeated within each chunk where the channel is used`,
        );
      }
      const sequence = reader.uint32();
      const publishTime = reader.uint64();
      const recordTime = reader.uint64();
      const messageData = recordView.buffer.slice(
        recordView.byteOffset + reader.offset,
        recordView.byteOffset + recordView.byteLength,
      );
      const record: McapRecord = {
        type: "Message",
        channelInfo,
        sequence,
        publishTime,
        recordTime,
        messageData,
      };
      return { record, usedBytes: recordEndOffset - startOffset };
    }

    case Opcode.CHUNK: {
      const uncompressedSize = reader.uint64();
      const uncompressedCrc = reader.uint32();
      const compression = reader.string();
      const records = recordView.buffer.slice(
        recordView.byteOffset + reader.offset,
        recordView.byteOffset + recordView.byteLength,
      );
      const record: McapRecord = {
        type: "Chunk",
        compression,
        uncompressedSize,
        uncompressedCrc,
        records,
      };
      channelInfosSeenInThisChunk.clear();
      return { record, usedBytes: recordEndOffset - startOffset };
    }

    case Opcode.MESSAGE_INDEX: {
      const channelId = reader.uint16();
      const count = reader.uint32();
      const records = reader.keyValuePairs(
        (r) => r.uint64(),
        (r) => r.uint64(),
      );
      const crcLength = reader.offset;
      const expectedCrc = reader.uint32();
      if (validateCrcs && expectedCrc !== 0) {
        const actualCrc = crc32(new DataView(recordView.buffer, recordView.byteOffset, crcLength));
        if (actualCrc !== expectedCrc) {
          throw new Error(
            `Message Index CRC32 mismatch: expected ${expectedCrc}, actual ${actualCrc}`,
          );
        }
      }
      const record: McapRecord = {
        type: "MessageIndex",
        channelId,
        count,
        records,
      };
      return { record, usedBytes: recordEndOffset - startOffset };
    }
    case Opcode.CHUNK_INDEX: {
      const startTime = reader.uint64();
      const endTime = reader.uint64();
      const chunkOffset = reader.uint64();
      const messageIndexOffsets = reader.map(
        (r) => r.uint16(),
        (r) => r.uint64(),
      );
      const messageIndexLength = reader.uint64();
      const compression = reader.string();
      const compressedSize = reader.uint64();
      const uncompressedSize = reader.uint64();
      const crcLength = reader.offset;
      const expectedCrc = reader.uint32();
      if (validateCrcs && expectedCrc !== 0) {
        const actualCrc = crc32(new DataView(recordView.buffer, recordView.byteOffset, crcLength));
        if (actualCrc !== expectedCrc) {
          throw new Error(
            `Chunk Index CRC32 mismatch: expected ${expectedCrc}, actual ${actualCrc}`,
          );
        }
      }
      const record: McapRecord = {
        type: "ChunkIndex",
        startTime,
        endTime,
        chunkOffset,
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
      const recordTime = reader.uint64();
      const contentType = reader.string();
      const dataLen = reader.uint64();
      if (BigInt(recordView.byteOffset + reader.offset) + dataLen > Number.MAX_SAFE_INTEGER) {
        throw new Error(`Attachment too large: ${dataLen}`);
      }
      const data = recordView.buffer.slice(
        recordView.byteOffset + reader.offset,
        recordView.byteOffset + reader.offset + Number(dataLen),
      );
      reader.offset += Number(dataLen);
      const crcLength = reader.offset;
      const expectedCrc = reader.uint32();
      if (validateCrcs && expectedCrc !== 0) {
        const actualCrc = crc32(
          new DataView(view.buffer, startOffset + 5, crcLength - (startOffset + 5)),
        );
        if (actualCrc !== expectedCrc) {
          throw new Error(
            `Attachment CRC32 mismatch: expected ${expectedCrc}, actual ${actualCrc}`,
          );
        }
      }

      const record: McapRecord = {
        type: "Attachment",
        name,
        recordTime,
        contentType,
        data,
      };
      return { record, usedBytes: recordEndOffset - startOffset };
    }
    case Opcode.ATTACHMENT_INDEX: {
      const recordTime = reader.uint64();
      const attachmentSize = reader.uint64();
      const name = reader.string();
      const contentType = reader.string();
      const attachmentOffset = reader.uint64();

      const record: McapRecord = {
        type: "AttachmentIndex",
        recordTime,
        attachmentSize,
        name,
        contentType,
        offset: attachmentOffset,
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

      const record: McapRecord = {
        type: "Statistics",
        messageCount,
        channelCount,
        attachmentCount,
        chunkCount,
        channelMessageCounts,
      };
      return { record, usedBytes: recordEndOffset - startOffset };
    }
  }
}
