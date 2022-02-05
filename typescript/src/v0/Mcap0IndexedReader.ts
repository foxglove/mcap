import { crc32, crc32Final, crc32Init, crc32Update } from "@foxglove/crc";
import Heap from "heap-js";

import { getBigUint64 } from "../common/getBigUint64";
import { IReadable } from "./IReadable";
import { MCAP0_MAGIC, Opcode } from "./constants";
import { parseMagic, parseRecord } from "./parse";
import { DecompressHandlers, TypedMcapRecords } from "./types";

export default class Mcap0IndexedReader {
  readonly chunkIndexes: readonly TypedMcapRecords["ChunkIndex"][];
  readonly attachmentIndexes: readonly TypedMcapRecords["AttachmentIndex"][];
  readonly channelInfosById: ReadonlyMap<number, TypedMcapRecords["ChannelInfo"]>;
  readonly statistics: TypedMcapRecords["Statistics"] | undefined;

  private readable: IReadable;
  private decompressHandlers?: DecompressHandlers;

  private startTime: bigint | undefined;
  private endTime: bigint | undefined;

  private constructor({
    readable,
    chunkIndexes,
    attachmentIndexes,
    statistics,
    decompressHandlers,
    channelInfosById,
  }: {
    readable: IReadable;
    chunkIndexes: readonly TypedMcapRecords["ChunkIndex"][];
    attachmentIndexes: readonly TypedMcapRecords["AttachmentIndex"][];
    statistics: TypedMcapRecords["Statistics"] | undefined;
    decompressHandlers?: DecompressHandlers;
    channelInfosById: Map<number, TypedMcapRecords["ChannelInfo"]>;
  }) {
    this.readable = readable;
    this.chunkIndexes = chunkIndexes;
    this.attachmentIndexes = attachmentIndexes;
    this.statistics = statistics;
    this.decompressHandlers = decompressHandlers;
    this.channelInfosById = channelInfosById;

    for (const chunk of chunkIndexes) {
      if (this.startTime == undefined || chunk.startTime < this.startTime) {
        this.startTime = chunk.startTime;
      }
      if (this.endTime == undefined || chunk.endTime > this.endTime) {
        this.endTime = chunk.endTime;
      }
    }
  }

  static async Initialize({
    readable,
    decompressHandlers,
  }: {
    readable: IReadable;

    /**
     * When a compressed chunk is encountered, the entry in `decompressHandlers` corresponding to the
     * compression will be called to decompress the chunk data.
     */
    decompressHandlers?: DecompressHandlers;
  }): Promise<Mcap0IndexedReader> {
    const size = await readable.size();
    let footerOffset: bigint;
    let footerAndMagicView: DataView;
    {
      const headerLengthLowerBound = BigInt(
        MCAP0_MAGIC.length +
          /* Opcode.HEADER */ 1 +
          /* record content length */ 8 +
          /* profile length */ 4 +
          /* library length */ 4,
      );
      const footerAndMagicReadLength = BigInt(
        /* Opcode.FOOTER */ 1 +
          /* record content length */ 8 +
          /* summaryStart */ 8 +
          /* summaryOffsetStart */ 8 +
          /* crc */ 4 +
          MCAP0_MAGIC.length,
      );
      if (size < headerLengthLowerBound + footerAndMagicReadLength) {
        throw new Error(`File size (${size}) is too small to be valid MCAP`);
      }
      footerOffset = size - footerAndMagicReadLength;
      const footerBuffer = await readable.read(footerOffset, footerAndMagicReadLength);

      footerAndMagicView = new DataView(
        footerBuffer.buffer,
        footerBuffer.byteOffset,
        footerBuffer.byteLength,
      );
    }

    void parseMagic(footerAndMagicView, footerAndMagicView.byteLength - MCAP0_MAGIC.length);

    const channelInfosById = new Map<number, TypedMcapRecords["ChannelInfo"]>();

    const footer = parseRecord({
      view: footerAndMagicView,
      startOffset: 0,
      validateCrcs: true,
    }).record;
    if (footer?.type !== "Footer") {
      throw new Error(
        `Unable to read footer from end of file (offset ${footerOffset}); found ${
          footer?.type ?? "nothing"
        }`,
      );
    }
    if (footer.summaryStart === 0n) {
      throw new Error("File is not indexed");
    }

    // Future optimization: avoid holding whole summary blob in memory at once
    const allSummaryData = await readable.read(
      footer.summaryStart,
      footerOffset - footer.summaryStart,
    );
    if (footer.summaryCrc !== 0) {
      let indexCrc = crc32Init();
      indexCrc = crc32Update(indexCrc, allSummaryData);
      indexCrc = crc32Update(
        indexCrc,
        new DataView(
          footerAndMagicView.buffer,
          footerAndMagicView.byteOffset,
          /* Opcode.FOOTER */ 1 +
            /* record content length */ 8 +
            /* summary start */ 8 +
            /* summary offset start */ 8,
        ),
      );
      indexCrc = crc32Final(indexCrc);
      if (indexCrc !== footer.summaryCrc) {
        throw new Error(`Incorrect index CRC ${indexCrc} (expected ${footer.summaryCrc})`);
      }
    }

    const indexView = new DataView(
      allSummaryData.buffer,
      allSummaryData.byteOffset,
      allSummaryData.byteLength,
    );

    const chunkIndexes: TypedMcapRecords["ChunkIndex"][] = [];
    const attachmentIndexes: TypedMcapRecords["AttachmentIndex"][] = [];
    let statistics: TypedMcapRecords["Statistics"] | undefined;

    let offset = 0;
    for (
      let result;
      (result = parseRecord({
        view: indexView,
        startOffset: offset,
        validateCrcs: true,
      })),
        result.record;
      offset += result.usedBytes
    ) {
      switch (result.record.type) {
        case "Schema":
          // duplicates
          break;
        case "ChannelInfo":
          channelInfosById.set(result.record.id, result.record);
          break;
        case "ChunkIndex":
          chunkIndexes.push(result.record);
          break;
        case "AttachmentIndex":
          attachmentIndexes.push(result.record);
          break;
        case "Statistics":
          if (statistics) {
            throw new Error("Duplicate Statistics record");
          }
          statistics = result.record;
          break;
        case "Unknown":
          break;
        default:
          throw new Error(`${result.record.type} record not allowed in index section`);
      }
    }
    if (offset !== indexView.byteLength) {
      throw new Error(`${indexView.byteLength - offset} bytes remaining in index section`);
    }

    return new Mcap0IndexedReader({
      readable,
      chunkIndexes,
      attachmentIndexes,
      statistics,
      decompressHandlers,
      channelInfosById,
    });
  }

  async *readMessages({
    topics,
    startTime = this.startTime,
    endTime = this.endTime,
  }: {
    topics?: readonly string[];
    startTime?: bigint;
    endTime?: bigint;
  } = {}): AsyncGenerator<TypedMcapRecords["Message"], void, void> {
    if (startTime == undefined || endTime == undefined) {
      return;
    }

    let relevantChannels: Set<number> | undefined;
    if (topics) {
      relevantChannels = new Set();
      for (const channelInfo of this.channelInfosById.values()) {
        if (topics.includes(channelInfo.topic)) {
          relevantChannels.add(channelInfo.id);
        }
      }
    }

    const relevantChunks = this.chunkIndexes.filter(
      (chunk) => chunk.startTime <= endTime && chunk.endTime >= startTime,
    );

    for (let i = 0; i + 1 < relevantChunks.length; i++) {
      if (relevantChunks[i]!.endTime > relevantChunks[i + 1]!.startTime) {
        throw new Error(
          `Overlapping chunks are not currently supported; chunk at offset ${
            relevantChunks[i]!.chunkStartOffset
          } ends at ${relevantChunks[i]!.endTime} and chunk at offset ${
            relevantChunks[i + 1]!.chunkStartOffset
          } starts at ${relevantChunks[i + 1]!.startTime}`,
        );
      }
    }
    for (const chunkIndex of relevantChunks) {
      yield* this.readChunk({ chunkIndex, channelIds: relevantChannels, startTime, endTime });
    }
  }

  private async *readChunk({
    chunkIndex,
    channelIds,
    startTime,
    endTime,
  }: {
    chunkIndex: TypedMcapRecords["ChunkIndex"];
    channelIds: ReadonlySet<number> | undefined;
    startTime: bigint;
    endTime: bigint;
  }): AsyncGenerator<TypedMcapRecords["Message"], void, void> {
    const chunkOpcodeAndLength = await this.readable.read(chunkIndex.chunkStartOffset, 1n + 8n);
    const chunkOpcodeAndLengthView = new DataView(
      chunkOpcodeAndLength.buffer,
      chunkOpcodeAndLength.byteOffset,
      chunkOpcodeAndLength.byteLength,
    );
    if (chunkOpcodeAndLengthView.getUint8(0) !== Opcode.CHUNK) {
      throw new Error(
        `Chunk index offset does not point to chunk record (expected opcode ${
          Opcode.CHUNK
        }, found ${chunkOpcodeAndLengthView.getUint8(0)})`,
      );
    }
    const chunkRecordLength = getBigUint64.call(chunkOpcodeAndLengthView, 1, true);

    // Future optimization: read only message indexes for given channelIds, not all message indexes for the chunk
    const chunkAndMessageIndexes = await this.readable.read(
      chunkIndex.chunkStartOffset,
      1n + 8n + chunkRecordLength + chunkIndex.messageIndexLength,
    );
    const chunkAndMessageIndexesView = new DataView(
      chunkAndMessageIndexes.buffer,
      chunkAndMessageIndexes.byteOffset,
      chunkAndMessageIndexes.byteLength,
    );

    let chunk: TypedMcapRecords["Chunk"];
    const messageIndexCursors = new Heap<{
      index: number;
      channelId: number;
      records: TypedMcapRecords["MessageIndex"]["records"];
    }>((a, b) => {
      const logTimeA = a.records[a.index]?.[0];
      const logTimeB = b.records[b.index]?.[0];
      if (logTimeA == undefined) {
        return 1;
      } else if (logTimeB == undefined) {
        return -1;
      }
      return Number(logTimeA - logTimeB);
    });

    {
      let offset = 0;
      const chunkResult = parseRecord({
        view: chunkAndMessageIndexesView,
        startOffset: offset,
        validateCrcs: true,
      });
      offset += chunkResult.usedBytes;
      if (chunkResult.record?.type !== "Chunk") {
        throw new Error(
          `Chunk index offset does not point to chunk record (found ${String(
            chunkResult.record?.type,
          )})`,
        );
      }
      chunk = chunkResult.record;

      for (
        let result;
        (result = parseRecord({
          view: chunkAndMessageIndexesView,
          startOffset: offset,
          validateCrcs: true,
        })),
          result.record;
        offset += result.usedBytes
      ) {
        if (result.record.type !== "MessageIndex") {
          throw new Error(`Unexpected record type ${result.record.type} in message index section`);
        }
        if (
          result.record.records.length > 0 &&
          (channelIds == undefined || channelIds.has(result.record.channelId))
        ) {
          for (let i = 0; i + 1 < result.record.records.length; i++) {
            if (result.record.records[i]![0] > result.record.records[i + 1]![0]) {
              throw new Error(
                `Message index entries for channel ${result.record.channelId} in chunk at offset ${chunkIndex.chunkStartOffset} must be sorted by log time`,
              );
            }
          }
          messageIndexCursors.push({
            index: 0,
            channelId: result.record.channelId,
            records: result.record.records,
          });
        }
      }
      if (offset !== chunkAndMessageIndexesView.byteLength) {
        throw new Error(
          `${
            chunkAndMessageIndexesView.byteLength - offset
          } bytes remaining in message index section`,
        );
      }
    }

    let buffer = chunk.records;
    if (chunk.compression !== "" && buffer.byteLength > 0) {
      const decompress = this.decompressHandlers?.[chunk.compression];
      if (!decompress) {
        throw new Error(`Unsupported compression ${chunk.compression}`);
      }
      buffer = decompress(buffer, chunk.uncompressedSize);
    }
    if (chunk.uncompressedCrc !== 0) {
      const chunkCrc = crc32(buffer);
      if (chunkCrc !== chunk.uncompressedCrc) {
        throw new Error(`Incorrect chunk CRC ${chunkCrc} (expected ${chunk.uncompressedCrc})`);
      }
    }

    const recordsView = new DataView(buffer.buffer, buffer.byteOffset, buffer.byteLength);

    let cursor;
    while ((cursor = messageIndexCursors.peek())) {
      const [logTime, offset] = cursor.records[cursor.index]!;
      if (logTime >= startTime && logTime <= endTime) {
        if (BigInt(recordsView.byteOffset) + offset >= Number.MAX_SAFE_INTEGER) {
          throw new Error(
            `Message offset too large (log time ${logTime}, offset ${offset}) in channel ${cursor.channelId} in chunk at offset ${chunkIndex.chunkStartOffset}`,
          );
        }
        const result = parseRecord({
          view: recordsView,
          startOffset: Number(offset),
          validateCrcs: true,
        });
        if (!result.record) {
          throw new Error(
            `Unable to parse record at offset ${offset} in chunk at offset ${chunkIndex.chunkStartOffset}`,
          );
        }
        if (result.record.type !== "Message") {
          throw new Error(
            `Unexpected record type ${result.record.type} in message index (time ${logTime}, offset ${offset} in chunk at offset ${chunkIndex.chunkStartOffset})`,
          );
        }
        if (result.record.logTime !== logTime) {
          throw new Error(
            `Message log time ${result.record.logTime} did not match message index entry (${logTime} at offset ${offset} in chunk at offset ${chunkIndex.chunkStartOffset})`,
          );
        }
        yield result.record;
      }

      if (cursor.index + 1 < cursor.records.length && logTime <= endTime) {
        cursor.index++;
        messageIndexCursors.replace(cursor);
      } else {
        messageIndexCursors.pop();
      }
    }
  }
}
