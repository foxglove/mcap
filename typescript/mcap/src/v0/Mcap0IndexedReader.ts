import { crc32, crc32Final, crc32Init, crc32Update } from "@foxglove/crc";
import Heap from "heap-js";
import { sortedIndexBy } from "lodash";

import { MCAP0_MAGIC } from "./constants";
import { parseMagic, parseRecord } from "./parse";
import { DecompressHandlers, IReadable, TypedMcapRecords } from "./types";

type ChunkCursorParams = {
  chunkIndex: TypedMcapRecords["ChunkIndex"];
  relevantChannels: Set<number> | undefined;
  startTime: bigint | undefined;
  endTime: bigint | undefined;
};

class ChunkCursor {
  chunkIndex: TypedMcapRecords["ChunkIndex"];
  relevantChannels?: Set<number>;
  startTime: bigint | undefined;
  endTime: bigint | undefined;

  messageIndexCursors?: Heap<{
    channelId: number;

    /** index of next message within `records` array */
    index: number;

    records: TypedMcapRecords["MessageIndex"]["records"];
  }>;

  constructor(params: ChunkCursorParams) {
    this.chunkIndex = params.chunkIndex;
    this.relevantChannels = params.relevantChannels;
    this.startTime = params.startTime;
    this.endTime = params.endTime;
  }

  compare(other: ChunkCursor): number {
    // If chunks don't overlap, sort earlier chunk first
    if (this.chunkIndex.messageEndTime < other.chunkIndex.messageStartTime) {
      return -1;
    }
    if (this.chunkIndex.messageStartTime > other.chunkIndex.messageEndTime) {
      return 1;
    }

    // If a cursor has not loaded message indexes, sort it first so it can get loaded and re-sorted
    if (!this.messageIndexCursors) {
      return -1;
    }
    if (!other.messageIndexCursors) {
      return 1;
    }

    // Earlier messages come first
    const cursorA = this.messageIndexCursors.peek();
    if (!cursorA) {
      throw new Error(
        `Unexpected empty cursor for chunk at offset ${this.chunkIndex.chunkStartOffset}`,
      );
    }
    const cursorB = other.messageIndexCursors.peek();
    if (!cursorB) {
      throw new Error(
        `Unexpected empty cursor for chunk at offset ${other.chunkIndex.chunkStartOffset}`,
      );
    }
    const logTimeA = cursorA.records[cursorA.index]![0];
    const logTimeB = cursorB.records[cursorB.index]![0];
    if (logTimeA !== logTimeB) {
      return Number(logTimeA - logTimeB);
    }

    // Break ties by chunk offset in the file
    return Number(this.chunkIndex.chunkStartOffset - other.chunkIndex.chunkStartOffset);
  }

  hasMore(): boolean {
    if (!this.messageIndexCursors) {
      throw new Error("loadMessageIndexCursors() must be called before hasMore()");
    }
    return this.messageIndexCursors.size() > 0;
  }

  popMessage(): [logTime: bigint, offset: bigint] {
    if (!this.messageIndexCursors) {
      throw new Error("loadMessageIndexCursors() must be called before popMessage()");
    }
    const cursor = this.messageIndexCursors.peek();
    if (!cursor) {
      throw new Error(
        `Unexpected popMessage() call when no more messages are available, in chunk at offset ${this.chunkIndex.chunkStartOffset}`,
      );
    }
    const record = cursor.records[cursor.index]!;
    const [logTime] = record;
    if (this.startTime != undefined && logTime < this.startTime) {
      throw new Error(
        `Encountered message with logTime (${logTime}) prior to startTime (${this.startTime}) in chunk at offset ${this.chunkIndex.chunkStartOffset}`,
      );
    }
    if (
      cursor.index + 1 < cursor.records.length &&
      (this.endTime == undefined || cursor.records[cursor.index + 1]![0] <= this.endTime)
    ) {
      cursor.index++;
      this.messageIndexCursors.replace(cursor);
    } else {
      this.messageIndexCursors.pop();
    }
    return record;
  }

  /** FIXME document */
  async loadMessageIndexCursors(readable: IReadable): Promise<void> {
    this.messageIndexCursors = new Heap((a, b) => {
      const logTimeA = a.records[a.index]?.[0];
      const logTimeB = b.records[b.index]?.[0];
      if (logTimeA == undefined) {
        return 1;
      } else if (logTimeB == undefined) {
        return -1;
      }
      return Number(logTimeA - logTimeB);
    });

    let messageIndexStartOffset: bigint | undefined;
    let relevantMessageIndexStartOffset: bigint | undefined;
    for (const [channelId, offset] of this.chunkIndex.messageIndexOffsets) {
      if (messageIndexStartOffset == undefined || offset < messageIndexStartOffset) {
        messageIndexStartOffset = offset;
      }
      if (!this.relevantChannels || this.relevantChannels.has(channelId)) {
        if (
          relevantMessageIndexStartOffset == undefined ||
          offset < relevantMessageIndexStartOffset
        ) {
          relevantMessageIndexStartOffset = offset;
        }
      }
    }
    if (messageIndexStartOffset == undefined || relevantMessageIndexStartOffset == undefined) {
      return;
    }

    // Future optimization: read only message indexes for given channelIds, not all message indexes for the chunk
    const messageIndexEndOffset = messageIndexStartOffset + this.chunkIndex.messageIndexLength;
    const messageIndexes = await readable.read(
      relevantMessageIndexStartOffset,
      messageIndexEndOffset - relevantMessageIndexStartOffset,
    );
    const messageIndexesView = new DataView(
      messageIndexes.buffer,
      messageIndexes.byteOffset,
      messageIndexes.byteLength,
    );

    let offset = 0;
    for (
      let result;
      (result = parseRecord({ view: messageIndexesView, startOffset: offset, validateCrcs: true })),
        result.record;
      offset += result.usedBytes
    ) {
      if (result.record.type !== "MessageIndex") {
        continue;
      }
      if (
        result.record.records.length > 0 &&
        (this.relevantChannels == undefined || this.relevantChannels.has(result.record.channelId))
      ) {
        for (let i = 0; i < result.record.records.length; i++) {
          const [logTime] = result.record.records[i]!;
          if (logTime < this.chunkIndex.messageStartTime) {
            throw new Error(
              `Encountered message index entry in channel ${result.record.channelId} with logTime (${logTime}) earlier than chunk messageStartTime (${this.chunkIndex.messageStartTime}) in chunk at offset ${this.chunkIndex.chunkStartOffset}`,
            );
          }
          if (logTime > this.chunkIndex.messageEndTime) {
            throw new Error(
              `Encountered message index entry in channel ${result.record.channelId} with logTime (${logTime}) later than chunk messageEndTime (${this.chunkIndex.messageEndTime}) in chunk at offset ${this.chunkIndex.chunkStartOffset}`,
            );
          }
          if (i + 1 < result.record.records.length && logTime > result.record.records[i + 1]![0]) {
            throw new Error(
              `Message index entries for channel ${result.record.channelId} in chunk at offset ${
                this.chunkIndex.chunkStartOffset
              } must be sorted by log time (${logTime} > ${result.record.records[i + 1]![0]})`,
            );
          }
        }
        const startIndex =
          this.startTime == undefined
            ? 0
            : sortedIndexBy(result.record.records, [this.startTime], ([logTime]) => logTime);
        if (startIndex >= result.record.records.length) {
          continue;
        }
        this.messageIndexCursors.push({
          index: startIndex,
          channelId: result.record.channelId,
          records: result.record.records,
        });
      }
    }
    if (offset !== messageIndexesView.byteLength) {
      throw new Error(
        `${messageIndexesView.byteLength - offset} bytes remaining in message index section`,
      );
    }
  }
}

export default class Mcap0IndexedReader {
  readonly chunkIndexes: readonly TypedMcapRecords["ChunkIndex"][];
  readonly attachmentIndexes: readonly TypedMcapRecords["AttachmentIndex"][];
  readonly metadataIndexes: readonly TypedMcapRecords["MetadataIndex"][] = [];
  readonly channelsById: ReadonlyMap<number, TypedMcapRecords["Channel"]>;
  readonly schemasById: ReadonlyMap<number, TypedMcapRecords["Schema"]>;
  readonly statistics: TypedMcapRecords["Statistics"] | undefined;
  readonly summaryOffsetsByOpcode: ReadonlyMap<number, TypedMcapRecords["SummaryOffset"]>;
  readonly header: TypedMcapRecords["Header"];
  readonly footer: TypedMcapRecords["Footer"];

  private readable: IReadable;
  private decompressHandlers?: DecompressHandlers;

  private startTime: bigint | undefined;
  private endTime: bigint | undefined;

  private constructor({
    readable,
    chunkIndexes,
    attachmentIndexes,
    metadataIndexes,
    statistics,
    decompressHandlers,
    channelsById,
    schemasById,
    summaryOffsetsByOpcode,
    header,
    footer,
  }: {
    readable: IReadable;
    chunkIndexes: readonly TypedMcapRecords["ChunkIndex"][];
    attachmentIndexes: readonly TypedMcapRecords["AttachmentIndex"][];
    metadataIndexes: readonly TypedMcapRecords["MetadataIndex"][];
    statistics: TypedMcapRecords["Statistics"] | undefined;
    decompressHandlers?: DecompressHandlers;
    channelsById: ReadonlyMap<number, TypedMcapRecords["Channel"]>;
    schemasById: ReadonlyMap<number, TypedMcapRecords["Schema"]>;
    summaryOffsetsByOpcode: ReadonlyMap<number, TypedMcapRecords["SummaryOffset"]>;
    header: TypedMcapRecords["Header"];
    footer: TypedMcapRecords["Footer"];
  }) {
    this.readable = readable;
    this.chunkIndexes = chunkIndexes;
    this.attachmentIndexes = attachmentIndexes;
    this.metadataIndexes = metadataIndexes;
    this.statistics = statistics;
    this.decompressHandlers = decompressHandlers;
    this.channelsById = channelsById;
    this.schemasById = schemasById;
    this.summaryOffsetsByOpcode = summaryOffsetsByOpcode;
    this.header = header;
    this.footer = footer;

    for (const chunk of chunkIndexes) {
      if (this.startTime == undefined || chunk.messageStartTime < this.startTime) {
        this.startTime = chunk.messageStartTime;
      }
      if (this.endTime == undefined || chunk.messageEndTime > this.endTime) {
        this.endTime = chunk.messageEndTime;
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

    let header: TypedMcapRecords["Header"];
    {
      const headerPrefix = await readable.read(
        0n,
        BigInt(MCAP0_MAGIC.length + /* Opcode.HEADER */ 1 + /* record content length */ 8),
      );
      const headerPrefixView = new DataView(
        headerPrefix.buffer,
        headerPrefix.byteOffset,
        headerPrefix.byteLength,
      );
      void parseMagic(headerPrefixView, 0);
      const headerLength = headerPrefixView.getBigUint64(
        MCAP0_MAGIC.length + /* Opcode.HEADER */ 1,
        true,
      );

      const headerRecord = await readable.read(
        BigInt(MCAP0_MAGIC.length),
        /* Opcode.HEADER */ 1n + /* record content length */ 8n + headerLength,
      );
      const headerResult = parseRecord({
        view: new DataView(headerRecord.buffer, headerRecord.byteOffset, headerRecord.byteLength),
        startOffset: 0,
        validateCrcs: true,
      });
      if (headerResult.record?.type !== "Header") {
        throw new Error(
          `Unable to read header at beginning of file; found ${
            headerResult.record?.type ?? "nothing"
          }`,
        );
      }
      if (headerResult.usedBytes !== headerRecord.byteLength) {
        throw new Error(
          `${
            headerRecord.byteLength - headerResult.usedBytes
          } bytes remaining after parsing header`,
        );
      }
      header = headerResult.record;
    }

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

    let footer: TypedMcapRecords["Footer"];
    {
      const footerResult = parseRecord({
        view: footerAndMagicView,
        startOffset: 0,
        validateCrcs: true,
      });
      if (footerResult.record?.type !== "Footer") {
        throw new Error(
          `Unable to read footer from end of file (offset ${footerOffset}); found ${
            footerResult.record?.type ?? "nothing"
          }`,
        );
      }
      if (footerResult.usedBytes !== footerAndMagicView.byteLength - MCAP0_MAGIC.length) {
        throw new Error(
          `${
            footerAndMagicView.byteLength - MCAP0_MAGIC.length - footerResult.usedBytes
          } bytes remaining after parsing footer`,
        );
      }
      footer = footerResult.record;
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

    const channelsById = new Map<number, TypedMcapRecords["Channel"]>();
    const schemasById = new Map<number, TypedMcapRecords["Schema"]>();
    const chunkIndexes: TypedMcapRecords["ChunkIndex"][] = [];
    const attachmentIndexes: TypedMcapRecords["AttachmentIndex"][] = [];
    const metadataIndexes: TypedMcapRecords["MetadataIndex"][] = [];
    const summaryOffsetsByOpcode = new Map<number, TypedMcapRecords["SummaryOffset"]>();
    let statistics: TypedMcapRecords["Statistics"] | undefined;

    let offset = 0;
    for (
      let result;
      (result = parseRecord({ view: indexView, startOffset: offset, validateCrcs: true })),
        result.record;
      offset += result.usedBytes
    ) {
      switch (result.record.type) {
        case "Schema":
          schemasById.set(result.record.id, result.record);
          break;
        case "Channel":
          channelsById.set(result.record.id, result.record);
          break;
        case "ChunkIndex":
          chunkIndexes.push(result.record);
          break;
        case "AttachmentIndex":
          attachmentIndexes.push(result.record);
          break;
        case "MetadataIndex":
          metadataIndexes.push(result.record);
          break;
        case "Statistics":
          if (statistics) {
            throw new Error("Duplicate Statistics record");
          }
          statistics = result.record;
          break;
        case "SummaryOffset":
          summaryOffsetsByOpcode.set(result.record.groupOpcode, result.record);
          break;
        case "Header":
        case "Footer":
        case "Message":
        case "Chunk":
        case "MessageIndex":
        case "Attachment":
        case "Metadata":
        case "DataEnd":
          throw new Error(`${result.record.type} record not allowed in index section`);
        case "Unknown":
          break;
      }
    }
    if (offset !== indexView.byteLength) {
      throw new Error(`${indexView.byteLength - offset} bytes remaining in index section`);
    }

    return new Mcap0IndexedReader({
      readable,
      chunkIndexes,
      attachmentIndexes,
      metadataIndexes,
      statistics,
      decompressHandlers,
      channelsById,
      schemasById,
      summaryOffsetsByOpcode,
      header,
      footer,
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
      for (const channel of this.channelsById.values()) {
        if (topics.includes(channel.topic)) {
          relevantChannels.add(channel.id);
        }
      }
    }

    const chunkCursors = new Heap<ChunkCursor>((a, b) => a.compare(b));
    for (const chunkIndex of this.chunkIndexes) {
      if (chunkIndex.messageStartTime <= endTime && chunkIndex.messageEndTime >= startTime) {
        chunkCursors.push(new ChunkCursor({ chunkIndex, relevantChannels, startTime, endTime }));
      }
    }

    const chunkViewCache = new Map<bigint, DataView>();
    const loadChunkData = async (chunkIndex: TypedMcapRecords["ChunkIndex"]): Promise<DataView> => {
      const chunkData = await this.readable.read(
        chunkIndex.chunkStartOffset,
        chunkIndex.chunkLength,
      );
      const chunkResult = parseRecord({
        view: new DataView(chunkData.buffer, chunkData.byteOffset, chunkData.byteLength),
        startOffset: 0,
        validateCrcs: true,
      });
      if (chunkResult.record?.type !== "Chunk") {
        throw new Error(
          `Chunk start offset ${
            chunkIndex.chunkStartOffset
          } does not point to chunk record (found ${String(chunkResult.record?.type)})`,
        );
      }

      const chunk = chunkResult.record;
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

      return new DataView(buffer.buffer, buffer.byteOffset, buffer.byteLength);
    };

    for (let cursor; (cursor = chunkCursors.peek()); ) {
      if (cursor.messageIndexCursors == undefined) {
        // If we encounter a chunk whose indexes have not been loaded yet, load them and re-organize the heap.
        await cursor.loadMessageIndexCursors(this.readable);
        if (cursor.hasMore()) {
          chunkCursors.replace(cursor);
        } else {
          chunkCursors.pop();
        }
        continue;
      }

      // FIXME: expensive to do get() in loop?
      let chunkView = chunkViewCache.get(cursor.chunkIndex.chunkStartOffset);
      if (!chunkView) {
        chunkView = await loadChunkData(cursor.chunkIndex);
        chunkViewCache.set(cursor.chunkIndex.chunkStartOffset, chunkView);
      }

      const [logTime, offset] = cursor.popMessage();
      if (offset >= BigInt(chunkView.byteLength)) {
        throw new Error(
          `Message offset beyond chunk bounds (log time ${logTime}, offset ${offset}, chunk data length ${chunkView.byteLength}) in chunk at offset ${cursor.chunkIndex.chunkStartOffset}`,
        );
      }
      const result = parseRecord({
        view: chunkView,
        startOffset: Number(offset),
        validateCrcs: true,
      });
      if (!result.record) {
        throw new Error(
          `Unable to parse record at offset ${offset} in chunk at offset ${cursor.chunkIndex.chunkStartOffset}`,
        );
      }
      if (result.record.type !== "Message") {
        throw new Error(
          `Unexpected record type ${result.record.type} in message index (time ${logTime}, offset ${offset} in chunk at offset ${cursor.chunkIndex.chunkStartOffset})`,
        );
      }
      if (result.record.logTime !== logTime) {
        throw new Error(
          `Message log time ${result.record.logTime} did not match message index entry (${logTime} at offset ${offset} in chunk at offset ${cursor.chunkIndex.chunkStartOffset})`,
        );
      }
      yield result.record;

      if (cursor.hasMore()) {
        chunkCursors.replace(cursor);
      } else {
        chunkCursors.pop();
      }
    }
  }
}
