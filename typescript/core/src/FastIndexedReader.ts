import { crc32, crc32Final, crc32Init, crc32Update } from "@foxglove/crc";
import assert from "assert";

import { MCAP_MAGIC, Opcode, isKnownOpcode } from "./constants";
import { parseMagic, parseRecord } from "./parse";
import { ChunkIndex, DecompressHandlers, IReadable, TypedMcapRecords } from "./types";

type FastIndexedReaderArgs = {
  readable: IReadable;
  chunkIndexes: readonly TypedMcapRecords["ChunkIndex"][];
  metadataIndexes: readonly TypedMcapRecords["MetadataIndex"][];
  statistics: TypedMcapRecords["Statistics"] | undefined;
  decompressHandlers?: DecompressHandlers;
  channelsById: ReadonlyMap<number, TypedMcapRecords["Channel"]>;
  schemasById: ReadonlyMap<number, TypedMcapRecords["Schema"]>;
  summaryOffsetsByOpcode: ReadonlyMap<number, TypedMcapRecords["SummaryOffset"]>;
  header: TypedMcapRecords["Header"];
  footer: TypedMcapRecords["Footer"];
  dataEndOffset: bigint;
  dataSectionCrc?: number;
};

type ChunkSlot = {
  buf: DataView;
  unreadMessages: number;
};

type MessageIndex = {
  offset: number;
  timestamp: bigint;
  chunkSlotIndex: number;
};

export class FastIndexedReader {
  readonly chunkIndexes: readonly TypedMcapRecords["ChunkIndex"][];
  readonly channelsById: ReadonlyMap<number, TypedMcapRecords["Channel"]>;
  readonly schemasById: ReadonlyMap<number, TypedMcapRecords["Schema"]>;
  readonly header: TypedMcapRecords["Header"];
  // Used for appending attachments/metadata to existing MCAP files
  readonly dataEndOffset: bigint;
  readonly dataSectionCrc?: number;

  #readable: IReadable;
  #decompressHandlers?: DecompressHandlers;

  #messageStartTime: bigint | undefined;
  #messageEndTime: bigint | undefined;

  private constructor(args: FastIndexedReaderArgs) {
    this.#readable = args.readable;
    this.chunkIndexes = args.chunkIndexes;
    this.#decompressHandlers = args.decompressHandlers;
    this.channelsById = args.channelsById;
    this.schemasById = args.schemasById;
    this.dataEndOffset = args.dataEndOffset;
    this.dataSectionCrc = args.dataSectionCrc;
    this.header = args.header;

    for (const chunk of args.chunkIndexes) {
      if (this.#messageStartTime == undefined || chunk.messageStartTime < this.#messageStartTime) {
        this.#messageStartTime = chunk.messageStartTime;
      }
      if (this.#messageEndTime == undefined || chunk.messageEndTime > this.#messageEndTime) {
        this.#messageEndTime = chunk.messageEndTime;
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
  }): Promise<FastIndexedReader> {
    const size = await readable.size();

    let header: TypedMcapRecords["Header"];
    let headerEndOffset: bigint;
    {
      const headerPrefix = await readable.read(
        0n,
        BigInt(MCAP_MAGIC.length + /* Opcode.HEADER */ 1 + /* record content length */ 8),
      );
      const headerPrefixView = new DataView(
        headerPrefix.buffer,
        headerPrefix.byteOffset,
        headerPrefix.byteLength,
      );
      void parseMagic(headerPrefixView, 0);
      const headerContentLength = headerPrefixView.getBigUint64(
        MCAP_MAGIC.length + /* Opcode.HEADER */ 1,
        true,
      );
      const headerReadLength =
        /* Opcode.HEADER */ 1n + /* record content length */ 8n + headerContentLength;

      const headerRecord = await readable.read(BigInt(MCAP_MAGIC.length), headerReadLength);
      headerEndOffset = BigInt(MCAP_MAGIC.length) + headerReadLength;
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

    function errorWithLibrary(message: string): Error {
      return new Error(`${message} [library=${header.library}]`);
    }

    let footerOffset: bigint;
    let footerAndMagicView: DataView;
    {
      const headerLengthLowerBound = BigInt(
        MCAP_MAGIC.length +
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
          MCAP_MAGIC.length,
      );
      if (size < headerLengthLowerBound + footerAndMagicReadLength) {
        throw errorWithLibrary(`File size (${size}) is too small to be valid MCAP`);
      }
      footerOffset = size - footerAndMagicReadLength;
      const footerBuffer = await readable.read(footerOffset, footerAndMagicReadLength);

      footerAndMagicView = new DataView(
        footerBuffer.buffer,
        footerBuffer.byteOffset,
        footerBuffer.byteLength,
      );
    }

    try {
      void parseMagic(footerAndMagicView, footerAndMagicView.byteLength - MCAP_MAGIC.length);
    } catch (error) {
      throw errorWithLibrary((error as Error).message);
    }

    let footer: TypedMcapRecords["Footer"];
    {
      const footerResult = parseRecord({
        view: footerAndMagicView,
        startOffset: 0,
        validateCrcs: true,
      });
      if (footerResult.record?.type !== "Footer") {
        throw errorWithLibrary(
          `Unable to read footer from end of file (offset ${footerOffset}); found ${
            footerResult.record?.type ?? "nothing"
          }`,
        );
      }
      if (footerResult.usedBytes !== footerAndMagicView.byteLength - MCAP_MAGIC.length) {
        throw errorWithLibrary(
          `${
            footerAndMagicView.byteLength - MCAP_MAGIC.length - footerResult.usedBytes
          } bytes remaining after parsing footer`,
        );
      }
      footer = footerResult.record;
    }
    if (footer.summaryStart === 0n) {
      throw errorWithLibrary("File is not indexed");
    }

    // Copy the footer prefix before reading the summary because calling readable.read() may reuse the buffer.
    const footerPrefix = new Uint8Array(
      /* Opcode.FOOTER */ 1 +
        /* record content length */ 8 +
        /* summary start */ 8 +
        /* summary offset start */ 8,
    );
    footerPrefix.set(
      new Uint8Array(
        footerAndMagicView.buffer,
        footerAndMagicView.byteOffset,
        footerPrefix.byteLength,
      ),
    );

    const dataEndLength =
      /* Opcode.DATA_END */ 1n + /* record content length */ 8n + /* data_section_crc */ 4n;

    const dataEndOffset = footer.summaryStart - dataEndLength;
    if (dataEndOffset < headerEndOffset) {
      throw errorWithLibrary(
        `Expected DataEnd position (summary start ${footer.summaryStart} - ${dataEndLength} = ${dataEndOffset}) to be after Header end offset (${headerEndOffset})`,
      );
    }

    // Future optimization: avoid holding whole summary blob in memory at once
    const dataEndAndSummarySection = await readable.read(
      dataEndOffset,
      footerOffset - dataEndOffset,
    );
    if (footer.summaryCrc !== 0) {
      let summaryCrc = crc32Init();
      summaryCrc = crc32Update(
        summaryCrc,
        dataEndAndSummarySection.subarray(Number(dataEndLength)),
      );
      summaryCrc = crc32Update(summaryCrc, footerPrefix);
      summaryCrc = crc32Final(summaryCrc);
      if (summaryCrc !== footer.summaryCrc) {
        throw errorWithLibrary(
          `Incorrect summary CRC ${summaryCrc} (expected ${footer.summaryCrc})`,
        );
      }
    }

    const indexView = new DataView(
      dataEndAndSummarySection.buffer,
      dataEndAndSummarySection.byteOffset,
      dataEndAndSummarySection.byteLength,
    );

    const channelsById = new Map<number, TypedMcapRecords["Channel"]>();
    const schemasById = new Map<number, TypedMcapRecords["Schema"]>();
    const chunkIndexes: TypedMcapRecords["ChunkIndex"][] = [];
    const metadataIndexes: TypedMcapRecords["MetadataIndex"][] = [];
    const summaryOffsetsByOpcode = new Map<number, TypedMcapRecords["SummaryOffset"]>();
    let statistics: TypedMcapRecords["Statistics"] | undefined;
    let dataSectionCrc: number | undefined;

    let offset = 0;
    for (
      let result;
      (result = parseRecord({ view: indexView, startOffset: offset, validateCrcs: true })),
        result.record;
      offset += result.usedBytes
    ) {
      if (offset === 0 && result.record.type !== "DataEnd") {
        throw errorWithLibrary(
          `Expected DataEnd record to precede summary section, but found ${result.record.type}`,
        );
      }
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
        case "MetadataIndex":
          metadataIndexes.push(result.record);
          break;
        case "Statistics":
          if (statistics) {
            throw errorWithLibrary("Duplicate Statistics record");
          }
          statistics = result.record;
          break;
        case "SummaryOffset":
          summaryOffsetsByOpcode.set(result.record.groupOpcode, result.record);
          break;
        case "DataEnd":
          dataSectionCrc =
            result.record.dataSectionCrc === 0 ? undefined : result.record.dataSectionCrc;
          break;
        case "Header":
        case "Footer":
        case "Message":
        case "Chunk":
        case "MessageIndex":
        case "Attachment":
        case "AttachmentIndex":
        case "Metadata":
          throw errorWithLibrary(`${result.record.type} record not allowed in index section`);
        case "Unknown":
          break;
      }
    }
    if (offset !== indexView.byteLength) {
      throw errorWithLibrary(`${indexView.byteLength - offset} bytes remaining in index section`);
    }

    return new FastIndexedReader({
      readable,
      chunkIndexes,
      metadataIndexes,
      statistics,
      decompressHandlers,
      channelsById,
      schemasById,
      summaryOffsetsByOpcode,
      header,
      footer,
      dataEndOffset,
      dataSectionCrc,
    });
  }

  async *readMessages(
    args: {
      topics?: readonly string[];
      startTime?: bigint;
      endTime?: bigint;
      reverse?: boolean;
      validateCrcs?: boolean;
    } = {},
  ): AsyncGenerator<TypedMcapRecords["Message"], void, void> {
    const it = this.makeMessageIterator(args);

    for (;;) {
      const res = it.next();
      switch (res.type) {
        case "end":
          return;
        case "wait":
          await res.promise;
          break;
        case "message":
          yield res.message;
      }
    }
  }
  makeMessageIterator(
    args: {
      topics?: readonly string[];
      startTime?: bigint;
      endTime?: bigint;
      reverse?: boolean;
      validateCrcs?: boolean;
    } = {},
  ): MessageIterator {
    return new MessageIterator({
      readable: this.#readable,
      chunkIndexes: this.chunkIndexes,
      library: this.header.library,
      channels: this.channelsById,
      topics: args.topics,
      decompressHandlers: this.#decompressHandlers,
      startTime: this.#messageStartTime,
      endTime: this.#messageEndTime,
      reverse: args.reverse,
      validateCrcs: args.validateCrcs,
    });
  }
}

function reverseTail<T>(arr: T[], start: number) {
  const sliceLength = arr.length - start;
  for (let i = 0; i < sliceLength / 2; i++) {
    const j = arr.length - i;
    const tmp = arr[i]!;
    arr[i] = arr[j]!;
    arr[j] = tmp;
  }
}

function sortTail<T>(arr: T[], start: number, cmp: (a: T, b: T) => number) {
  const slice = arr.slice(start);
  slice.sort(cmp);
  let i = 0;
  for (const elem of slice) {
    arr[i + start] = elem;
    i++;
  }
}

type MessageIteratorResult =
  | {
      type: "wait";
      promise: Promise<void>;
    }
  | {
      type: "message";
      message: TypedMcapRecords["Message"];
    }
  | {
      type: "end";
    };

export class MessageIterator {
  #chunkIndexes: readonly ChunkIndex[];
  #chunkIndexCursor = 0;
  #messageIndexCursor = 0;
  #messageIndexes: MessageIndex[] = [];
  #chunkSlots: ChunkSlot[] = [];
  #relevantChannels?: Set<number>;
  #readable: IReadable;
  #reverse: boolean;
  #library: string;
  #startTime?: bigint;
  #endTime?: bigint;
  #validateCrcs: boolean;
  #decompressHandlers?: DecompressHandlers;

  constructor(args: {
    readable: IReadable;
    chunkIndexes: readonly ChunkIndex[];
    channels: ReadonlyMap<number, TypedMcapRecords["Channel"]>;
    library: string;
    topics?: readonly string[];
    startTime?: bigint;
    endTime?: bigint;
    reverse?: boolean;
    validateCrcs?: boolean;
    decompressHandlers?: DecompressHandlers;
  }) {
    this.#readable = args.readable;
    this.#chunkIndexCursor = 0;
    this.#messageIndexCursor = 0;
    this.#library = args.library;
    this.#reverse = args.reverse ?? false;
    this.#startTime = args.startTime;
    this.#endTime = args.endTime;
    this.#validateCrcs = args.validateCrcs ?? true;
    this.#decompressHandlers = args.decompressHandlers;

    const chunkIndexes: ChunkIndex[] = [];
    if (this.#startTime != undefined && this.#endTime != undefined) {
      for (const chunkIndex of args.chunkIndexes) {
        if (
          chunkIndex.messageStartTime <= this.#endTime &&
          chunkIndex.messageEndTime >= this.#startTime
        ) {
          chunkIndexes.push(chunkIndex);
        }
      }
      if (!this.#reverse) {
        chunkIndexes.sort((a, b) => Number(a.messageStartTime - b.messageStartTime));
      } else {
        chunkIndexes.sort((a, b) => Number(b.messageEndTime - a.messageEndTime));
      }
    }
    this.#chunkIndexes = chunkIndexes;
    if (args.topics != undefined) {
      this.#relevantChannels = new Set<number>();
      for (const channel of args.channels.values()) {
        if (channel.topic in args.topics) {
          this.#relevantChannels.add(channel.id);
        }
      }
    }
  }

  next(): MessageIteratorResult {
    if (this.#startTime == undefined || this.#endTime == undefined) {
      return { type: "end" };
    }
    if (this.#messageIndexCursor >= this.#messageIndexes.length) {
      if (this.#chunkIndexCursor >= this.#chunkIndexes.length) {
        return { type: "end" };
      }
      const chunkIndex = this.#chunkIndexes[this.#chunkIndexCursor]!;
      this.#chunkIndexCursor++;
      return { type: "wait", promise: this.#loadChunkData(chunkIndex) };
    }
    if (this.#chunkIndexCursor < this.#chunkIndexes.length) {
      const chunkIndex = this.#chunkIndexes[this.#chunkIndexCursor]!;
      const messageIndex = this.#messageIndexes[this.#messageIndexCursor]!;
      if (
        (!this.#reverse && chunkIndex.messageStartTime < messageIndex.timestamp) ||
        (this.#reverse && chunkIndex.messageEndTime > messageIndex.timestamp)
      ) {
        this.#chunkIndexCursor++;
        return { type: "wait", promise: this.#loadChunkData(chunkIndex) };
      }
    }
    if (this.#messageIndexes.length - this.#messageIndexCursor < this.#messageIndexCursor) {
      this.#messageIndexes.splice(0, this.#messageIndexCursor);
      this.#messageIndexCursor = 0;
    }
    const messageIndex = this.#messageIndexes[this.#messageIndexCursor]!;
    const chunkSlot = this.#chunkSlots[messageIndex.chunkSlotIndex]!;
    const res = parseRecord({
      view: chunkSlot.buf,
      startOffset: messageIndex.offset,
      validateCrcs: false,
    });
    assert(res.record?.type === "Message", "failed to index message");
    this.#messageIndexCursor++;
    return { type: "message", message: res.record };
  }
  async #loadChunkData(chunkIndex: ChunkIndex): Promise<void> {
    if (this.#startTime == undefined || this.#endTime == undefined) {
      return;
    }
    const chunkData = await this.#readable.read(
      chunkIndex.chunkStartOffset,
      chunkIndex.chunkLength,
    );
    const chunkResult = parseRecord({
      view: new DataView(chunkData.buffer, chunkData.byteOffset, chunkData.byteLength),
      startOffset: 0,
      validateCrcs: this.#validateCrcs,
    });
    if (chunkResult.record?.type !== "Chunk") {
      throw this.#errorWithLibrary(
        `Chunk start offset ${
          chunkIndex.chunkStartOffset
        } does not point to chunk record (found ${String(chunkResult.record?.type)})`,
      );
    }

    const chunk = chunkResult.record;
    let buffer = chunk.records;
    if (chunk.compression !== "" && buffer.byteLength > 0) {
      const decompress = this.#decompressHandlers?.[chunk.compression];
      if (!decompress) {
        throw this.#errorWithLibrary(`Unsupported compression ${chunk.compression}`);
      }
      buffer = decompress(buffer, chunk.uncompressedSize);
    }
    if (chunk.uncompressedCrc !== 0 && this.#validateCrcs) {
      const chunkCrc = crc32(buffer);
      if (chunkCrc !== chunk.uncompressedCrc) {
        throw this.#errorWithLibrary(
          `Incorrect chunk CRC ${chunkCrc} (expected ${chunk.uncompressedCrc})`,
        );
      }
    }
    let chunkSlotIndex: number | undefined = undefined;
    for (let i = 0; i < this.#chunkSlots.length; i++) {
      if (this.#chunkSlots[i]!.unreadMessages === 0) {
        chunkSlotIndex = i;
        break;
      }
    }
    if (chunkSlotIndex == undefined) {
      this.#chunkSlots.push({
        buf: new DataView(buffer.buffer, buffer.byteOffset, buffer.byteLength),
        unreadMessages: 0,
      });
      chunkSlotIndex = this.#chunkSlots.length - 1;
    }
    const chunkSlot = this.#chunkSlots[chunkSlotIndex]!;
    let sortingRequired = this.#messageIndexCursor !== 0;
    const startIdx = this.#messageIndexes.length;
    let maxLogTime = BigInt(0);
    for (let offset = 0; offset < buffer.byteLength; ) {
      if (buffer.byteLength < offset + 9) {
        throw this.#errorWithLibrary(
          `expected another record in chunk, but left with ${buffer.byteLength} bytes`,
        );
      }
      const opcode = chunkSlot.buf.getUint8(offset + 0);
      const length = chunkSlot.buf.getBigUint64(offset + 1, true);
      if (!isKnownOpcode(opcode)) {
        throw this.#errorWithLibrary(`expected known opcode, got ${opcode} at ${offset}`);
      }
      if (isKnownOpcode(opcode) && opcode === Opcode.MESSAGE) {
        const channelId = chunkSlot.buf.getUint16(offset + 9);
        const logTime = chunkSlot.buf.getBigUint64(offset + 9 + 6, true);
        if (this.#relevantChannels != undefined && !this.#relevantChannels.has(channelId)) {
          offset = offset + 9 + Number(length);
          continue;
        }
        if (logTime < this.#startTime || logTime > this.#endTime) {
          offset = offset + 9 + Number(length);
          continue;
        }
        this.#messageIndexes.push({ offset, timestamp: logTime, chunkSlotIndex });
        if (logTime < maxLogTime) {
          sortingRequired = true;
        } else {
          maxLogTime = logTime;
        }
        chunkSlot.unreadMessages++;
      }
      offset = offset + 9 + Number(length);
    }
    if (!this.#reverse) {
      if (sortingRequired) {
        sortTail(this.#messageIndexes, this.#messageIndexCursor, (a, b) =>
          Number(a.timestamp - b.timestamp),
        );
      }
    } else {
      reverseTail(this.#messageIndexes, startIdx);
      if (sortingRequired) {
        sortTail(this.#messageIndexes, this.#messageIndexCursor, (a, b) =>
          Number(a.timestamp - b.timestamp),
        );
      }
    }
  }
  #errorWithLibrary(message: string): Error {
    return new Error(`${message} [library=${this.#library}]`);
  }
}
