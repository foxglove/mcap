import { crc32, crc32Final, crc32Init, crc32Update } from "@foxglove/crc";
import Heap from "heap-js";

import { getBigUint64 } from "../common/getBigUint64";
import { IReadable } from "./IReadable";
import { MCAP0_MAGIC, Opcode } from "./constants";
import { parseMagic, parseRecord } from "./parse";
import { TypedMcapRecords } from "./types";

type DecompressHandlers = {
  [compression: string]: (buffer: Uint8Array, decompressedSize: bigint) => Uint8Array;
};

export default class Mcap0IndexedReader {
  private readable: IReadable;
  // private size: bigint;//FIXME
  private chunkIndexes: readonly TypedMcapRecords["ChunkIndex"][];
  private decompressHandlers?: DecompressHandlers;
  private channelInfosById: ReadonlyMap<number, TypedMcapRecords["ChannelInfo"]>;

  private startTime: bigint | undefined;
  private endTime: bigint | undefined;

  private constructor({
    readable,
    // size,
    chunkIndexes,
    decompressHandlers,
    channelInfosById,
  }: {
    readable: IReadable;
    // size: bigint;
    chunkIndexes: readonly TypedMcapRecords["ChunkIndex"][];
    decompressHandlers?: DecompressHandlers;
    channelInfosById: ReadonlyMap<number, TypedMcapRecords["ChannelInfo"]>;
  }) {
    this.readable = readable;
    // this.size = size;
    this.chunkIndexes = chunkIndexes;
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
    let footerView: DataView;
    {
      const headerLengthLowerBound = BigInt(
        MCAP0_MAGIC.length +
          /* Opcode.HEADER */ 1 +
          /* record length */ 8 +
          /* profile length */ 4 +
          /* library length */ 4,
      );
      const footerReadLength = BigInt(
        /* Opcode.FOOTER */ 1 +
          /* record length */ 8 +
          /* indexOffset */ 8 +
          /* indexCrc */ 4 +
          MCAP0_MAGIC.length,
      );
      if (size < headerLengthLowerBound + footerReadLength) {
        throw new Error(`File size (${size}) is too small to be valid MCAP`);
      }
      footerOffset = size - footerReadLength;
      const footerBuffer = await readable.read(footerOffset, footerReadLength);
      footerView = new DataView(
        footerBuffer.buffer,
        footerBuffer.byteOffset,
        footerBuffer.byteLength,
      );
    }

    void parseMagic(footerView, footerView.byteLength - MCAP0_MAGIC.length);

    const channelInfosById = new Map<number, TypedMcapRecords["ChannelInfo"]>();
    const channelInfosSeenInThisChunk = new Set<number>();

    const footer = parseRecord({
      view: footerView,
      startOffset: 0,
      channelInfosById: new Map(),
      channelInfosSeenInThisChunk: new Set(),
      validateCrcs: true,
    }).record;
    if (footer?.type !== "Footer") {
      throw new Error(
        `Unable to read footer from end of file (offset ${footerOffset}); found ${
          footer?.type ?? "nothing"
        }`,
      );
    }
    if (footer.indexOffset === 0n) {
      throw new Error("File is not indexed");
    }

    //FIXME: avoid holding whole index blob in memory at once?
    const indexData = await readable.read(footer.indexOffset, footerOffset - footer.indexOffset);
    if (footer.indexCrc !== 0) {
      let indexCrc = crc32Init();
      indexCrc = crc32Update(indexCrc, indexData);
      indexCrc = crc32Update(
        indexCrc,
        new DataView(
          footerView.buffer,
          footerView.byteOffset,
          /* Opcode.FOOTER */ 1 + /* record length */ 8 + /* indexOffset */ 8,
        ),
      );
      indexCrc = crc32Final(indexCrc);
      if (indexCrc !== footer.indexCrc) {
        throw new Error(`Incorrect index CRC ${indexCrc} (expected ${footer.indexCrc})`);
      }
    }

    const indexView = new DataView(indexData.buffer, indexData.byteOffset, indexData.byteLength);

    const chunkIndexes: TypedMcapRecords["ChunkIndex"][] = [];
    const attachmentIndexes: TypedMcapRecords["AttachmentIndex"][] = [];
    let statistics: TypedMcapRecords["Statistics"] | undefined;

    let offset = 0;
    for (
      let result;
      (result = parseRecord({
        view: indexView,
        startOffset: offset,
        channelInfosById,
        channelInfosSeenInThisChunk,
        validateCrcs: true,
      })),
        result.record;
      offset += result.usedBytes
    ) {
      switch (result.record.type) {
        case "ChannelInfo":
          // FIXME: done in parseRecord - is that ok?
          // if (channelInfosById.has(result.record.channelId)) {
          //   throw new Error(`Duplicate ChannelInfo for channel id ${result.record.channelId}`);
          // }
          // channelInfosById.set(result.record.channelId, result.record);
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
      // size,
      chunkIndexes,
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
        if (topics.includes(channelInfo.topicName)) {
          relevantChannels.add(channelInfo.channelId);
        }
      }
    }

    const relevantChunks = this.chunkIndexes.filter(
      (chunk) => chunk.startTime <= endTime && chunk.endTime >= startTime,
    );

    // FIXME: merge overlapping chunks
    for (const chunkIndex of relevantChunks) {
      yield* this.readChunk({ chunkIndex, channelIds: relevantChannels, startTime, endTime });
    }
  }

  async *readChunk({
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
    const chunkOpcodeAndLength = await this.readable.read(chunkIndex.chunkOffset, 1n + 8n);
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

    // FIXME: read only message indexes for given channelIds? more requests = more latency
    const chunkAndMessageIndexes = await this.readable.read(
      chunkIndex.chunkOffset,
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
      records: TypedMcapRecords["MessageIndex"]["records"];
    }>((a, b) => {
      const recordTimeA = a.records[a.index]?.[0];
      const recordTimeB = b.records[b.index]?.[0];
      if (recordTimeA == undefined) {
        return 1;
      } else if (recordTimeB == undefined) {
        return -1;
      }
      return Number(recordTimeA - recordTimeB);
    });

    // FIXME: make these optional params?
    const channelInfosById = new Map<number, TypedMcapRecords["ChannelInfo"]>();
    const channelInfosSeenInThisChunk = new Set<number>();

    {
      let offset = 0;
      const chunkResult = parseRecord({
        view: chunkAndMessageIndexesView,
        startOffset: offset,
        channelInfosById,
        channelInfosSeenInThisChunk,
        validateCrcs: true,
      });
      offset += chunkResult.usedBytes;
      if (chunkResult.record?.type !== "Chunk") {
        throw new Error(
          `Chunk index offset does not point to chunk record (found ${chunkResult.record?.type})`,
        );
      }
      chunk = chunkResult.record;

      for (
        let result;
        (result = parseRecord({
          view: chunkAndMessageIndexesView,
          startOffset: offset,
          channelInfosById,
          channelInfosSeenInThisChunk,
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
          messageIndexCursors.push({ index: 0, records: result.record.records });
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
      const [recordTime, offset] = cursor.records[cursor.index]!;
      if (recordTime >= startTime && recordTime < endTime) {
        const result = parseRecord({
          view: recordsView,
          startOffset: Number(offset), // FIXME: conversion should not happen here
          channelInfosById,
          channelInfosSeenInThisChunk,
          validateCrcs: true,
        });
        if (!result.record) {
          throw new Error(
            `Unable to parse record at offset ${offset} in chunk at offset ${chunkIndex.chunkOffset}`,
          );
        }
        if (result.record.type !== "Message") {
          throw new Error(
            `Unexpected record type ${result.record.type} in message index (time ${recordTime}, offset ${offset} in chunk at offset ${chunkIndex.chunkOffset})`,
          );
        }
        if (result.record.recordTime !== recordTime) {
          throw new Error(
            `Message recordTime ${result.record.recordTime} did not match message index entry (${recordTime} at offset ${offset} in chunk at offset ${chunkIndex.chunkOffset})`,
          );
        }
        yield result.record;
      }

      if (cursor.index + 1 < cursor.records.length && recordTime < endTime) {
        cursor.index++;
        messageIndexCursors.replace(cursor);
      } else {
        messageIndexCursors.pop();
      }
    }
  }
}
