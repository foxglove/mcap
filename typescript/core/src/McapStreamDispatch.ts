import { crc32 } from "@foxglove/crc";

import Reader from "./Reader";
import { MCAP_MAGIC } from "./constants";
import { monoParseMessage, parseMagic, parseRecord } from "./parse";
import { Channel, DecompressHandlers, McapMagic, TypedMcapRecord, TypedMcapRecords } from "./types";

type McapDispatchOptions = {
  includeChunks?: boolean;
  decompressHandlers?: DecompressHandlers;
  validateCrcs?: boolean;
  noMagicPrefix?: boolean;
};

type McapDispatchHandlers = {
  onHeader?: (record: TypedMcapRecords["Header"]) => void;
  onSchema?: (record: TypedMcapRecords["Schema"]) => void;
  onChannel?: (record: TypedMcapRecords["Channel"]) => void;
  onMessage?: (record: TypedMcapRecords["Message"]) => void;
  onChunk?: (record: TypedMcapRecords["Chunk"]) => void;
  onMessageIndex?: (record: TypedMcapRecords["MessageIndex"]) => void;
  onChunkIndex?: (record: TypedMcapRecords["ChunkIndex"]) => void;
  onAttachment?: (record: TypedMcapRecords["Attachment"]) => void;
  onAttachmentIndex?: (record: TypedMcapRecords["AttachmentIndex"]) => void;
  onStatistics?: (record: TypedMcapRecords["Statistics"]) => void;
  onMetadata?: (record: TypedMcapRecords["Metadata"]) => void;
  onMetadataIndex?: (record: TypedMcapRecords["MetadataIndex"]) => void;
  onSummaryOffset?: (record: TypedMcapRecords["SummaryOffset"]) => void;
  onDataEnd?: (record: TypedMcapRecords["DataEnd"]) => void;
  onFooter?: (record: TypedMcapRecords["Footer"]) => void;
  onError?: (error: Error) => void;
};

export default class McapStreamDispatch {
  #buffer = new ArrayBuffer(MCAP_MAGIC.length * 2);
  #view = new DataView(this.#buffer);
  #reader = new Reader(this.#view, MCAP_MAGIC.length * 2);
  #decompressHandlers: DecompressHandlers;
  #includeChunks: boolean;
  #validateCrcs: boolean;
  #noMagicPrefix: boolean;
  #doneReading = false;
  #channelsById = new Map<number, TypedMcapRecords["Channel"]>();
  #handlers: McapDispatchHandlers;
  #header: TypedMcapRecords["Header"] | undefined;

  constructor(handlers: McapDispatchHandlers, options: McapDispatchOptions = {}) {
    this.#handlers = handlers;
    this.#includeChunks = options.includeChunks ?? false;
    this.#decompressHandlers = options.decompressHandlers ?? {};
    this.#validateCrcs = options.validateCrcs ?? true;
    this.#noMagicPrefix = options.noMagicPrefix ?? false;
  }

  append(data: Uint8Array): void {
    if (this.#doneReading) {
      throw new Error("Already done reading");
    }
    this.#appendOrShift(data);
    this.#reader.reset(this.#view);
    this.#processRecords();
  }

  #appendOrShift(data: Uint8Array): void {
    const remainingBytes = this.#reader.bytesRemaining();
    const totalNeededBytes = remainingBytes + data.byteLength;

    if (totalNeededBytes <= this.#buffer.byteLength) {
      if (this.#view.byteOffset + totalNeededBytes <= this.#buffer.byteLength) {
        const array = new Uint8Array(this.#buffer, this.#view.byteOffset);
        array.set(data, remainingBytes);
        this.#view = new DataView(this.#buffer, this.#view.byteOffset, totalNeededBytes);
      } else {
        const existingData = new Uint8Array(this.#buffer, this.#view.byteOffset, remainingBytes);
        const array = new Uint8Array(this.#buffer);
        array.set(existingData, 0);
        array.set(data, existingData.byteLength);
        this.#view = new DataView(this.#buffer, 0, totalNeededBytes);
      }
    } else {
      this.#buffer = new ArrayBuffer(totalNeededBytes * 2);
      const array = new Uint8Array(this.#buffer);
      const existingData = new Uint8Array(this.#view.buffer, this.#view.byteOffset, remainingBytes);
      array.set(existingData, 0);
      array.set(data, existingData.byteLength);
      this.#view = new DataView(this.#buffer, 0, totalNeededBytes);
    }
  }

  #processRecords(): void {
    if (!this.#noMagicPrefix) {
      let magic: McapMagic | undefined;
      while ((magic = parseMagic(this.#reader)) === undefined) {
        if (this.#reader.bytesRemaining() === 0) return;
      }
    }

    while (!this.#doneReading) {
      const record = parseRecord(this.#reader, this.#validateCrcs);
      if (!record) break;

      this.#handleRecord(record);
    }
  }

  #handleRecord(record: TypedMcapRecord): void {
    switch (record.type) {
      case "Header":
        if (this.#header) {
          this.#handleError(new Error(`Duplicate Header record`));
          return;
        }
        this.#header = record;
        this.#handlers.onHeader?.(record);
        break;
      case "Footer":
        this.#handlers.onFooter?.(record);
        this.#doneReading = true;
        break;
      case "Schema":
        this.#handlers.onSchema?.(record);
        break;
      case "Channel":
        this.#handleChannel(record);
        break;
      case "Message":
        this.#handleMessage(record);
        break;
      case "Chunk":
        this.#handleChunk(record);
        break;
      case "MessageIndex":
        this.#handlers.onMessageIndex?.(record);
        break;
      case "ChunkIndex":
        this.#handlers.onChunkIndex?.(record);
        break;
      case "Attachment":
        this.#handlers.onAttachment?.(record);
        break;
      case "AttachmentIndex":
        this.#handlers.onAttachmentIndex?.(record);
        break;
      case "Statistics":
        this.#handlers.onStatistics?.(record);
        break;
      case "Metadata":
        this.#handlers.onMetadata?.(record);
        break;
      case "MetadataIndex":
        this.#handlers.onMetadataIndex?.(record);
        break;
      case "SummaryOffset":
        this.#handlers.onSummaryOffset?.(record);
        break;
      case "DataEnd":
        this.#handlers.onDataEnd?.(record);
        break;
    }
  }

  #handleChannel(record: TypedMcapRecords["Channel"]): void {
    const existing = this.#channelsById.get(record.id);
    this.#channelsById.set(record.id, record);
    if (existing && !this.#isChannelEqual(existing, record)) {
      this.#handleError(
        new Error(`Channel record for id ${record.id} (topic: ${record.topic}) differs from previous channel record of the same id.`)
      );
      return;
    }
    this.#handlers.onChannel?.(record);
  }

  #handleMessage(record: TypedMcapRecords["Message"]): void {
    const channelId = record.channelId;
    const existing = this.#channelsById.get(channelId);
    if (!existing) {
      this.#handleError(new Error(`Encountered message on channel ${channelId} without prior channel record`));
      return;
    }
    this.#handlers.onMessage?.(record);
  }

  #handleChunk(record: TypedMcapRecords["Chunk"]): void {
    if (this.#includeChunks) {
      this.#handlers.onChunk?.(record);
    }

    let buffer = record.records;
    if (record.compression !== "" && buffer.byteLength > 0) {
      const decompress = this.#decompressHandlers[record.compression];
      if (!decompress) {
        this.#handleError(new Error(`Unsupported compression ${record.compression}`));
        return;
      }
      buffer = decompress(buffer, record.uncompressedSize);
    }

    if (this.#validateCrcs && record.uncompressedCrc !== 0) {
      const chunkCrc = crc32(buffer);
      if (chunkCrc !== record.uncompressedCrc) {
        this.#handleError(new Error(`Incorrect chunk CRC ${chunkCrc} (expected ${record.uncompressedCrc})`));
        return;
      }
    }

    const view = new DataView(buffer.buffer, buffer.byteOffset, buffer.byteLength);
    const chunkReader = new Reader(view);

    while (chunkReader.bytesRemaining() > 0) {
      const chunkRecord = monoParseMessage(chunkReader) ?? parseRecord(chunkReader, this.#validateCrcs);
      if (!chunkRecord) break;

      if (chunkRecord.type === "Schema" || chunkRecord.type === "Channel" || chunkRecord.type === "Message") {
        this.#handleRecord(chunkRecord);
      } else if (chunkRecord.type !== "Unknown") {
        this.#handleError(new Error(`${chunkRecord.type} record not allowed inside a chunk`));
        return;
      }
    }

    if (chunkReader.bytesRemaining() !== 0) {
      this.#handleError(new Error(`${chunkReader.bytesRemaining()} bytes remaining in chunk`));
    }
  }

  #isChannelEqual(a: Channel, b: Channel): boolean {
    if (
      !(
        a.id === b.id &&
        a.messageEncoding === b.messageEncoding &&
        a.schemaId === b.schemaId &&
        a.topic === b.topic &&
        a.metadata.size === b.metadata.size
      )
    ) {
      return false;
    }
    for (const [keyA, valueA] of a.metadata.entries()) {
      const valueB = b.metadata.get(keyA);
      if (valueA !== valueB) {
        return false;
      }
    }
    return true;
  }

  #handleError(error: Error): void {
    this.#handlers.onError?.(error);
  }
}
