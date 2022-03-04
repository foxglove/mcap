import { crc32 } from "@foxglove/crc";
import { isEqual } from "lodash";

import StreamBuffer from "../common/StreamBuffer";
import { MCAP0_MAGIC } from "./constants";
import { parseMagic, parseRecord } from "./parse";
import { DecompressHandlers, McapStreamReader, TypedMcapRecord, TypedMcapRecords } from "./types";

type McapReaderOptions = {
  /**
   * When set to true, Chunk records will be returned from `nextRecord()`. Chunk contents will still
   * be processed after each chunk record itself.
   */
  includeChunks?: boolean;

  /**
   * When a compressed chunk is encountered, the entry in `decompressHandlers` corresponding to the
   * compression will be called to decompress the chunk data.
   */
  decompressHandlers?: DecompressHandlers;

  /**
   * When set to true (the default), chunk CRCs will be validated. Set to false to improve performance.
   */
  validateCrcs?: boolean;
};

/**
 * A streaming reader for MCAP files.
 *
 * Usage example:
 * ```
 * const reader = new Mcap0StreamReader();
 * stream.on("data", (data) => {
 *   try {
 *     reader.append(data);
 *     for (let record; (record = reader.nextRecord()); ) {
 *       // process available records
 *     }
 *   } catch (e) {
 *     // handle errors
 *   }
 * });
 * ```
 */
export default class Mcap0StreamReader implements McapStreamReader {
  private buffer = new StreamBuffer(MCAP0_MAGIC.length * 2);
  private decompressHandlers;
  private includeChunks;
  private validateCrcs;
  private doneReading = false;
  private generator = this.read();
  private channelsById = new Map<number, TypedMcapRecords["Channel"]>();

  constructor({
    includeChunks = false,
    decompressHandlers = {},
    validateCrcs = true,
  }: McapReaderOptions = {}) {
    this.includeChunks = includeChunks;
    this.decompressHandlers = decompressHandlers;
    this.validateCrcs = validateCrcs;
  }

  /** @returns True if a valid, complete mcap file has been parsed. */
  done(): boolean {
    return this.doneReading;
  }

  /** @returns The number of bytes that have been received by `append()` but not yet parsed. */
  bytesRemaining(): number {
    return this.buffer.bytesRemaining();
  }

  /**
   * Provide the reader with newly received bytes for it to process. After calling this function,
   * call `nextRecord()` again to parse any records that are now available.
   */
  append(data: Uint8Array): void {
    if (this.doneReading) {
      throw new Error("Already done reading");
    }
    this.buffer.append(data);
  }

  /**
   * Read the next record from the stream if possible. If not enough data is available to parse a
   * complete record, or if the reading has terminated with a valid footer, returns undefined.
   *
   * This function may throw any errors encountered during parsing. If an error is thrown, the
   * reader is in an unspecified state and should no longer be used.
   */
  nextRecord(): TypedMcapRecord | undefined {
    if (this.doneReading) {
      return undefined;
    }
    const result = this.generator.next();

    if (result.value?.type === "Channel") {
      const existing = this.channelsById.get(result.value.id);
      this.channelsById.set(result.value.id, result.value);
      if (existing && !isEqual(existing, result.value)) {
        throw new Error(
          `Channel record for id ${result.value.id} (topic: ${result.value.topic}) differs from previous for the same id.`,
        );
      }
    } else if (result.value?.type === "Message") {
      const channelId = result.value.channelId;
      const existing = this.channelsById.get(channelId);
      if (!existing) {
        throw new Error(`Encountered message on channel ${channelId} without prior channel record`);
      }
    }

    if (result.done === true) {
      this.doneReading = true;
    }
    return result.value;
  }

  private *read(): Generator<TypedMcapRecord | undefined, TypedMcapRecord | undefined, void> {
    {
      let magic, usedBytes;
      while ((({ magic, usedBytes } = parseMagic(this.buffer.view, 0)), !magic)) {
        yield;
      }
      this.buffer.consume(usedBytes);
    }

    for (;;) {
      let record;
      {
        let usedBytes;
        while (
          (({ record, usedBytes } = parseRecord({
            view: this.buffer.view,
            startOffset: 0,
            validateCrcs: this.validateCrcs,
          })),
          !record)
        ) {
          yield;
        }
        this.buffer.consume(usedBytes);
      }
      switch (record.type) {
        case "Unknown":
          break;
        case "Header":
        case "Schema":
        case "Channel":
        case "Message":
        case "MessageIndex":
        case "ChunkIndex":
        case "Attachment":
        case "AttachmentIndex":
        case "Statistics":
        case "Metadata":
        case "MetadataIndex":
        case "SummaryOffset":
        case "DataEnd":
          yield record;
          break;

        case "Chunk": {
          if (this.includeChunks) {
            yield record;
          }
          let buffer = record.records;
          if (record.compression !== "" && buffer.byteLength > 0) {
            const decompress = this.decompressHandlers[record.compression];
            if (!decompress) {
              throw new Error(`Unsupported compression ${record.compression}`);
            }
            buffer = decompress(buffer, record.uncompressedSize);
          }
          if (this.validateCrcs && record.uncompressedCrc !== 0) {
            const chunkCrc = crc32(buffer);
            if (chunkCrc !== record.uncompressedCrc) {
              throw new Error(
                `Incorrect chunk CRC ${chunkCrc} (expected ${record.uncompressedCrc})`,
              );
            }
          }
          const view = new DataView(buffer.buffer, buffer.byteOffset, buffer.byteLength);
          let chunkOffset = 0;
          for (
            let chunkResult;
            (chunkResult = parseRecord({
              view,
              startOffset: chunkOffset,
              validateCrcs: this.validateCrcs,
            })),
              chunkResult.record;
            chunkOffset += chunkResult.usedBytes
          ) {
            switch (chunkResult.record.type) {
              case "Unknown":
                break;
              case "Header":
              case "Footer":
              case "Chunk":
              case "MessageIndex":
              case "ChunkIndex":
              case "Attachment":
              case "AttachmentIndex":
              case "Statistics":
              case "Metadata":
              case "MetadataIndex":
              case "SummaryOffset":
              case "DataEnd":
                throw new Error(`${chunkResult.record.type} record not allowed inside a chunk`);
              case "Schema":
              case "Channel":
              case "Message":
                yield chunkResult.record;
                break;
            }
          }
          if (chunkOffset !== buffer.byteLength) {
            throw new Error(`${buffer.byteLength - chunkOffset} bytes remaining in chunk`);
          }
          break;
        }
        case "Footer":
          {
            let magic, usedBytes;
            while ((({ magic, usedBytes } = parseMagic(this.buffer.view, 0)), !magic)) {
              yield;
            }
            this.buffer.consume(usedBytes);
          }
          if (this.buffer.bytesRemaining() !== 0) {
            throw new Error(
              `${this.buffer.bytesRemaining()} bytes remaining after MCAP footer and trailing magic`,
            );
          }
          return record;
      }
    }
  }
}
