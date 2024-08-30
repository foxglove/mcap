import { crc32 } from "@foxglove/crc";

import StreamBuffer from "./StreamBuffer";
import { MCAP_MAGIC } from "./constants";
import { parseMagic, parseRecord } from "./parse";
import { Channel, DecompressHandlers, McapMagic, TypedMcapRecord, TypedMcapRecords } from "./types";

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

  /**
   * When set to true, the reader will not expect a magic prefix at the beginning of the stream.
   * This is useful when reading a stream that contains a fragment of an MCAP file, or when
   * reading starts in the middle of an MCAP file.
   */
  noMagicPrefix?: boolean;
};

/**
 * A streaming reader for MCAP files.
 *
 * Usage example:
 * ```
 * const reader = new McapStreamReader();
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
export default class McapStreamReader {
  #buffer = new StreamBuffer(MCAP_MAGIC.length * 2);
  #decompressHandlers;
  #includeChunks;
  #validateCrcs;
  #noMagicPrefix;
  #doneReading = false;
  #generator = this.#read();
  #channelsById = new Map<number, TypedMcapRecords["Channel"]>();

  constructor({
    includeChunks = false,
    decompressHandlers = {},
    validateCrcs = true,
    noMagicPrefix = false,
  }: McapReaderOptions = {}) {
    this.#includeChunks = includeChunks;
    this.#decompressHandlers = decompressHandlers;
    this.#validateCrcs = validateCrcs;
    this.#noMagicPrefix = noMagicPrefix;
  }

  /** @returns True if a valid, complete mcap file has been parsed. */
  done(): boolean {
    return this.#doneReading;
  }

  /** @returns The number of bytes that have been received by `append()` but not yet parsed. */
  bytesRemaining(): number {
    return this.#buffer.bytesRemaining();
  }

  /**
   * Provide the reader with newly received bytes for it to process. After calling this function,
   * call `nextRecord()` again to parse any records that are now available.
   */
  append(data: Uint8Array): void {
    if (this.#doneReading) {
      throw new Error("Already done reading");
    }
    this.#buffer.append(data);
  }

  /**
   * Read the next record from the stream if possible. If not enough data is available to parse a
   * complete record, or if the reading has terminated with a valid footer, returns undefined.
   *
   * This function may throw any errors encountered during parsing. If an error is thrown, the
   * reader is in an unspecified state and should no longer be used.
   */
  nextRecord(): TypedMcapRecord | undefined {
    if (this.#doneReading) {
      return undefined;
    }
    const result = this.#generator.next();

    if (result.value?.type === "Channel") {
      const existing = this.#channelsById.get(result.value.id);
      this.#channelsById.set(result.value.id, result.value);
      if (existing && !isChannelEqual(existing, result.value)) {
        throw new Error(
          `Channel record for id ${result.value.id} (topic: ${result.value.topic}) differs from previous channel record of the same id.`,
        );
      }
    } else if (result.value?.type === "Message") {
      const channelId = result.value.channelId;
      const existing = this.#channelsById.get(channelId);
      if (!existing) {
        throw new Error(`Encountered message on channel ${channelId} without prior channel record`);
      }
    }

    if (result.done === true) {
      this.#doneReading = true;
    }
    return result.value;
  }

  *#read(): Generator<TypedMcapRecord | undefined, TypedMcapRecord | undefined, void> {
    if (!this.#noMagicPrefix) {
      let magic: McapMagic | undefined, usedBytes: number | undefined;
      while ((({ magic, usedBytes } = parseMagic(this.#buffer.view, 0)), !magic)) {
        yield;
      }
      this.#buffer.consume(usedBytes);
    }

    let header: TypedMcapRecords["Header"] | undefined;

    function errorWithLibrary(message: string): Error {
      return new Error(`${message} ${header ? `[library=${header.library}]` : "[no header]"}`);
    }

    for (;;) {
      let record;
      {
        let usedBytes;
        while (
          (({ record, usedBytes } = parseRecord({
            view: this.#buffer.view,
            startOffset: 0,
            validateCrcs: this.#validateCrcs,
          })),
          !record)
        ) {
          yield;
        }
        this.#buffer.consume(usedBytes);
      }
      switch (record.type) {
        case "Unknown":
          break;
        case "Header":
          if (header) {
            throw new Error(
              `Duplicate Header record: library=${header.library} profile=${header.profile} vs. library=${record.library} profile=${record.profile}`,
            );
          }
          header = record;
          yield record;
          break;
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
          if (this.#includeChunks) {
            yield record;
          }
          let buffer = record.records;
          if (record.compression !== "" && buffer.byteLength > 0) {
            const decompress = this.#decompressHandlers[record.compression];
            if (!decompress) {
              throw errorWithLibrary(`Unsupported compression ${record.compression}`);
            }
            buffer = decompress(buffer, record.uncompressedSize);
          }
          if (this.#validateCrcs && record.uncompressedCrc !== 0) {
            const chunkCrc = crc32(buffer);
            if (chunkCrc !== record.uncompressedCrc) {
              throw errorWithLibrary(
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
              validateCrcs: this.#validateCrcs,
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
                throw errorWithLibrary(
                  `${chunkResult.record.type} record not allowed inside a chunk`,
                );
              case "Schema":
              case "Channel":
              case "Message":
                yield chunkResult.record;
                break;
            }
          }
          if (chunkOffset !== buffer.byteLength) {
            throw errorWithLibrary(`${buffer.byteLength - chunkOffset} bytes remaining in chunk`);
          }
          break;
        }
        case "Footer":
          try {
            let magic, usedBytes;
            while ((({ magic, usedBytes } = parseMagic(this.#buffer.view, 0)), !magic)) {
              yield;
            }
            this.#buffer.consume(usedBytes);
          } catch (error) {
            throw errorWithLibrary((error as Error).message);
          }
          if (this.#buffer.bytesRemaining() !== 0) {
            throw errorWithLibrary(
              `${this.#buffer.bytesRemaining()} bytes remaining after MCAP footer and trailing magic`,
            );
          }
          return record;
      }
    }
  }
}

function isChannelEqual(a: Channel, b: Channel): boolean {
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
