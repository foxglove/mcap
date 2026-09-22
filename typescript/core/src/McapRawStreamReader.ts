import Reader from "./Reader.ts";
import { MCAP_MAGIC } from "./constants.ts";
import { parseMagic, parseRecord } from "./parse.ts";
import type { McapMagic, TypedMcapRecord, TypedMcapRecords } from "./types.ts";

export type McapRawStreamReaderOptions = {
  /**
   * Validate nonzero attachment CRCs (default: true). Chunk, data-section, and summary CRCs
   * are not validated by this reader.
   */
  validateCrcs?: boolean;

  /** Skip the initial magic prefix when reading a fragment starting at a record boundary. */
  noMagicPrefix?: boolean;
};

/**
 * Incrementally reads typed records from the outer MCAP stream in file order.
 *
 * Chunks are always returned with their original `records` payload, including compressed bytes.
 * No chunk contents are decompressed or parsed, even for uncompressed chunks. Unknown compression
 * algorithms are accepted. Consumers are responsible for grouping chunks with message indexes.
 *
 * Like McapStreamReader, this reader checks magic bytes, record parsing, duplicate headers, and
 * trailing bytes after the footer. It is not a complete MCAP validator. Chunk CRCs and contents
 * must be validated by the layer that expands chunks. Message/channel relationships are not
 * checked here because channel definitions may be inside chunks. Data-section and summary CRCs
 * are not checked.
 *
 * Returned byte arrays are owned copies, independent of input and internal buffering. They may
 * be retained or modified across subsequent `append()` and `nextRecord()` calls.
 *
 * `nextRecord()` returning undefined can mean that more input is needed. At end of input, callers
 * must check `done()` to detect truncation, even when `bytesRemaining()` is zero. With
 * `noMagicPrefix`, completion still requires a footer and trailing magic.
 *
 * @example
 * ```ts
 * const reader = new McapRawStreamReader();
 * for await (const bytes of input) {
 *   reader.append(bytes);
 *   for (let record; (record = reader.nextRecord()) != undefined; ) {
 *     // Handle outer records; record.records is still compressed for compressed chunks.
 *     consume(record);
 *   }
 * }
 * if (!reader.done()) {
 *   throw new Error("Incomplete MCAP stream");
 * }
 * ```
 */
export default class McapRawStreamReader {
  #buffer = new ArrayBuffer(MCAP_MAGIC.length * 2);
  #view = new DataView(this.#buffer, 0, 0);
  #reader = new Reader(this.#view);
  #validateCrcs;
  #noMagicPrefix;
  #doneReading = false;
  #generator = this.#read();

  constructor({ validateCrcs = true, noMagicPrefix = false }: McapRawStreamReaderOptions = {}) {
    this.#validateCrcs = validateCrcs;
    this.#noMagicPrefix = noMagicPrefix;
  }

  /** @returns True once the footer and trailing magic have been parsed successfully. */
  done(): boolean {
    return this.#doneReading;
  }

  /** @returns The number of bytes that have been received by `append()` but not yet parsed. */
  bytesRemaining(): number {
    return this.#reader.bytesRemaining();
  }

  /**
   * Provide the reader with newly received bytes for it to process. After calling this function,
   * call `nextRecord()` again to parse any records that are now available.
   *
   * Input bytes are copied, so callers may reuse or modify `data` after this method returns.
   * Previously returned records and their payloads are unaffected by subsequent appends.
   */
  append(data: Uint8Array): void {
    if (this.#doneReading) {
      throw new Error("Already done reading");
    }
    this.#appendOrShift(data);
  }

  #appendOrShift(data: Uint8Array): void {
    /** Add data to the buffer, shifting existing data or reallocating if necessary. */
    const consumedBytes = this.#reader.offset;
    const unconsumedBytes = this.#view.byteLength - consumedBytes;
    const neededCapacity = unconsumedBytes + data.byteLength;

    if (neededCapacity <= this.#buffer.byteLength) {
      // Data fits in the current buffer
      if (
        this.#view.byteOffset + this.#view.byteLength + data.byteLength <=
        this.#buffer.byteLength
      ) {
        // Data fits by appending only
        const array = new Uint8Array(this.#buffer, this.#view.byteOffset);
        array.set(data, this.#view.byteLength);
        this.#view = new DataView(
          this.#buffer,
          this.#view.byteOffset,
          this.#view.byteLength + data.byteLength,
        );
        // Reset the reader to use the new larger view. We keep the reader's previous offset as the
        // view's byte offset didn't change, it only got larger.
        this.#reader.reset(this.#view, this.#reader.offset);
      } else {
        // Data fits but requires moving existing data to start of buffer
        const existingData = new Uint8Array(
          this.#buffer,
          this.#view.byteOffset + consumedBytes,
          unconsumedBytes,
        );
        const array = new Uint8Array(this.#buffer);
        array.set(existingData, 0);
        array.set(data, existingData.byteLength);
        this.#view = new DataView(this.#buffer, 0, existingData.byteLength + data.byteLength);
        this.#reader.reset(this.#view);
      }
    } else {
      // New data doesn't fit, copy to a new buffer

      // Currently, the new buffer size may be smaller than the old size. For future optimizations,
      // we could consider making the buffer size increase monotonically.
      this.#buffer = new ArrayBuffer(neededCapacity * 2);
      const array = new Uint8Array(this.#buffer);
      const existingData = new Uint8Array(
        this.#view.buffer,
        this.#view.byteOffset + consumedBytes,
        unconsumedBytes,
      );
      array.set(existingData, 0);
      array.set(data, existingData.byteLength);
      this.#view = new DataView(this.#buffer, 0, existingData.byteLength + data.byteLength);
      this.#reader.reset(this.#view);
    }
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

    if (result.done === true) {
      this.#doneReading = true;
    }
    return result.value;
  }

  *#read(): Generator<TypedMcapRecord | undefined, TypedMcapRecord | undefined, void> {
    if (!this.#noMagicPrefix) {
      let magic: McapMagic | undefined;
      while (((magic = parseMagic(this.#reader)), !magic)) {
        yield;
      }
    }

    let header: TypedMcapRecords["Header"] | undefined;

    function errorWithLibrary(message: string): Error {
      return new Error(`${message} ${header ? `[library=${header.library}]` : "[no header]"}`);
    }

    for (;;) {
      let record;
      while (((record = parseRecord(this.#reader, this.#validateCrcs)), !record)) {
        yield;
      }

      switch (record.type) {
        case "Header":
          if (header) {
            throw new Error(
              `Duplicate Header record: library=${header.library} profile=${header.profile} vs. library=${record.library} profile=${record.profile}`,
            );
          }
          header = record;
          yield record;
          break;
        case "Unknown":
          // The low-level parser borrows unknown payloads. Give all emitted byte arrays the
          // same ownership guarantee before the streaming buffer can be reused.
          yield { ...record, data: record.data.slice() };
          break;
        case "Chunk":
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

        case "Footer":
          try {
            let magic;
            while (((magic = parseMagic(this.#reader)), !magic)) {
              yield;
            }
          } catch (error) {
            throw errorWithLibrary((error as Error).message);
          }
          if (this.#reader.bytesRemaining() !== 0) {
            throw errorWithLibrary(
              `${this.#reader.bytesRemaining()} bytes remaining after MCAP footer and trailing magic`,
            );
          }
          return record;
      }
    }
  }
}
