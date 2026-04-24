import { crc32, crc32Final, crc32Init, crc32Update } from "@foxglove/crc";
import { Heap } from "heap-js";

import { CachedReadable } from "./CachedReadable.ts";
import { ChunkCursor } from "./ChunkCursor.ts";
import Reader from "./Reader.ts";
import { MCAP_MAGIC } from "./constants.ts";
import { parseMagic, parseRecord } from "./parse.ts";
import type { DecompressHandlers, IReadable, TypedMcapRecords } from "./types.ts";

const MAX_PREFETCH_CONCURRENCY = 6;
const MAX_PREFETCH_CACHE_BYTES = 256 * 1024 * 1024; // 256 MB

type McapIndexedReaderArgs = {
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
  dataEndOffset: bigint;
  dataSectionCrc?: number;
  /**
   * Maximum number of bytes of message index data to cache in memory across calls to
   * `readMessages()`. When > 0, message indexes read on demand are cached so subsequent calls do
   * not re-read them from the underlying readable. Defaults to 0 (no caching).
   *
   * When caching is enabled, each chunk's full message index region is read on first access so
   * that later queries against different channels can be served from the cache.
   */
  messageIndexCacheSizeBytes?: number;
  /**
   * When true, message indexes are prefetched in parallel (up to `MAX_PREFETCH_CONCURRENCY`
   * concurrent reads) during reader construction. Requires the readable to support concurrent
   * reads (see {@link IReadable.supportsConcurrentReads}).
   */
  prefetchMessageIndexes?: boolean;
};

export class McapIndexedReader {
  readonly chunkIndexes: readonly TypedMcapRecords["ChunkIndex"][];
  readonly attachmentIndexes: readonly TypedMcapRecords["AttachmentIndex"][];
  readonly metadataIndexes: readonly TypedMcapRecords["MetadataIndex"][] = [];
  readonly channelsById: ReadonlyMap<number, TypedMcapRecords["Channel"]>;
  readonly schemasById: ReadonlyMap<number, TypedMcapRecords["Schema"]>;
  readonly statistics: TypedMcapRecords["Statistics"] | undefined;
  readonly summaryOffsetsByOpcode: ReadonlyMap<number, TypedMcapRecords["SummaryOffset"]>;
  readonly header: TypedMcapRecords["Header"];
  readonly footer: TypedMcapRecords["Footer"];
  // Used for appending attachments/metadata to existing MCAP files
  readonly dataEndOffset: bigint;
  readonly dataSectionCrc?: number;

  #readable: IReadable;
  #messageIndexReadable: IReadable;
  #decompressHandlers?: DecompressHandlers;
  #messageIndexPrefetchPromise?: Promise<void>;
  #messageIndexPrefetchError?: Error;

  #messageStartTime: bigint | undefined;
  #messageEndTime: bigint | undefined;
  #attachmentStartTime: bigint | undefined;
  #attachmentEndTime: bigint | undefined;

  private constructor(args: McapIndexedReaderArgs) {
    this.#readable = args.readable;
    this.chunkIndexes = args.chunkIndexes;
    this.attachmentIndexes = args.attachmentIndexes;
    this.metadataIndexes = args.metadataIndexes;
    this.statistics = args.statistics;
    this.#decompressHandlers = args.decompressHandlers;
    this.channelsById = args.channelsById;
    this.schemasById = args.schemasById;
    this.summaryOffsetsByOpcode = args.summaryOffsetsByOpcode;
    this.header = args.header;
    this.footer = args.footer;
    this.dataEndOffset = args.dataEndOffset;
    this.dataSectionCrc = args.dataSectionCrc;

    const messageIndexCacheSizeBytes = args.messageIndexCacheSizeBytes ?? 0;
    this.#messageIndexReadable =
      messageIndexCacheSizeBytes > 0
        ? new CachedReadable(this.#readable, messageIndexCacheSizeBytes)
        : this.#readable;

    for (const chunk of args.chunkIndexes) {
      if (this.#messageStartTime == undefined || chunk.messageStartTime < this.#messageStartTime) {
        this.#messageStartTime = chunk.messageStartTime;
      }
      if (this.#messageEndTime == undefined || chunk.messageEndTime > this.#messageEndTime) {
        this.#messageEndTime = chunk.messageEndTime;
      }
    }

    for (const attachment of args.attachmentIndexes) {
      if (
        this.#attachmentStartTime == undefined ||
        attachment.logTime < this.#attachmentStartTime
      ) {
        this.#attachmentStartTime = attachment.logTime;
      }
      if (this.#attachmentEndTime == undefined || attachment.logTime > this.#attachmentEndTime) {
        this.#attachmentEndTime = attachment.logTime;
      }
    }

    if (
      args.prefetchMessageIndexes === true &&
      this.#messageIndexReadable instanceof CachedReadable
    ) {
      this.#startMessageIndexPrefetch(this.#messageIndexReadable);
    }
  }

  /**
   * Kick off parallel reads (up to `MAX_PREFETCH_CONCURRENCY`) of every chunk's message index
   * region through the cached readable. Successful reads populate the byte cache so that later
   * `readMessages()` calls hit cache.
   *
   * The first failure is captured on the instance and surfaced once from the next
   * `readMessages()` call; subsequent calls fall back to on-demand loading via
   * `ChunkCursor.loadMessageIndexes()`. Chunks that were successfully prefetched before the
   * failure remain in cache and continue to be served from there.
   */
  #startMessageIndexPrefetch(cachedReadable: CachedReadable): void {
    type Request = { readStart: bigint; readLength: bigint };
    const indexRequests: Request[] = [];
    for (const chunk of this.chunkIndexes) {
      if (chunk.messageIndexLength === 0n) {
        continue;
      }
      let readStart: bigint | undefined;
      for (const offset of chunk.messageIndexOffsets.values()) {
        if (readStart == undefined || offset < readStart) {
          readStart = offset;
        }
      }
      if (readStart == undefined) {
        continue;
      }
      indexRequests.push({ readStart, readLength: chunk.messageIndexLength });
    }

    if (indexRequests.length === 0) {
      return;
    }

    // Shared state between workers. Accessed via closures (`getError`/`setError`) so that
    // TypeScript does not narrow the value across awaits; each call re-reads the current value.
    let prefetchError: Error | undefined;
    const getError = (): Error | undefined => prefetchError;
    const setError = (err: Error): void => {
      prefetchError = err;
    };
    let nextIndex = 0;
    const workers = Array.from(
      { length: Math.min(MAX_PREFETCH_CONCURRENCY, indexRequests.length) },
      async () => {
        for (;;) {
          // Safe: JS is single-threaded; each worker awaits between iterations,
          // so nextIndex++ never races.
          if (getError()) {
            return;
          }
          const i = nextIndex++;
          if (i >= indexRequests.length) {
            return;
          }
          const req = indexRequests[i]!;
          try {
            await cachedReadable.read(req.readStart, req.readLength);
          } catch (err) {
            // On the first failure, record the error. Other workers observe the error flag on
            // their next iteration and exit without processing further requests. Successfully
            // prefetched entries stay in cache and will be used by on-demand loads.
            if (!getError()) {
              setError(err as Error);
            }
            return;
          }
        }
      },
    );
    this.#messageIndexPrefetchPromise = Promise.all(workers).then(() => {
      this.#messageIndexPrefetchError = getError();
    });
  }

  #errorWithLibrary(message: string): Error {
    return new Error(`${message} [library=${this.header.library}]`);
  }

  static async Initialize({
    readable,
    decompressHandlers,
    messageIndexCacheSizeBytes,
    prefetchMessageIndexes,
  }: {
    readable: IReadable;

    /**
     * When a compressed chunk is encountered, the entry in `decompressHandlers` corresponding to the
     * compression will be called to decompress the chunk data.
     */
    decompressHandlers?: DecompressHandlers;

    /**
     * Maximum number of bytes of message index data to cache in memory across calls to
     * `readMessages()`. When > 0, message indexes read on demand are cached so subsequent calls do
     * not re-read them from the underlying readable. Defaults to 0 (no caching).
     */
    messageIndexCacheSizeBytes?: number;

    /**
     * When true, message indexes are prefetched in parallel (up to 6 concurrent reads) during
     * reader construction. Requires `readable.supportsConcurrentReads === true`; otherwise a
     * warning is logged and the option is downgraded to on-demand loading with message index
     * caching. Implies message index caching; the cache is sized to fit all message indexes,
     * capped at 256 MB.
     */
    prefetchMessageIndexes?: boolean;
  }): Promise<McapIndexedReader> {
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
      void parseMagic(new Reader(headerPrefixView));
      const headerContentLength = headerPrefixView.getBigUint64(
        MCAP_MAGIC.length + /* Opcode.HEADER */ 1,
        true,
      );
      const headerReadLength =
        /* Opcode.HEADER */ 1n + /* record content length */ 8n + headerContentLength;

      const headerRecord = await readable.read(BigInt(MCAP_MAGIC.length), headerReadLength);
      headerEndOffset = BigInt(MCAP_MAGIC.length) + headerReadLength;
      const headerReader = new Reader(
        new DataView(headerRecord.buffer, headerRecord.byteOffset, headerRecord.byteLength),
      );
      const headerResult = parseRecord(headerReader, true);
      if (headerResult?.type !== "Header") {
        throw new Error(
          `Unable to read header at beginning of file; found ${headerResult?.type ?? "nothing"}`,
        );
      }
      if (headerReader.bytesRemaining() !== 0) {
        throw new Error(`${headerReader.bytesRemaining()} bytes remaining after parsing header`);
      }
      header = headerResult;
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
      void parseMagic(
        new Reader(footerAndMagicView, footerAndMagicView.byteLength - MCAP_MAGIC.length),
      );
    } catch (error) {
      throw errorWithLibrary((error as Error).message);
    }

    let footer: TypedMcapRecords["Footer"];
    {
      const footerReader = new Reader(footerAndMagicView);
      const footerRecord = parseRecord(footerReader, true);
      if (footerRecord?.type !== "Footer") {
        throw errorWithLibrary(
          `Unable to read footer from end of file (offset ${footerOffset}); found ${
            footerRecord?.type ?? "nothing"
          }`,
        );
      }
      if (footerReader.bytesRemaining() !== MCAP_MAGIC.length) {
        throw errorWithLibrary(
          `${
            footerReader.bytesRemaining() - MCAP_MAGIC.length
          } bytes remaining after parsing footer`,
        );
      }
      footer = footerRecord;
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
    const indexReader = new Reader(indexView);

    const channelsById = new Map<number, TypedMcapRecords["Channel"]>();
    const schemasById = new Map<number, TypedMcapRecords["Schema"]>();
    const chunkIndexes: TypedMcapRecords["ChunkIndex"][] = [];
    const attachmentIndexes: TypedMcapRecords["AttachmentIndex"][] = [];
    const metadataIndexes: TypedMcapRecords["MetadataIndex"][] = [];
    const summaryOffsetsByOpcode = new Map<number, TypedMcapRecords["SummaryOffset"]>();
    let statistics: TypedMcapRecords["Statistics"] | undefined;
    let dataSectionCrc: number | undefined;

    let first = true;
    let result;
    while ((result = parseRecord(indexReader, true))) {
      if (first && result.type !== "DataEnd") {
        throw errorWithLibrary(
          `Expected DataEnd record to precede summary section, but found ${result.type}`,
        );
      }
      first = false;
      switch (result.type) {
        case "Schema":
          schemasById.set(result.id, result);
          break;
        case "Channel":
          channelsById.set(result.id, result);
          break;
        case "ChunkIndex":
          chunkIndexes.push(result);
          break;
        case "AttachmentIndex":
          attachmentIndexes.push(result);
          break;
        case "MetadataIndex":
          metadataIndexes.push(result);
          break;
        case "Statistics":
          if (statistics) {
            throw errorWithLibrary("Duplicate Statistics record");
          }
          statistics = result;
          break;
        case "SummaryOffset":
          summaryOffsetsByOpcode.set(result.groupOpcode, result);
          break;
        case "DataEnd":
          dataSectionCrc = result.dataSectionCrc === 0 ? undefined : result.dataSectionCrc;
          break;
        case "Header":
        case "Footer":
        case "Message":
        case "Chunk":
        case "MessageIndex":
        case "Attachment":
        case "Metadata":
          throw errorWithLibrary(`${result.type} record not allowed in index section`);
        case "Unknown":
          break;
      }
    }
    if (indexReader.bytesRemaining() !== 0) {
      throw errorWithLibrary(`${indexReader.bytesRemaining()} bytes remaining in index section`);
    }

    let effectiveCacheSizeBytes = messageIndexCacheSizeBytes ?? 0;
    if (prefetchMessageIndexes === true) {
      if (readable.supportsConcurrentReads !== true) {
        throw errorWithLibrary(
          "prefetchMessageIndexes requires the readable to declare supportsConcurrentReads",
        );
      }
      let totalMessageIndexBytes = 0;
      for (const ci of chunkIndexes) {
        totalMessageIndexBytes += Number(ci.messageIndexLength);
      }
      const targetCacheSizeBytes = Math.min(totalMessageIndexBytes, MAX_PREFETCH_CACHE_BYTES);
      if (effectiveCacheSizeBytes < targetCacheSizeBytes) {
        effectiveCacheSizeBytes = targetCacheSizeBytes;
      }
    }

    return new McapIndexedReader({
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
      dataEndOffset,
      dataSectionCrc,
      messageIndexCacheSizeBytes: effectiveCacheSizeBytes,
      prefetchMessageIndexes: effectivePrefetch,
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
    const {
      topics,
      startTime = this.#messageStartTime,
      endTime = this.#messageEndTime,
      reverse = false,
      validateCrcs,
    } = args;

    if (startTime == undefined || endTime == undefined) {
      return;
    }

    // If a prefetch is in flight, wait for it to complete so any captured error can be surfaced
    // and all successfully prefetched indexes are in the cache before we start reading.
    if (this.#messageIndexPrefetchPromise) {
      const promise = this.#messageIndexPrefetchPromise;
      this.#messageIndexPrefetchPromise = undefined;
      await promise;
      const err = this.#messageIndexPrefetchError;
      this.#messageIndexPrefetchError = undefined;
      if (err) {
        throw err;
      }
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
    let chunksOrdered = true;
    let prevChunkEndTime: bigint | undefined;
    const readFullMessageIndexRange = this.#messageIndexReadable !== this.#readable;
    for (const chunkIndex of this.chunkIndexes) {
      if (chunkIndex.messageStartTime <= endTime && chunkIndex.messageEndTime >= startTime) {
        chunkCursors.push(
          new ChunkCursor({
            chunkIndex,
            relevantChannels,
            startTime,
            endTime,
            reverse,
            readFullMessageIndexRange,
          }),
        );
        if (chunksOrdered && prevChunkEndTime != undefined) {
          chunksOrdered = chunkIndex.messageStartTime >= prevChunkEndTime;
        }
        prevChunkEndTime = chunkIndex.messageEndTime;
      }
    }

    // Holds the decompressed chunk data for "active" chunks. Items are added below when a chunk
    // cursor becomes active (i.e. when we first need to access messages from the chunk) and removed
    // when the cursor is removed from the heap.
    const chunkViewCache = new Map<bigint, DataView>();
    const chunkReader = new Reader(new DataView(new ArrayBuffer(0)));
    for (let cursor; (cursor = chunkCursors.peek()); ) {
      if (!cursor.hasMessageIndexes()) {
        // If we encounter a chunk whose message indexes have not been loaded yet, load them and re-organize the heap.
        await cursor.loadMessageIndexes(this.#messageIndexReadable);
        if (cursor.hasMoreMessages()) {
          chunkCursors.replace(cursor);
        } else {
          chunkCursors.pop();
        }
        continue;
      }

      let chunkView = chunkViewCache.get(cursor.chunkIndex.chunkStartOffset);
      if (!chunkView) {
        chunkView = await this.#loadChunkData(cursor.chunkIndex, {
          validateCrcs: validateCrcs ?? true,
        });
        chunkViewCache.set(cursor.chunkIndex.chunkStartOffset, chunkView);
      }

      const [logTime, offset] = cursor.popMessage();
      if (offset >= BigInt(chunkView.byteLength)) {
        throw this.#errorWithLibrary(
          `Message offset beyond chunk bounds (log time ${logTime}, offset ${offset}, chunk data length ${chunkView.byteLength}) in chunk at offset ${cursor.chunkIndex.chunkStartOffset}`,
        );
      }
      chunkReader.reset(chunkView, Number(offset));
      const record = parseRecord(chunkReader, validateCrcs ?? true);
      if (!record) {
        throw this.#errorWithLibrary(
          `Unable to parse record at offset ${offset} in chunk at offset ${cursor.chunkIndex.chunkStartOffset}`,
        );
      }
      if (record.type !== "Message") {
        throw this.#errorWithLibrary(
          `Unexpected record type ${record.type} in message index (time ${logTime}, offset ${offset} in chunk at offset ${cursor.chunkIndex.chunkStartOffset})`,
        );
      }
      if (record.logTime !== logTime) {
        throw this.#errorWithLibrary(
          `Message log time ${record.logTime} did not match message index entry (${logTime} at offset ${offset} in chunk at offset ${cursor.chunkIndex.chunkStartOffset})`,
        );
      }
      yield record;

      if (cursor.hasMoreMessages()) {
        // There is no need to reorganize the heap when chunks are ordered and not overlapping.
        // We can simply keep on reading messages from the current chunk.
        if (!chunksOrdered) {
          chunkCursors.replace(cursor);
        }
      } else {
        chunkCursors.pop();
        chunkViewCache.delete(cursor.chunkIndex.chunkStartOffset);
      }
    }
  }

  async *readMetadata(
    args: {
      name?: string;
    } = {},
  ): AsyncGenerator<TypedMcapRecords["Metadata"], void, void> {
    const { name } = args;

    for (const metadataIndex of this.metadataIndexes) {
      if (name != undefined && metadataIndex.name !== name) {
        continue;
      }
      const metadataData = await this.#readable.read(metadataIndex.offset, metadataIndex.length);
      const metadataReader = new Reader(
        new DataView(metadataData.buffer, metadataData.byteOffset, metadataData.byteLength),
      );
      const metadataRecord = parseRecord(metadataReader, false);
      if (metadataRecord?.type !== "Metadata") {
        throw this.#errorWithLibrary(
          `Metadata data at offset ${
            metadataIndex.offset
          } does not point to metadata record (found ${String(metadataRecord?.type)})`,
        );
      }
      yield metadataRecord;
    }
  }

  async *readAttachments(
    args: {
      name?: string;
      mediaType?: string;
      startTime?: bigint;
      endTime?: bigint;
      validateCrcs?: boolean;
    } = {},
  ): AsyncGenerator<TypedMcapRecords["Attachment"], void, void> {
    const {
      name,
      mediaType,
      startTime = this.#attachmentStartTime,
      endTime = this.#attachmentEndTime,
      validateCrcs,
    } = args;

    if (startTime == undefined || endTime == undefined) {
      return;
    }

    for (const attachmentIndex of this.attachmentIndexes) {
      if (name != undefined && attachmentIndex.name !== name) {
        continue;
      }
      if (mediaType != undefined && attachmentIndex.mediaType !== mediaType) {
        continue;
      }
      if (attachmentIndex.logTime > endTime || attachmentIndex.logTime < startTime) {
        continue;
      }
      const attachmentData = await this.#readable.read(
        attachmentIndex.offset,
        attachmentIndex.length,
      );
      const attachmentReader = new Reader(
        new DataView(attachmentData.buffer, attachmentData.byteOffset, attachmentData.byteLength),
      );
      const attachmentRecord = parseRecord(attachmentReader, validateCrcs ?? true);
      if (attachmentRecord?.type !== "Attachment") {
        throw this.#errorWithLibrary(
          `Attachment data at offset ${
            attachmentIndex.offset
          } does not point to attachment record (found ${String(attachmentRecord?.type)})`,
        );
      }
      yield attachmentRecord;
    }
  }

  async #loadChunkData(
    chunkIndex: TypedMcapRecords["ChunkIndex"],
    options?: { validateCrcs: boolean },
  ): Promise<DataView> {
    const chunkData = await this.#readable.read(
      chunkIndex.chunkStartOffset,
      chunkIndex.chunkLength,
    );
    const chunkReader = new Reader(
      new DataView(chunkData.buffer, chunkData.byteOffset, chunkData.byteLength),
    );
    const chunkRecord = parseRecord(chunkReader, options?.validateCrcs ?? true);
    if (chunkRecord?.type !== "Chunk") {
      throw this.#errorWithLibrary(
        `Chunk start offset ${
          chunkIndex.chunkStartOffset
        } does not point to chunk record (found ${String(chunkRecord?.type)})`,
      );
    }

    const chunk = chunkRecord;
    let buffer = chunk.records;
    if (chunk.compression !== "" && buffer.byteLength > 0) {
      const decompress = this.#decompressHandlers?.[chunk.compression];
      if (!decompress) {
        throw this.#errorWithLibrary(`Unsupported compression ${chunk.compression}`);
      }
      buffer = decompress(buffer, chunk.uncompressedSize);
    }
    if (chunk.uncompressedCrc !== 0 && options?.validateCrcs !== false) {
      const chunkCrc = crc32(buffer);
      if (chunkCrc !== chunk.uncompressedCrc) {
        throw this.#errorWithLibrary(
          `Incorrect chunk CRC ${chunkCrc} (expected ${chunk.uncompressedCrc})`,
        );
      }
    }

    return new DataView(buffer.buffer, buffer.byteOffset, buffer.byteLength);
  }
}
