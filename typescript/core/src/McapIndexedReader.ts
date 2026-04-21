import { crc32, crc32Final, crc32Init, crc32Update } from "@foxglove/crc";
import { Heap } from "heap-js";

import { ChunkCursor } from "./ChunkCursor.ts";
import Reader from "./Reader.ts";
import { MCAP_MAGIC } from "./constants.ts";
import { parseMagic, parseRecord } from "./parse.ts";
import type { DecompressHandlers, IReadable, TypedMcapRecords } from "./types.ts";

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
  messageIndexCache?: ReadonlyMap<bigint, Uint8Array>;
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
  #decompressHandlers?: DecompressHandlers;
  #messageIndexCache?: ReadonlyMap<bigint, Uint8Array>;

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
    this.#messageIndexCache = args.messageIndexCache;

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
  }

  #errorWithLibrary(message: string): Error {
    return new Error(`${message} [library=${this.header.library}]`);
  }

  static async Initialize({
    readable,
    decompressHandlers,
    prefetchMessageIndexes = false,
  }: {
    readable: IReadable;

    /**
     * When a compressed chunk is encountered, the entry in `decompressHandlers` corresponding to the
     * compression will be called to decompress the chunk data.
     */
    decompressHandlers?: DecompressHandlers;

    /**
     * When `true`, every chunk's MessageIndex records are read and cached in memory during
     * `Initialize`. Subsequent calls to `readMessages` reuse the cache instead of issuing one
     * network/disk read per chunk to load message indexes.
     *
     * Use this when:
     * - Message reads are latency-bound (e.g. a remote `IReadable` over HTTP, S3, or similar) and
     *   you want to parallelize the many small message-index reads up front instead of serializing
     *   them inside `readMessages`.
     * - You will call `readMessages` multiple times on the same reader (e.g. seeking, topic
     *   filtering, playback UIs) and want consistently low per-call latency.
     *
     * Avoid this when:
     * - The file is very large and memory is constrained. The cache holds the entirety of every
     *   chunk's MessageIndex bytes until the reader is discarded.
     * - You only need to read a small, known slice of the file once. The prefetch will read
     *   message indexes for chunks you never iterate, which can be wasteful.
     * - The underlying `IReadable` is already low-latency (e.g. a local mmapped file) where the
     *   per-chunk await cost is negligible.
     *
     * Memory cost: roughly 16 bytes per message in the file, held for the reader's lifetime
     * (each MessageIndex entry is a `(logTime, offset)` pair of `uint64`s).
     *
     * Parallelism: if the provided `IReadable` advertises `supportsConcurrentReads`, the prefetch
     * issues reads with a small bounded concurrency. Otherwise the reads are serialized so that
     * implementations reusing an internal buffer (e.g. `FileHandleReadable`) remain correct; in
     * that case the main benefit is that the cost is amortized once at init time and reused
     * across every `readMessages` call.
     *
     * Defaults to `false`.
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

    let messageIndexCache: Map<bigint, Uint8Array> | undefined;
    if (prefetchMessageIndexes && chunkIndexes.length > 0) {
      messageIndexCache = new Map<bigint, Uint8Array>();

      // Resolve each chunk's message-index byte range up front so we can either fan out reads in
      // parallel or iterate them sequentially based on the readable's advertised capability.
      const indexRequests: { chunkStartOffset: bigint; offset: bigint; length: bigint }[] = [];
      for (const chunkIndex of chunkIndexes) {
        if (chunkIndex.messageIndexLength === 0n) {
          messageIndexCache.set(chunkIndex.chunkStartOffset, new Uint8Array());
          continue;
        }
        let messageIndexStartOffset: bigint | undefined;
        for (const offset of chunkIndex.messageIndexOffsets.values()) {
          if (messageIndexStartOffset == undefined || offset < messageIndexStartOffset) {
            messageIndexStartOffset = offset;
          }
        }
        if (messageIndexStartOffset == undefined) {
          messageIndexCache.set(chunkIndex.chunkStartOffset, new Uint8Array());
          continue;
        }
        indexRequests.push({
          chunkStartOffset: chunkIndex.chunkStartOffset,
          offset: messageIndexStartOffset,
          length: chunkIndex.messageIndexLength,
        });
      }

      if (readable.supportsConcurrentReads) {
        // Fan out reads with a bounded worker pool. Concurrency-safe readables guarantee the
        // returned Uint8Arrays won't be mutated or aliased by any subsequent read. We cap the
        // in-flight count so that files with many chunks don't overwhelm the underlying transport
        // (e.g. HTTP connection limits) when reading from a remote IReadable.
        const MAX_CONCURRENT_READS = 6;
        const indexResults = new Array<Uint8Array>(indexRequests.length);
        let nextIndex = 0;
        const workers = Array.from(
          { length: Math.min(MAX_CONCURRENT_READS, indexRequests.length) }, // iterable
          async () => {
            // map function
            for (;;) {
              // Safe: JS is single-threaded; each worker awaits between iterations,
              // so nextIndex++ never races.
              const i = nextIndex++;
              if (i >= indexRequests.length) {
                return;
              }
              const { offset, length } = indexRequests[i]!;
              indexResults[i] = await readable.read(offset, length);
            }
          },
        );
        await Promise.all(workers);
        for (let i = 0; i < indexRequests.length; i++) {
          messageIndexCache.set(indexRequests[i]!.chunkStartOffset, indexResults[i]!);
        }
      } else {
        // Readable may reuse an internal buffer across read() calls; serialize reads and copy each
        // result into a fresh Uint8Array before the next read starts.
        for (const { chunkStartOffset, offset, length } of indexRequests) {
          const bytes = await readable.read(offset, length);
          messageIndexCache.set(chunkStartOffset, new Uint8Array(bytes));
        }
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
      messageIndexCache,
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
    for (const chunkIndex of this.chunkIndexes) {
      if (chunkIndex.messageStartTime <= endTime && chunkIndex.messageEndTime >= startTime) {
        chunkCursors.push(
          new ChunkCursor({
            chunkIndex,
            relevantChannels,
            startTime,
            endTime,
            reverse,
            messageIndexCache: this.#messageIndexCache,
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
        await cursor.loadMessageIndexes(this.#readable);
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
