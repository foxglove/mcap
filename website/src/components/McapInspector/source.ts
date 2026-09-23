import {
  MCAP_MAGIC,
  McapRecordBuilder,
  McapStreamReader,
  type DecompressHandlers,
  type IReadable,
  type TypedMcapRecord,
} from "@mcap/core";

import type { ChannelRow, ChunkInfo, MessageMark, Recording } from "./model.ts";
// Bound transient payload allocations and retained message metadata.
const MAX_RECORD_BYTES = 512 * 1024 * 1024;
const MAX_MESSAGES = 2_000_000;

export function fileReadable(file: Blob): IReadable {
  return {
    size: async () => BigInt(file.size),
    read: async (offset, size) =>
      new Uint8Array(
        await file.slice(Number(offset), Number(offset + size)).arrayBuffer(),
      ),
  };
}

function records(bytes: Uint8Array): TypedMcapRecord[] {
  const reader = new McapStreamReader({ noMagicPrefix: true });
  reader.append(bytes);
  const result: TypedMcapRecord[] = [];
  for (let record; (record = reader.nextRecord()); ) {
    result.push(record);
  }
  if (reader.bytesRemaining() !== 0) {
    throw new Error("Truncated metadata record.");
  }
  return result;
}

interface ChunkMessages {
  channels: Map<number, MessageMark[]>;
  count: number;
}

/** A seekable catalog: inspect record headers, skip payloads, decode only visible chunks.
 * Works without summary/message indexes and preserves mixed loose/chunked records.
 */
export class InspectorSource {
  readonly recording: Recording;
  #readable: IReadable;
  #handlers: DecompressHandlers;
  #definitions = new Map<
    number,
    Extract<TypedMcapRecord, { type: "Channel" }>
  >();
  #loose = new Map<number, MessageMark[]>();
  #cache = new Map<number, ChunkMessages>();
  #cacheMessages = 0;
  // Full chunk metadata backing the latest completed window, bounded by MAX_MESSAGES.
  #retained = new Map<number, ChunkMessages>();
  #signal: AbortSignal;
  private constructor(
    readable: IReadable,
    handlers: DecompressHandlers,
    size: number,
    name: string,
    signal: AbortSignal,
  ) {
    this.#readable = readable;
    this.#handlers = handlers;
    this.#signal = signal;
    this.recording = {
      name,
      fileSize: size,
      channels: [],
      chunks: [],
      startTime: 0n,
      duration: 0,
      messageCount: 0,
      looseCount: 0,
      profile: "",
      partial: true,
    };
  }
  async #read(offset: number, size: number): Promise<Uint8Array> {
    this.#signal.throwIfAborted();
    if (
      size > MAX_RECORD_BYTES ||
      offset < 0 ||
      size < 0 ||
      offset + size > this.recording.fileSize
    ) {
      throw new Error("Truncated record or record exceeds the 512 MiB limit.");
    }
    const result = await this.#readable.read(BigInt(offset), BigInt(size));
    this.#signal.throwIfAborted();
    if (result.length !== size) {
      throw new Error("Truncated read from MCAP source.");
    }
    return result;
  }
  static async open(
    readable: IReadable,
    handlers: DecompressHandlers,
    name: string,
    signal: AbortSignal,
    progress: (fraction: number) => void = () => {
      /* Optional catalog progress. */
    },
  ): Promise<InspectorSource> {
    const size = await readable.size();
    if (size > BigInt(Number.MAX_SAFE_INTEGER)) {
      throw new Error("Source is too large for exact file offsets.");
    }
    const source = new InspectorSource(
      readable,
      handlers,
      Number(size),
      name,
      signal,
    );
    await source.#catalog(progress);
    return source;
  }
  #channel(record: Extract<TypedMcapRecord, { type: "Channel" }>) {
    this.#definitions.set(record.id, record);
  }
  async #catalog(progress: (fraction: number) => void) {
    const magic = await this.#read(0, 8);
    if (!MCAP_MAGIC.every((byte, i) => byte === magic[i])) {
      throw new Error("Invalid MCAP magic.");
    }
    const tail = await this.#read(this.recording.fileSize - 37, 37);
    if (
      tail[0] !== 2 ||
      new DataView(tail.buffer, tail.byteOffset, tail.byteLength).getBigUint64(
        1,
        true,
      ) !== 20n ||
      !MCAP_MAGIC.every((byte, i) => byte === tail[29 + i])
    ) {
      throw new Error("Incomplete MCAP footer or trailing magic.");
    }
    let offset = 8;
    let first: bigint | undefined;
    let last = 0n;
    let lastProgress = 0;
    let count = 0;
    const extend = (start: bigint, end = start) => {
      first = first == undefined || start < first ? start : first;
      if (end > last) {
        last = end;
      }
    };
    while (offset < this.recording.fileSize - 37) {
      if (++count > MAX_MESSAGES) {
        throw new Error(
          "Too many top-level records for the inspector catalog.",
        );
      }
      const prefix = await this.#read(offset, 9);
      const view = new DataView(
        prefix.buffer,
        prefix.byteOffset,
        prefix.byteLength,
      );
      const opcode = prefix[0]!;
      const length64 = view.getBigUint64(1, true) + 9n;
      if (length64 > BigInt(Number.MAX_SAFE_INTEGER)) {
        throw new Error("Record length exceeds exact file offsets.");
      }
      const length = Number(length64);
      if (offset + length > this.recording.fileSize - 37) {
        throw new Error("Truncated MCAP record.");
      }
      if (offset === 8 && opcode !== 1) {
        throw new Error("MCAP Header must be the first record.");
      }
      if (opcode === 1 || opcode === 4 || opcode === 11) {
        for (const record of records(await this.#read(offset, length))) {
          if (record.type === "Channel") {
            this.#channel(record);
          } else if (record.type === "Header") {
            this.recording.profile = record.profile;
          } else if (record.type === "Statistics") {
            this.recording.totalMessageCount = Number(record.messageCount);
          }
        }
      } else if (opcode === 6) {
        if (length < 49) {
          throw new Error("Invalid chunk header.");
        }
        const header = await this.#read(offset + 9, 32);
        const chunk = new DataView(
          header.buffer,
          header.byteOffset,
          header.byteLength,
        );
        const compressionLength = chunk.getUint32(28, true);
        if (compressionLength > length - 49) {
          throw new Error("Invalid chunk compression field.");
        }
        const suffix = await this.#read(offset + 41, compressionLength + 8);
        const startTime = chunk.getBigUint64(0, true),
          endTime = chunk.getBigUint64(8, true);
        if (endTime < startTime) {
          throw new Error("Invalid chunk time range.");
        }
        const compressedSize = Number(
          new DataView(
            suffix.buffer,
            suffix.byteOffset + compressionLength,
            8,
          ).getBigUint64(0, true),
        );
        if (compressedSize !== length - 49 - compressionLength) {
          throw new Error("Invalid chunk records length.");
        }
        this.recording.chunks.push({
          id: this.recording.chunks.length,
          offset,
          byteLength: length,
          compression:
            new TextDecoder().decode(suffix.subarray(0, compressionLength)) ||
            "none",
          compressedSize,
          uncompressedSize: Number(chunk.getBigUint64(16, true)),
          startTime,
          endTime,
          messageCount: 0,
          ranges: new Map(),
          loaded: false,
        });
        extend(startTime, endTime);
      } else if (opcode === 5) {
        if (length < 31) {
          throw new Error("Invalid message record.");
        }
        const bytes = await this.#read(offset + 9, 22);
        const message = new DataView(
          bytes.buffer,
          bytes.byteOffset,
          bytes.byteLength,
        );
        const channel = message.getUint16(0, true),
          logTime = message.getBigUint64(6, true);
        const marks = this.#loose.get(channel) ?? [];
        marks.push({
          logTime,
          publishTime: message.getBigUint64(14, true),
          sequence: message.getUint32(2, true),
          size: length - 31,
          chunkId: null,
          offset,
          time: 0,
        });
        this.#loose.set(channel, marks);
        this.recording.looseCount++;
        extend(logTime);
      }
      offset += length;
      if (Date.now() - lastProgress > 80) {
        progress(offset / this.recording.fileSize);
        lastProgress = Date.now();
      }
    }
    if (offset !== this.recording.fileSize - 37) {
      throw new Error("Invalid footer position.");
    }
    this.recording.startTime = first ?? 0n;
    this.recording.duration =
      first == undefined ? 0 : Number(last - first) / 1e9;
    for (const marks of this.#loose.values()) {
      for (const mark of marks) {
        mark.time = Number(mark.logTime - this.recording.startTime) / 1e9;
      }
    }
    this.recording.channels = [...this.#definitions.values()].map((c) => ({
      id: c.id,
      topic: c.topic,
      schemaId: c.schemaId,
      encoding: c.messageEncoding,
      messages: [],
    }));
    progress(1);
  }
  async #chunk(
    chunk: ChunkInfo,
    isCurrent: () => boolean,
  ): Promise<ChunkMessages> {
    try {
      return await this.#decodeChunk(chunk);
    } catch (error) {
      if (
        !(error instanceof Error) ||
        !error.message.includes("without prior channel record")
      ) {
        throw error;
      }
      // Unindexed files may define a channel only inside an earlier chunk.
      // Discover those definitions in physical order only when actually needed.
      for (const previous of this.recording.chunks) {
        if (!isCurrent()) {
          return { channels: new Map(), count: 0 };
        }
        if (previous.id >= chunk.id) {
          break;
        }
        await this.#decodeChunk(previous);
      }
      return await this.#decodeChunk(chunk);
    }
  }
  async #decodeChunk(chunk: ChunkInfo): Promise<ChunkMessages> {
    const retained = this.#retained.get(chunk.id);
    if (retained) {
      return retained;
    }
    const cached = this.#cache.get(chunk.id);
    if (cached) {
      this.#cache.delete(chunk.id);
      this.#cache.set(chunk.id, cached);
      return cached;
    }
    if (chunk.uncompressedSize > MAX_RECORD_BYTES) {
      throw new Error("A decompressed chunk exceeds the 512 MiB limit.");
    }
    const reader = new McapStreamReader({
      noMagicPrefix: true,
      decompressHandlers: this.#handlers,
    });
    const definitions = new McapRecordBuilder();
    for (const channel of this.#definitions.values()) {
      definitions.writeChannel(channel);
    }
    reader.append(definitions.buffer);
    while (reader.nextRecord()) {
      /* Seed channel definitions outside this chunk. */
    }
    reader.append(await this.#read(chunk.offset, chunk.byteLength));
    const result: ChunkMessages = { channels: new Map(), count: 0 };
    chunk.ranges.clear();
    for (let record; (record = reader.nextRecord()); ) {
      if (record.type === "Channel") {
        this.#channel(record);
      } else if (record.type === "Message") {
        if (++result.count > MAX_MESSAGES) {
          throw new Error(
            "This chunk exceeds the 2,000,000-message metadata limit.",
          );
        }
        const time = Number(record.logTime - this.recording.startTime) / 1e9;
        const messages = result.channels.get(record.channelId) ?? [];
        messages.push({
          logTime: record.logTime,
          publishTime: record.publishTime,
          sequence: record.sequence,
          size: record.data.length,
          chunkId: chunk.id,
          offset: chunk.offset,
          time,
        });
        result.channels.set(record.channelId, messages);
        const range = chunk.ranges.get(record.channelId);
        if (range) {
          range.start = Math.min(range.start, time);
          range.end = Math.max(range.end, time);
          range.count++;
        } else {
          chunk.ranges.set(record.channelId, {
            start: time,
            end: time,
            count: 1,
          });
        }
      }
    }
    chunk.messageCount = result.count;
    chunk.loaded = true;
    // Keep a small metadata-only LRU; no decompressed payloads survive this method.
    if (result.count <= 200_000) {
      this.#cache.set(chunk.id, result);
      this.#cacheMessages += result.count;
      while (this.#cacheMessages > 200_000 || this.#cache.size > 32) {
        const oldest = this.#cache.keys().next().value!;
        this.#cacheMessages -= this.#cache.get(oldest)!.count;
        this.#cache.delete(oldest);
      }
    }
    return result;
  }
  async readWindow(
    start: number,
    end: number,
    isCurrent: () => boolean = () => true,
    progress: (fraction: number) => void = () => {
      /* Optional window progress. */
    },
  ): Promise<Recording | undefined> {
    const nextRetained = new Map<number, ChunkMessages>();
    const channels = new Map<number, ChannelRow>();
    const add = (id: number, marks: MessageMark[]) => {
      let channel = channels.get(id);
      if (!channel) {
        const definition = this.#definitions.get(id);
        if (!definition) {
          throw new Error(`Missing definition for channel ${id}.`);
        }
        channel = {
          id,
          topic: definition.topic,
          schemaId: definition.schemaId,
          encoding: definition.messageEncoding,
          messages: [],
        };
        channels.set(id, channel);
      }
      for (const mark of marks) {
        if (mark.time >= start && mark.time <= end) {
          channel.messages.push(mark);
        }
      }
    };
    const chunks = this.recording.chunks.filter((chunk) => {
      const a = Number(chunk.startTime - this.recording.startTime) / 1e9;
      const b = Number(chunk.endTime - this.recording.startTime) / 1e9;
      return b >= start && a <= end;
    });
    const totalBytes = chunks.reduce((sum, chunk) => sum + chunk.byteLength, 0);
    let completedBytes = 0;
    let count = 0;
    if (!isCurrent()) {
      return undefined;
    }
    progress(0);
    for (const chunk of chunks) {
      if (!isCurrent()) {
        return undefined;
      }
      const result = await this.#chunk(chunk, isCurrent);
      if (!isCurrent()) {
        return undefined;
      }
      count += result.count;
      if (count > MAX_MESSAGES) {
        throw new Error(
          "This view exceeds the 2,000,000-message metadata limit. Zoom in to a smaller time range.",
        );
      }
      nextRetained.set(chunk.id, result);
      for (const [id, marks] of result.channels) {
        add(id, marks);
      }
      completedBytes += chunk.byteLength;
      // Reserve completion for loose metadata, channel discovery, and sorting.
      progress(totalBytes > 0 ? (0.95 * completedBytes) / totalBytes : 0.95);
    }
    const visibleLoose = new Map<number, MessageMark[]>();
    for (const [id, marks] of this.#loose) {
      const visible = marks.filter(
        (mark) => mark.time >= start && mark.time <= end,
      );
      if (visible.length > 0) {
        visibleLoose.set(id, visible);
      }
    }
    const missing = new Set(
      [...visibleLoose.keys()].filter((id) => !this.#definitions.has(id)),
    );
    if (missing.size > 0) {
      for (const chunk of this.recording.chunks) {
        if (!isCurrent()) {
          return undefined;
        }
        await this.#chunk(chunk, isCurrent);
        for (const id of missing) {
          if (this.#definitions.has(id)) {
            missing.delete(id);
          }
        }
        if (missing.size === 0) {
          break;
        }
      }
    }
    for (const [id, marks] of visibleLoose) {
      add(id, marks);
    }
    for (const id of this.#definitions.keys()) {
      add(id, []);
    }
    for (const channel of channels.values()) {
      channel.messages.sort((a, b) =>
        a.logTime < b.logTime ? -1 : a.logTime > b.logTime ? 1 : 0,
      );
    }
    const result = {
      ...this.recording,
      channels: [...channels.values()].sort((a, b) => a.id - b.id),
      messageCount: [...channels.values()].reduce(
        (sum, c) => sum + c.messages.length,
        0,
      ),
      loadedRange: { start, end },
    };
    if (!isCurrent()) {
      return undefined;
    }
    this.#retained = nextRetained;
    progress(1);
    return result;
  }
}
