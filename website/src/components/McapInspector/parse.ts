import { McapStreamReader, type DecompressHandlers } from "@mcap/core";

import type { Recording, ChannelRow, ChunkInfo } from "./model.ts";

// These limits prevent accidental browser OOM; adjust for a known workload.
export const MAX_RECORD_BYTES = 512 * 1024 * 1024;
export const MAX_MESSAGES = 2_000_000;

/** Read each physical top-level record separately so chunk membership is unambiguous.
 * No summary or message index is required; payload bytes are discarded after scanning.
 */
export async function parseMcap(
  file: Blob & { name?: string },
  decompressHandlers: DecompressHandlers,
  progress: (fraction: number) => void = () => {
    /* Progress reporting is optional. */
  },
): Promise<Recording> {
  const reader = new McapStreamReader({
    includeChunks: true,
    decompressHandlers,
    validateCrcs: true,
  });
  const channels = new Map<number, ChannelRow>();
  const chunks: ChunkInfo[] = [];
  let messageCount = 0,
    looseCount = 0,
    startTime: bigint | undefined,
    endTime = 0n,
    profile = "";
  // Cache small adjacent reads, avoiding a Blob read for every tiny record.
  let cacheStart = -1,
    cache = new Uint8Array();
  async function read(offset: number, length: number): Promise<Uint8Array> {
    if (offset + length > file.size) {
      throw new Error(`Truncated MCAP at byte ${offset.toLocaleString()}.`);
    }
    if (offset < cacheStart || offset + length > cacheStart + cache.length) {
      cacheStart = offset;
      cache = new Uint8Array(
        await file
          .slice(
            offset,
            Math.min(file.size, offset + Math.max(length, 4 * 1024 * 1024)),
          )
          .arrayBuffer(),
      );
    }
    return cache.subarray(offset - cacheStart, offset - cacheStart + length);
  }
  if (file.size < 16) {
    throw new Error("This file is too short to be an MCAP recording.");
  }
  reader.append(await read(0, 8));
  reader.nextRecord(); // Validate leading magic.
  let offset = 8,
    lastProgress = 0,
    sawHeader = false;
  while (offset < file.size) {
    const header = await read(offset, 9);
    const opcode = header[0]!;
    if (!sawHeader && opcode !== 0x01) {
      throw new Error("MCAP Header must be the first record.");
    }
    sawHeader = true;
    const length64 = new DataView(
      header.buffer,
      header.byteOffset,
      9,
    ).getBigUint64(1, true);
    if (length64 > BigInt(MAX_RECORD_BYTES)) {
      throw new Error(
        "A record exceeds the 512 MiB safety limit. See README for memory limits.",
      );
    }
    const length = Number(length64) + 9;
    const data = await read(offset, length + (opcode === 0x02 ? 8 : 0));
    // Guard declared uncompressed size before the SDK allocates a decompression buffer.
    if (
      opcode === 0x06 &&
      length >= 33 &&
      new DataView(data.buffer, data.byteOffset, data.byteLength).getBigUint64(
        25,
        true,
      ) > BigInt(MAX_RECORD_BYTES)
    ) {
      throw new Error("A decompressed chunk exceeds the 512 MiB safety limit.");
    }
    reader.append(data);
    let currentChunk: ChunkInfo | undefined;
    for (let record; (record = reader.nextRecord()); ) {
      if (record.type === "Header") {
        profile = record.profile;
      }
      if (record.type === "Channel") {
        if (!channels.has(record.id)) {
          channels.set(record.id, {
            id: record.id,
            topic: record.topic,
            schemaId: record.schemaId,
            encoding: record.messageEncoding,
            messages: [],
          });
        }
      } else if (record.type === "Chunk") {
        currentChunk = {
          id: chunks.length,
          offset,
          byteLength: length,
          compression: record.compression || "none",
          compressedSize: record.records.length,
          uncompressedSize: Number(record.uncompressedSize),
          startTime: record.messageStartTime,
          endTime: record.messageEndTime,
          messageCount: 0,
          ranges: new Map(),
        };
        chunks.push(currentChunk);
      } else if (record.type === "Message") {
        if (++messageCount > MAX_MESSAGES) {
          throw new Error(
            "This recording exceeds the 2,000,000-message safety limit. See README for details.",
          );
        }
        const channel = channels.get(record.channelId);
        if (!channel) {
          throw new Error(
            `Missing definition for channel ${record.channelId}.`,
          );
        }
        if (startTime == undefined || record.logTime < startTime) {
          startTime = record.logTime;
        }
        if (record.logTime > endTime) {
          endTime = record.logTime;
        }
        channel.messages.push({
          logTime: record.logTime,
          publishTime: record.publishTime,
          sequence: record.sequence,
          size: record.data.byteLength,
          chunkId: currentChunk?.id ?? null,
          offset,
          time: 0,
        });
        if (currentChunk) {
          currentChunk.messageCount++;
        } else {
          looseCount++;
        }
      }
    }
    offset += data.length;
    if (Date.now() - lastProgress > 80) {
      progress(offset / file.size);
      lastProgress = Date.now();
    }
    if (reader.done()) {
      break;
    }
  }
  if (!reader.done() || offset !== file.size) {
    throw new Error(
      "Incomplete MCAP: missing footer, invalid trailing magic, or unexpected trailing bytes.",
    );
  }
  const origin = startTime ?? 0n;
  for (const channel of channels.values()) {
    channel.messages.sort((a, b) =>
      a.logTime < b.logTime ? -1 : a.logTime > b.logTime ? 1 : 0,
    );
    for (const message of channel.messages) {
      message.time = Number(message.logTime - origin) / 1e9;
      if (message.chunkId != undefined) {
        const chunk = chunks[message.chunkId]!;
        const range = chunk.ranges.get(channel.id);
        if (range) {
          range.end = message.time;
          range.count++;
        } else {
          chunk.ranges.set(channel.id, {
            start: message.time,
            end: message.time,
            count: 1,
          });
        }
      }
    }
  }
  progress(1);
  return {
    name: file.name ?? "recording.mcap",
    fileSize: file.size,
    channels: [...channels.values()].sort((a, b) => a.id - b.id),
    chunks,
    startTime: origin,
    duration: startTime == undefined ? 0 : Number(endTime - origin) / 1e9,
    messageCount,
    looseCount,
    profile,
  };
}
