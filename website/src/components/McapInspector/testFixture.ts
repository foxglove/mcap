import type { Recording, ChunkInfo, ChannelRow } from "./model.ts";

export function overlappingRecording(): Recording {
  const origin = 1_720_000_000_000_000_000n;
  const chunks: ChunkInfo[] = [0, 1].map((id) => ({
    id,
    offset: 100 + id * 1000,
    byteLength: 1000,
    compression: "none",
    compressedSize: 900,
    uncompressedSize: 900,
    startTime: origin + BigInt(id) * 1_000_000_000n,
    endTime: origin + BigInt(id + 2) * 1_000_000_000n,
    messageCount: 6,
    ranges: new Map([
      [1, { start: id, end: id + 2, count: 3 }],
      [2, { start: id, end: id + 2, count: 3 }],
    ]),
  }));
  const channels: ChannelRow[] = [1, 2].map((id) => ({
    id,
    topic: id === 1 ? "/imu" : "/camera",
    schemaId: 0,
    encoding: "json",
    messages: chunks
      .flatMap((chunk) =>
        [0, 1, 2].map((dt) => ({
          time: chunk.id + dt,
          logTime: origin + BigInt(chunk.id + dt) * 1_000_000_000n,
          publishTime: origin,
          sequence: chunk.id * 3 + dt,
          size: 10,
          offset: chunk.offset,
          chunkId: chunk.id,
        })),
      )
      .sort((a, b) => a.time - b.time),
  }));
  return {
    name: "overlap",
    startTime: origin,
    duration: 3,
    fileSize: 2100,
    channels,
    chunks,
    messageCount: 12,
    looseCount: 0,
    profile: "",
  };
}
