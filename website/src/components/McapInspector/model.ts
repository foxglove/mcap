export interface MessageMark {
  logTime: bigint;
  publishTime: bigint;
  sequence: number;
  size: number;
  chunkId: number | null;
  /** Absolute file offset of this message (loose), or its enclosing chunk. */
  offset: number;
  /** Seconds from the earliest message, computed after loading to preserve ns precision. */
  time: number;
}
export interface ChannelRow {
  id: number;
  topic: string;
  schemaId: number;
  encoding: string;
  messages: MessageMark[];
}
export interface ChunkInfo {
  id: number;
  offset: number;
  byteLength: number;
  compression: string;
  compressedSize: number;
  uncompressedSize: number;
  startTime: bigint;
  endTime: bigint;
  messageCount: number;
  /** False until this chunk has been decoded. */
  loaded?: boolean;
  ranges: Map<number, { start: number; end: number; count: number }>;
}
export interface Recording {
  name: string;
  fileSize: number;
  channels: ChannelRow[];
  chunks: ChunkInfo[];
  startTime: bigint;
  duration: number;
  messageCount: number;
  looseCount: number;
  profile: string;
  partial?: boolean;
  totalMessageCount?: number;
  loadedRange?: { start: number; end: number };
}
export function lowerBound(messages: MessageMark[], time: number): number {
  let lo = 0,
    hi = messages.length;
  while (lo < hi) {
    const mid = (lo + hi) >>> 1;
    if (messages[mid]!.time < time) {
      lo = mid + 1;
    } else {
      hi = mid;
    }
  }
  return lo;
}
export function bytes(n: number): string {
  if (n < 1024) {
    return `${n.toLocaleString()} B`;
  }
  const units = ["B", "KiB", "MiB", "GiB", "TiB", "PiB"];
  const i = Math.min(
    units.length - 1,
    Math.floor(Math.log(n) / Math.log(1024)),
  );
  return `${(n / 1024 ** i).toFixed(1)} ${units[i] ?? "B"}`;
}
export function timeLabel(seconds: number): string {
  return `${seconds.toFixed(seconds < 0.001 ? 9 : seconds < 1 ? 6 : 3)} s`;
}

export type LoaderRequest =
  | { type: "cancel-window"; id: number }
  | { type: "open"; file?: File; size?: bigint; name: string }
  | { type: "window"; id: number; start: number; end: number }
  | {
      type: "read-result";
      id: number;
      bytes?: Uint8Array<ArrayBuffer>;
      error?: string;
    };

export type LoaderMessage =
  | { type: "prefetched"; id: number; recording: Recording }
  | {
      type: "preload-status";
      id: number;
      state: "loading" | "complete" | "paused";
    }
  | { type: "progress"; fraction: number; id?: number }
  | { type: "opened"; recording: Recording }
  | { type: "window"; id: number; recording: Recording }
  | { type: "read"; id: number; offset: bigint; size: bigint }
  | { type: "error"; id?: number; message: string };
