import type { ChannelRow, ChunkInfo, MessageMark, Recording } from "./model.ts";

export type Grouping = "channel" | "chunk";
export interface MessageRow {
  kind: "channel";
  channel: ChannelRow;
  messages: MessageMark[];
  /** Set only on channel rows nested under a physical chunk. */
  chunk?: ChunkInfo;
}
export interface ChunkGroup {
  key: number | "loose";
  chunk?: ChunkInfo;
  children: MessageRow[];
}
export interface GroupRow extends ChunkGroup {
  kind: "group";
  groups: ChunkGroup[];
  messages: MessageMark[];
  shownMessageCount: number;
}
export type TimelineRow = MessageRow | GroupRow;

/** Partition by actual record membership once, retaining references to message metadata.
 * Each child's messages stay in log-time order because the source channel is sorted.
 */
export function groupChunks(recording: Recording): ChunkGroup[] {
  const groups: ChunkGroup[] = recording.chunks.map((chunk) => ({
    key: chunk.id,
    chunk,
    children: [],
  }));
  const byId = new Map(groups.map((group) => [group.key, group]));
  const loose: ChunkGroup = { key: "loose", children: [] };
  for (const channel of recording.channels) {
    const partitions = new Map<number | "loose", MessageMark[]>();
    for (const message of channel.messages) {
      const key = message.chunkId ?? "loose";
      let messages = partitions.get(key);
      if (!messages) {
        messages = [];
        partitions.set(key, messages);
      }
      messages.push(message);
    }
    for (const [key, messages] of partitions) {
      const group = key === "loose" ? loose : byId.get(key);
      if (!group) {
        throw new Error(`Missing physical chunk ${key}.`);
      }
      group.children.push({
        kind: "channel",
        channel,
        messages,
        chunk: group.chunk,
      });
    }
  }
  if (loose.children.length > 0) {
    groups.push(loose);
  }
  return groups;
}

export function matchesChannel(channel: ChannelRow, filter: string): boolean {
  return `${channel.id} ${channel.topic}`
    .toLowerCase()
    .includes(filter.toLowerCase());
}

/** Interval partitioning in O(chunks log lanes). MCAP end timestamps are inclusive,
 * so chunks sharing an endpoint occupy separate lanes. IDs and offsets break ties.
 */
export function chunkRows(groups: ChunkGroup[], filter: string): GroupRow[] {
  const selected = groups
    .map((group) => ({
      ...group,
      children: group.children.filter((row) =>
        matchesChannel(row.channel, filter),
      ),
    }))
    .filter((group) => !filter || group.children.length > 0);
  const ordered = selected
    .filter((group) => group.chunk)
    .sort((a, b) => {
      const left = a.chunk!,
        right = b.chunk!;
      return left.startTime < right.startTime
        ? -1
        : left.startTime > right.startTime
          ? 1
          : left.offset !== right.offset
            ? left.offset - right.offset
            : left.id - right.id;
    });
  const lanes: ChunkGroup[][] = [];
  const heap: { end: bigint; lane: number }[] = [];
  const less = (
    a: { end: bigint; lane: number },
    b: { end: bigint; lane: number },
  ) => a.end < b.end || (a.end === b.end && a.lane < b.lane);
  for (const group of ordered) {
    const chunk = group.chunk!;
    let lane: number;
    if (heap[0] && heap[0].end < chunk.startTime) {
      lane = heap[0].lane;
      heap[0] = heap[heap.length - 1]!;
      heap.pop();
      let index = 0;
      while (index * 2 + 1 < heap.length) {
        let child = index * 2 + 1;
        if (child + 1 < heap.length && less(heap[child + 1]!, heap[child]!)) {
          child++;
        }
        if (!less(heap[child]!, heap[index]!)) {
          break;
        }
        [heap[index], heap[child]] = [heap[child]!, heap[index]!];
        index = child;
      }
    } else {
      lane = lanes.length;
      lanes.push([]);
    }
    lanes[lane]!.push(group);
    heap.push({ end: chunk.endTime, lane });
    let index = heap.length - 1;
    while (index > 0) {
      const parent = (index - 1) >>> 1;
      if (!less(heap[index]!, heap[parent]!)) {
        break;
      }
      [heap[index], heap[parent]] = [heap[parent]!, heap[index]!];
      index = parent;
    }
  }
  const makeRow = (members: ChunkGroup[], key: number | "loose"): GroupRow => {
    const children = members.flatMap((group) => group.children);
    const messages = children
      .flatMap((child) => child.messages)
      .sort((a, b) => a.time - b.time);
    return {
      kind: "group",
      key,
      groups: members,
      children,
      messages,
      shownMessageCount: messages.length,
    };
  };
  const rows = lanes.map((members, index) => makeRow(members, index));
  const loose = selected.find((group) => group.key === "loose");
  if (loose) {
    rows.push(makeRow([loose], "loose"));
  }
  return rows;
}

export function groupTimeRange(
  row: ChunkGroup,
  origin: bigint,
): { start: number; end: number } | undefined {
  if (row.chunk) {
    return {
      start: Number(row.chunk.startTime - origin) / 1e9,
      end: Number(row.chunk.endTime - origin) / 1e9,
    };
  }
  let start = Infinity,
    end = -Infinity;
  for (const child of row.children) {
    if (child.messages.length > 0) {
      start = Math.min(start, child.messages[0]!.time);
      end = Math.max(end, child.messages[child.messages.length - 1]!.time);
    }
  }
  return Number.isFinite(start) ? { start, end } : undefined;
}
