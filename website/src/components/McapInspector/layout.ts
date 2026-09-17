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
  expanded: boolean;
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

export function chunkRows(
  groups: ChunkGroup[],
  filter: string,
  expanded: ReadonlySet<number | "loose">,
): TimelineRow[] {
  const rows: TimelineRow[] = [];
  for (const group of groups) {
    const children = group.children.filter((row) =>
      matchesChannel(row.channel, filter),
    );
    if (children.length === 0 && filter) {
      continue;
    }
    const open = expanded.has(group.key);
    rows.push({
      ...group,
      kind: "group",
      children,
      expanded: open,
      shownMessageCount: children.reduce(
        (sum, child) => sum + child.messages.length,
        0,
      ),
    });
    if (open) {
      rows.push(...children);
    }
  }
  return rows;
}

export function groupTimeRange(
  row: GroupRow,
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
