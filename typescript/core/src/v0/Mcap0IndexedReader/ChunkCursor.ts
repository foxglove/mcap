import Heap from "heap-js";
import { sortedIndexBy } from "lodash";

import { parseRecord } from "../parse";
import { IReadable, TypedMcapRecords } from "../types";

type ChunkCursorParams = {
  chunkIndex: TypedMcapRecords["ChunkIndex"];
  relevantChannels: Set<number> | undefined;
  startTime: bigint | undefined;
  endTime: bigint | undefined;
};

/**
 * ChunkCursor represents the reader's position in a particular chunk. The indexed reader holds
 * ChunkCursors in a heap in order to merge multiple chunks together.
 *
 * Each chunk can contain multiple channels, and so a ChunkCursor itself contains a heap of cursors
 * pointing into the message index for each channel of interest.
 */
export class ChunkCursor {
  readonly chunkIndex: TypedMcapRecords["ChunkIndex"];

  private relevantChannels?: Set<number>;
  private startTime: bigint | undefined;
  private endTime: bigint | undefined;

  private messageIndexCursors?: Heap<{
    channelId: number;

    /** index of next message within `records` array */
    index: number;

    records: TypedMcapRecords["MessageIndex"]["records"];
  }>;

  constructor(params: ChunkCursorParams) {
    this.chunkIndex = params.chunkIndex;
    this.relevantChannels = params.relevantChannels;
    this.startTime = params.startTime;
    this.endTime = params.endTime;

    if (this.chunkIndex.messageIndexLength === 0n) {
      throw new Error(`Chunks without message indexes are not currently supported`);
    }
  }

  /**
   * Returns `< 0` if the callee's next available message logTime is earlier than `other`'s, `> 0`
   * for the opposite case. Never returns `0` because ties are broken by the chunks' offsets in the
   * file.
   *
   * Cursors that still need `loadMessageIndexes()` are sorted earlier so the caller can load them
   * and re-sort the cursors.
   */
  compare(other: ChunkCursor): number {
    // If chunks don't overlap at all, sort earlier chunk first
    if (this.chunkIndex.messageEndTime < other.chunkIndex.messageStartTime) {
      return -1;
    }
    if (this.chunkIndex.messageStartTime > other.chunkIndex.messageEndTime) {
      return 1;
    }

    // If a cursor has not loaded message indexes, sort it first so it can get loaded and re-sorted.
    // If it overlaps with other cursors, we won't know its order relative to other cursors until we
    // have the message indexes.
    if (!this.messageIndexCursors) {
      return -1;
    }
    if (!other.messageIndexCursors) {
      return 1;
    }

    // Earlier messages come first
    const cursorA = this.messageIndexCursors.peek();
    if (!cursorA) {
      throw new Error(
        `Unexpected empty cursor for chunk at offset ${this.chunkIndex.chunkStartOffset}`,
      );
    }
    const cursorB = other.messageIndexCursors.peek();
    if (!cursorB) {
      throw new Error(
        `Unexpected empty cursor for chunk at offset ${other.chunkIndex.chunkStartOffset}`,
      );
    }
    const logTimeA = cursorA.records[cursorA.index]![0];
    const logTimeB = cursorB.records[cursorB.index]![0];
    if (logTimeA !== logTimeB) {
      return Number(logTimeA - logTimeB);
    }

    // Break ties by chunk offset in the file
    return Number(this.chunkIndex.chunkStartOffset - other.chunkIndex.chunkStartOffset);
  }

  /**
   * Returns true if there are more messages available in the chunk. Message indexes must have been
   * loaded before using this method.
   */
  hasMoreMessages(): boolean {
    if (!this.messageIndexCursors) {
      throw new Error("loadMessageIndexes() must be called before hasMore()");
    }
    return this.messageIndexCursors.size() > 0;
  }

  /**
   * Pop a message offset off of the chunk cursor. Message indexes must have been loaded before
   * using this method.
   */
  popMessage(): [logTime: bigint, offset: bigint] {
    if (!this.messageIndexCursors) {
      throw new Error("loadMessageIndexes() must be called before popMessage()");
    }
    const cursor = this.messageIndexCursors.peek();
    if (!cursor) {
      throw new Error(
        `Unexpected popMessage() call when no more messages are available, in chunk at offset ${this.chunkIndex.chunkStartOffset}`,
      );
    }
    const record = cursor.records[cursor.index]!;
    const [logTime] = record;
    if (this.startTime != undefined && logTime < this.startTime) {
      throw new Error(
        `Encountered message with logTime (${logTime}) prior to startTime (${this.startTime}) in chunk at offset ${this.chunkIndex.chunkStartOffset}`,
      );
    }
    if (
      cursor.index + 1 < cursor.records.length &&
      (this.endTime == undefined || cursor.records[cursor.index + 1]![0] <= this.endTime)
    ) {
      cursor.index++;
      this.messageIndexCursors.replace(cursor);
    } else {
      this.messageIndexCursors.pop();
    }
    return record;
  }

  /**
   * Returns true if message indexes have been loaded, false if `loadMessageIndexes()` needs to be
   * called.
   */
  hasMessageIndexes(): boolean {
    return this.messageIndexCursors != undefined;
  }

  async loadMessageIndexes(readable: IReadable): Promise<void> {
    this.messageIndexCursors = new Heap((a, b) => {
      const logTimeA = a.records[a.index]?.[0];
      const logTimeB = b.records[b.index]?.[0];
      if (logTimeA == undefined) {
        return 1;
      } else if (logTimeB == undefined) {
        return -1;
      }
      return Number(logTimeA - logTimeB);
    });

    let messageIndexStartOffset: bigint | undefined;
    let relevantMessageIndexStartOffset: bigint | undefined;
    for (const [channelId, offset] of this.chunkIndex.messageIndexOffsets) {
      if (messageIndexStartOffset == undefined || offset < messageIndexStartOffset) {
        messageIndexStartOffset = offset;
      }
      if (!this.relevantChannels || this.relevantChannels.has(channelId)) {
        if (
          relevantMessageIndexStartOffset == undefined ||
          offset < relevantMessageIndexStartOffset
        ) {
          relevantMessageIndexStartOffset = offset;
        }
      }
    }
    if (messageIndexStartOffset == undefined || relevantMessageIndexStartOffset == undefined) {
      return;
    }

    // Future optimization: read only message indexes for given channelIds, not all message indexes for the chunk
    const messageIndexEndOffset = messageIndexStartOffset + this.chunkIndex.messageIndexLength;
    const messageIndexes = await readable.read(
      relevantMessageIndexStartOffset,
      messageIndexEndOffset - relevantMessageIndexStartOffset,
    );
    const messageIndexesView = new DataView(
      messageIndexes.buffer,
      messageIndexes.byteOffset,
      messageIndexes.byteLength,
    );

    let offset = 0;
    for (
      let result;
      (result = parseRecord({ view: messageIndexesView, startOffset: offset, validateCrcs: true })),
        result.record;
      offset += result.usedBytes
    ) {
      if (result.record.type !== "MessageIndex") {
        continue;
      }
      if (
        result.record.records.length === 0 ||
        (this.relevantChannels && !this.relevantChannels.has(result.record.channelId))
      ) {
        continue;
      }

      result.record.records.sort(([logTimeA], [logTimeB]) => Number(logTimeA - logTimeB));

      for (let i = 0; i < result.record.records.length; i++) {
        const [logTime] = result.record.records[i]!;
        if (logTime < this.chunkIndex.messageStartTime) {
          throw new Error(
            `Encountered message index entry in channel ${result.record.channelId} with logTime (${logTime}) earlier than chunk messageStartTime (${this.chunkIndex.messageStartTime}) in chunk at offset ${this.chunkIndex.chunkStartOffset}`,
          );
        }
        if (logTime > this.chunkIndex.messageEndTime) {
          throw new Error(
            `Encountered message index entry in channel ${result.record.channelId} with logTime (${logTime}) later than chunk messageEndTime (${this.chunkIndex.messageEndTime}) in chunk at offset ${this.chunkIndex.chunkStartOffset}`,
          );
        }
      }
      const startIndex =
        this.startTime == undefined
          ? 0
          : sortedIndexBy(result.record.records, [this.startTime], ([logTime]) => logTime);
      if (startIndex >= result.record.records.length) {
        continue;
      }
      this.messageIndexCursors.push({
        index: startIndex,
        channelId: result.record.channelId,
        records: result.record.records,
      });
    }

    if (offset !== messageIndexesView.byteLength) {
      throw new Error(
        `${messageIndexesView.byteLength - offset} bytes remaining in message index section`,
      );
    }
  }
}
