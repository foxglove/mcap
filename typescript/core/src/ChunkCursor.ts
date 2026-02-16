import Reader from "./Reader.ts";
import { parseRecord } from "./parse.ts";
import { sortedIndexBy } from "./sortedIndexBy.ts";
import { sortedLastIndexBy } from "./sortedLastIndex.ts";
import type { IReadable, TypedMcapRecords } from "./types.ts";

type ChunkCursorParams = {
  chunkIndex: TypedMcapRecords["ChunkIndex"];
  relevantChannels: Set<number> | undefined;
  startTime: bigint | undefined;
  endTime: bigint | undefined;
  reverse: boolean;
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

  #relevantChannels?: Set<number>;
  #startTime: bigint | undefined;
  #endTime: bigint | undefined;
  #reverse: boolean;

  // List of message offsets (across all channels) sorted by logTime.
  #orderedMessageOffsets?: [logTime: bigint, offset: bigint][];
  // Index for the next message offset. Gets incremented for every popMessage() call.
  #nextMessageOffsetIndex = 0;

  constructor(params: ChunkCursorParams) {
    this.chunkIndex = params.chunkIndex;
    this.#relevantChannels = params.relevantChannels;
    this.#startTime = params.startTime;
    this.#endTime = params.endTime;
    this.#reverse = params.reverse;

    if (this.chunkIndex.messageIndexLength === 0n) {
      // Chunk has no message indexes.
      // We only allow that if the chunk has no messages and the start and end times are 0.
      if (this.chunkIndex.messageStartTime !== 0n || this.chunkIndex.messageEndTime !== 0n) {
        throw new Error(
          `Encountered a chunk index without message indexes and non-zero start and end times`,
        );
      }
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
    if (this.#reverse !== other.#reverse) {
      throw new Error("Cannot compare a reversed ChunkCursor to a non-reversed ChunkCursor");
    }

    let diff = Number(this.#getSortTime() - other.#getSortTime());

    // Break ties by chunk offset in the file
    if (diff === 0) {
      diff = Number(this.chunkIndex.chunkStartOffset - other.chunkIndex.chunkStartOffset);
    }

    return this.#reverse ? -diff : diff;
  }

  /**
   * Returns true if there are more messages available in the chunk. Message indexes must have been
   * loaded before using this method.
   */
  hasMoreMessages(): boolean {
    if (this.#orderedMessageOffsets == undefined) {
      throw new Error("loadMessageIndexes() must be called before hasMore()");
    }
    return this.#nextMessageOffsetIndex < this.#orderedMessageOffsets.length;
  }

  /**
   * Pop a message offset off of the chunk cursor. Message indexes must have been loaded before
   * using this method.
   */
  popMessage(): [logTime: bigint, offset: bigint] {
    if (this.#orderedMessageOffsets == undefined) {
      throw new Error("loadMessageIndexes() must be called before popMessage()");
    }
    if (this.#nextMessageOffsetIndex >= this.#orderedMessageOffsets.length) {
      throw new Error(
        `Unexpected popMessage() call when no more messages are available, in chunk at offset ${this.chunkIndex.chunkStartOffset}`,
      );
    }

    return this.#orderedMessageOffsets[this.#nextMessageOffsetIndex++]!;
  }

  /**
   * Returns true if message indexes have been loaded, false if `loadMessageIndexes()` needs to be
   * called.
   */
  hasMessageIndexes(): boolean {
    return this.#orderedMessageOffsets != undefined;
  }

  async loadMessageIndexes(readable: IReadable): Promise<void> {
    const reverse = this.#reverse;
    let messageIndexStartOffset: bigint | undefined;
    let relevantMessageIndexStartOffset: bigint | undefined;

    for (const [channelId, offset] of this.chunkIndex.messageIndexOffsets) {
      if (messageIndexStartOffset == undefined || offset < messageIndexStartOffset) {
        messageIndexStartOffset = offset;
      }
      if (!this.#relevantChannels || this.#relevantChannels.has(channelId)) {
        if (
          relevantMessageIndexStartOffset == undefined ||
          offset < relevantMessageIndexStartOffset
        ) {
          relevantMessageIndexStartOffset = offset;
        }
      }
    }
    if (messageIndexStartOffset == undefined || relevantMessageIndexStartOffset == undefined) {
      this.#orderedMessageOffsets = [];
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

    const reader = new Reader(messageIndexesView);
    const arrayOfMessageOffsets: [logTime: bigint, offset: bigint][][] = [];
    let record;
    while ((record = parseRecord(reader, true))) {
      if (record.type !== "MessageIndex") {
        continue;
      }
      if (
        record.records.length === 0 ||
        (this.#relevantChannels && !this.#relevantChannels.has(record.channelId))
      ) {
        continue;
      }

      arrayOfMessageOffsets.push(record.records);
    }

    if (reader.bytesRemaining() !== 0) {
      throw new Error(`${reader.bytesRemaining()} bytes remaining in message index section`);
    }

    this.#orderedMessageOffsets = arrayOfMessageOffsets
      .flat()
      .sort(([logTimeA, offsetA], [logTimeB, offsetB]) => {
        let diff = Number(logTimeA - logTimeB);

        // Break ties by message offset in the file
        if (diff === 0) {
          diff = Number(offsetA - offsetB);
        }

        return diff;
      });

    if (reverse) {
      // If we used `logTimeB - logTimeA` as the comparator for reverse iteration, messages with
      // the same timestamp would not be in reverse order. To avoid this problem we use reverse()
      // instead.
      this.#orderedMessageOffsets.reverse();
    }

    if (this.#orderedMessageOffsets.length === 0) {
      return;
    }

    const [logTimeFirstMessage] = this.#orderedMessageOffsets[0]!;
    if (logTimeFirstMessage < this.chunkIndex.messageStartTime) {
      throw new Error(
        `Chunk at offset ${this.chunkIndex.chunkStartOffset} contains a message with logTime (${logTimeFirstMessage}) earlier than chunk messageStartTime (${this.chunkIndex.messageStartTime})`,
      );
    }

    const [logTimeLastMessage] =
      this.#orderedMessageOffsets[this.#orderedMessageOffsets.length - 1]!;
    if (logTimeLastMessage > this.chunkIndex.messageEndTime) {
      throw new Error(
        `Chunk at offset ${this.chunkIndex.chunkStartOffset} contains a message with logTime (${logTimeLastMessage}) later than chunk messageEndTime (${this.chunkIndex.messageEndTime})`,
      );
    }

    // Determine the indexes corresponding to the start and end time.
    const startTime = reverse ? this.#endTime : this.#startTime;
    const endTime = reverse ? this.#startTime : this.#endTime;
    const iteratee = reverse ? (logTime: bigint) => -logTime : (logTime: bigint) => logTime;
    let startIndex: number | undefined;
    let endIndex: number | undefined;

    if (startTime != undefined) {
      startIndex = sortedIndexBy(this.#orderedMessageOffsets, startTime, iteratee);
    }
    if (endTime != undefined) {
      endIndex = sortedLastIndexBy(this.#orderedMessageOffsets, endTime, iteratee);
    }

    // Remove offsets whose log time is outside of the range [startTime, endTime] which
    // avoids having to do additional book-keep of additional array start & stop indexes.
    if (startIndex != undefined || endIndex != undefined) {
      this.#orderedMessageOffsets = this.#orderedMessageOffsets.slice(startIndex, endIndex);
    }
  }

  // Get the next available message logTime which is being used when comparing chunkCursors (for ordering purposes).
  #getSortTime(): bigint {
    // If message indexes have been loaded and are non-empty, we return the logTime of the next available message.
    if (
      this.#orderedMessageOffsets != undefined &&
      this.#orderedMessageOffsets.length > 0 &&
      this.#nextMessageOffsetIndex < this.#orderedMessageOffsets.length
    ) {
      return this.#orderedMessageOffsets[this.#nextMessageOffsetIndex]![0];
    }

    // Fall back to the chunk index' start time or end time.
    return this.#reverse ? this.chunkIndex.messageEndTime : this.chunkIndex.messageStartTime;
  }
}
