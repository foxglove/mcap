import { parseRecord } from "./parse";
import { sortedIndexBy } from "./sortedIndexBy";
import { IReadable, TypedMcapRecords } from "./types";

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
  #orderedMessageOffset?: [logTime: bigint, offset: bigint][];
  // Index for the next message offset. Gets incremented for every popMessage() call.
  #nextMessageOffsetIndex = 0;
  // If endTime is specified, this corresponds to the index of the first message that is not within the [startTime, endTime] range.
  #messageOffsetEndIndex?: number;

  constructor(params: ChunkCursorParams) {
    this.chunkIndex = params.chunkIndex;
    this.#relevantChannels = params.relevantChannels;
    this.#startTime = params.startTime;
    this.#endTime = params.endTime;
    this.#reverse = params.reverse;

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
    if (this.#orderedMessageOffset == undefined) {
      throw new Error("loadMessageIndexes() must be called before hasMore()");
    }
    return (
      this.#nextMessageOffsetIndex <
      (this.#messageOffsetEndIndex ?? this.#orderedMessageOffset.length)
    );
  }

  /**
   * Pop a message offset off of the chunk cursor. Message indexes must have been loaded before
   * using this method.
   */
  popMessage(): [logTime: bigint, offset: bigint] {
    if (this.#orderedMessageOffset == undefined) {
      throw new Error("loadMessageIndexes() must be called before popMessage()");
    }
    if (
      this.#nextMessageOffsetIndex >=
      (this.#messageOffsetEndIndex ?? this.#orderedMessageOffset.length)
    ) {
      throw new Error(
        `Unexpected popMessage() call when no more messages are available, in chunk at offset ${this.chunkIndex.chunkStartOffset}`,
      );
    }

    return this.#orderedMessageOffset[this.#nextMessageOffsetIndex++]!;
  }

  /**
   * Returns true if message indexes have been loaded, false if `loadMessageIndexes()` needs to be
   * called.
   */
  hasMessageIndexes(): boolean {
    return this.#orderedMessageOffset != undefined;
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

    this.#orderedMessageOffset = [];
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
        (this.#relevantChannels && !this.#relevantChannels.has(result.record.channelId))
      ) {
        continue;
      }

      this.#orderedMessageOffset = this.#orderedMessageOffset.concat(result.record.records);
    }

    if (offset !== messageIndexesView.byteLength) {
      throw new Error(
        `${messageIndexesView.byteLength - offset} bytes remaining in message index section`,
      );
    }

    if (this.#orderedMessageOffset.length === 0) {
      return;
    }

    this.#orderedMessageOffset.sort(([logTimeA], [logTimeB]) => Number(logTimeA - logTimeB));
    if (reverse) {
      // If we used `logTimeB - logTimeA` as the comparator for reverse iteration, messages with
      // the same timestamp would not be in reverse order. To avoid this problem we use reverse()
      // instead.
      this.#orderedMessageOffset.reverse();
    }

    const [logTimeFirstMessage] = this.#orderedMessageOffset[0]!;
    if (logTimeFirstMessage < this.chunkIndex.messageStartTime) {
      throw new Error(
        `Chunk contains a message with logTime (${logTimeFirstMessage}) earlier than chunk messageStartTime (${this.chunkIndex.messageStartTime}) in chunk at offset ${this.chunkIndex.chunkStartOffset}`,
      );
    }

    const [logTimeLastMessage] = this.#orderedMessageOffset[this.#orderedMessageOffset.length - 1]!;
    if (logTimeLastMessage > this.chunkIndex.messageEndTime) {
      throw new Error(
        `Chunk contains a message with logTime with logTime (${logTimeLastMessage}) later than chunk messageEndTime (${this.chunkIndex.messageEndTime}) in chunk at offset ${this.chunkIndex.chunkStartOffset}`,
      );
    }

    // Determine the indexes corresponding to the start and end time.
    const startTime = reverse ? this.#endTime : this.#startTime;
    const endTime = reverse ? this.#startTime : this.#endTime;
    const iteratee = (logTime: bigint) => (reverse ? -logTime : logTime);
    if (startTime != undefined) {
      this.#nextMessageOffsetIndex = sortedIndexBy(this.#orderedMessageOffset, startTime, iteratee);
    }
    if (endTime != undefined) {
      this.#messageOffsetEndIndex = sortedIndexBy(this.#orderedMessageOffset, endTime, iteratee);
      // sortedIndexBy returns the minimum index but for the end index we actually want the highest index since
      // endTime is inclusive. So we count up the end index manually until we reach a logTime that is not included anymore.
      while (this.#messageOffsetEndIndex < this.#orderedMessageOffset.length) {
        const logTime = this.#orderedMessageOffset[this.#messageOffsetEndIndex]![0];
        if (reverse ? logTime < endTime : logTime > endTime) {
          break;
        }
        this.#messageOffsetEndIndex++;
      }
    }
  }

  // Get the next available message logTime which is being used when comparing chunkCursors (for ordering purposes).
  #getSortTime(): bigint {
    // If message indexes have been loaded and are non-empty, we return the logTime of the next available message.
    if (
      this.#orderedMessageOffset != undefined &&
      this.#orderedMessageOffset.length > 0 &&
      this.#nextMessageOffsetIndex <
        (this.#messageOffsetEndIndex ?? this.#orderedMessageOffset.length)
    ) {
      return this.#orderedMessageOffset[this.#nextMessageOffsetIndex]![0];
    }

    // Fall back to the chunk index' start time or end time.
    return this.#reverse ? this.chunkIndex.messageEndTime : this.chunkIndex.messageStartTime;
  }
}
