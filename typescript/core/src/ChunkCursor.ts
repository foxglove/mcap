import McapByteReader from "./McapByteReader.ts";
import { Opcode } from "./constants.ts";
import { parseRecord } from "./parse.ts";
import { sortedIndexBy } from "./sortedIndexBy.ts";
import { sortedLastIndexBy } from "./sortedLastIndex.ts";
import type { IReadable, TypedMcapRecords } from "./types.ts";

type MessageOffset = [logTime: bigint, offset: bigint, channelId: number];

type ChunkCursorParams = {
  chunkIndex: TypedMcapRecords["ChunkIndex"];
  relevantChannels: Set<number> | undefined;
  startingAt: bigint | undefined;
  endingAt: bigint | undefined;
  reverse: boolean;
  /**
   * When true, `loadMessageIndexes()` reads the full message index region for the chunk (not just
   * the portion containing `relevantChannels`). This is useful when the underlying readable caches
   * reads, since it keeps the read offset/size stable across calls with different topic filters.
   */
  readFullMessageIndexRange?: boolean;
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
  #startingAt: bigint | undefined;
  #endingAt: bigint | undefined;
  #reverse: boolean;
  #readFullMessageIndexRange: boolean;

  // List of message offsets (across all channels) sorted by logTime.
  #orderedMessageOffsets?: MessageOffset[];
  // Index for the next message offset. Gets incremented for every popMessage() call.
  #nextMessageOffsetIndex = 0;

  constructor(params: ChunkCursorParams) {
    this.chunkIndex = params.chunkIndex;
    this.#relevantChannels = params.relevantChannels;
    this.#startingAt = params.startingAt;
    this.#endingAt = params.endingAt;
    this.#reverse = params.reverse;
    this.#readFullMessageIndexRange = params.readFullMessageIndexRange ?? false;
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
  popMessage(): MessageOffset {
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
   * Returns true if message offsets have been loaded, false if `loadMessageIndexes()` or
   * `loadMessageIndexesFromChunk()` needs to be called.
   */
  hasMessageIndexes(): boolean {
    return this.#orderedMessageOffsets != undefined;
  }

  /**
   * Returns true if the chunk is scanned for its messages: it has no Message Index records, or
   * every message in it is wanted.
   */
  needsChunkScan(): boolean {
    const { messageIndexOffsets, messageIndexLength } = this.chunkIndex;
    if (messageIndexLength === 0n || messageIndexOffsets.size === 0) {
      return true;
    }
    if (this.#relevantChannels) {
      for (const channelId of messageIndexOffsets.keys()) {
        if (!this.#relevantChannels.has(channelId)) {
          return false;
        }
      }
    }
    return (
      (this.#startingAt == undefined || this.#startingAt <= this.chunkIndex.messageStartTime) &&
      (this.#endingAt == undefined || this.#endingAt >= this.chunkIndex.messageEndTime)
    );
  }

  /**
   * Finds the chunk's messages by scanning its uncompressed records. This stands in for
   * `loadMessageIndexes()` when `needsChunkScan()` is true.
   */
  loadMessageIndexesFromChunk(chunkView: DataView): void {
    const messageOffsets: MessageOffset[] = [];
    const chunkLength = chunkView.byteLength;
    // Message fields before the data: channel id (2), sequence (4), log time (8), publish time (8).
    const messageFieldsLength = 2 + 4 + 8 + 8;
    let offset = 0;
    while (offset < chunkLength) {
      const recordStart = offset + 1 + 8;
      if (recordStart > chunkLength) {
        throw new Error(
          `Truncated record header at offset ${offset} in chunk at offset ${this.chunkIndex.chunkStartOffset}`,
        );
      }
      // The length is read in two halves to keep the scan free of BigInts.
      const recordLength = chunkView.getUint32(offset + 1, true);
      if (chunkView.getUint32(offset + 5, true) !== 0 || recordLength > chunkLength - recordStart) {
        const fullLength = chunkView.getBigUint64(offset + 1, true);
        throw new Error(
          `Record at offset ${offset} with length ${fullLength} extends beyond chunk bounds (chunk data length ${chunkLength}) in chunk at offset ${this.chunkIndex.chunkStartOffset}`,
        );
      }
      const opcode: Opcode = chunkView.getUint8(offset);
      if (opcode === Opcode.MESSAGE) {
        if (recordLength < messageFieldsLength) {
          throw new Error(
            `Message record at offset ${offset} is too short (${recordLength} bytes) in chunk at offset ${this.chunkIndex.chunkStartOffset}`,
          );
        }
        const channelId = chunkView.getUint16(recordStart, true);
        if (!this.#relevantChannels || this.#relevantChannels.has(channelId)) {
          messageOffsets.push([
            chunkView.getBigUint64(recordStart + 2 + 4, true),
            BigInt(offset),
            channelId,
          ]);
        }
      }
      offset = recordStart + recordLength;
    }
    this.#setMessageOffsets(messageOffsets);
  }

  async loadMessageIndexes<TReadOptions>(
    readable: IReadable<TReadOptions>,
    readOptions?: TReadOptions,
  ): Promise<void> {
    let messageIndexStartOffset: bigint | undefined;
    let relevantMessageIndexStartOffset: bigint | undefined;
    const readFullRange = this.#readFullMessageIndexRange;

    for (const [channelId, offset] of this.chunkIndex.messageIndexOffsets) {
      if (messageIndexStartOffset == undefined || offset < messageIndexStartOffset) {
        messageIndexStartOffset = offset;
      }
      if (readFullRange || !this.#relevantChannels || this.#relevantChannels.has(channelId)) {
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
      readOptions,
    );
    const messageIndexesView = new DataView(
      messageIndexes.buffer,
      messageIndexes.byteOffset,
      messageIndexes.byteLength,
    );

    const reader = new McapByteReader(messageIndexesView);
    const messageOffsets: MessageOffset[] = [];
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

      for (const [logTime, offset] of record.records) {
        messageOffsets.push([logTime, offset, record.channelId]);
      }
    }

    if (reader.bytesRemaining() !== 0) {
      throw new Error(`${reader.bytesRemaining()} bytes remaining in message index section`);
    }

    this.#setMessageOffsets(messageOffsets);
  }

  /**
   * Orders the offsets of the chunk's relevant messages for iteration and drops those outside
   * the requested time range.
   */
  #setMessageOffsets(messageOffsets: MessageOffset[]): void {
    const reverse = this.#reverse;
    this.#orderedMessageOffsets = messageOffsets.sort(
      ([logTimeA, offsetA], [logTimeB, offsetB]) => {
        let diff = Number(logTimeA - logTimeB);

        // Break ties by message offset in the file
        if (diff === 0) {
          diff = Number(offsetA - offsetB);
        }

        return diff;
      },
    );

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
    const startingAt = reverse ? this.#endingAt : this.#startingAt;
    const endingAt = reverse ? this.#startingAt : this.#endingAt;
    const iteratee = reverse ? (logTime: bigint) => -logTime : (logTime: bigint) => logTime;
    let startIndex: number | undefined;
    let endIndex: number | undefined;

    if (startingAt != undefined) {
      startIndex = sortedIndexBy(this.#orderedMessageOffsets, startingAt, iteratee);
    }
    if (endingAt != undefined) {
      endIndex = sortedLastIndexBy(this.#orderedMessageOffsets, endingAt, iteratee);
    }

    // Remove offsets whose log time is outside of the range [startingAt, endingAt] which
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
