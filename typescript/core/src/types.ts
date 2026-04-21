export type McapMagic = {
  specVersion: "0";
};
export type Header = {
  profile: string;
  library: string;
};
export type Footer = {
  summaryStart: bigint;
  summaryOffsetStart: bigint;
  summaryCrc: number;
};
export type Schema = {
  id: number;
  name: string;
  encoding: string;
  data: Uint8Array;
};
export type Channel = {
  id: number;
  schemaId: number;
  topic: string;
  messageEncoding: string;
  metadata: Map<string, string>;
};
export type Message = {
  channelId: number;
  sequence: number;
  logTime: bigint;
  publishTime: bigint;
  data: Uint8Array;
};
export type Chunk = {
  messageStartTime: bigint;
  messageEndTime: bigint;
  uncompressedSize: bigint;
  uncompressedCrc: number;
  compression: string;
  records: Uint8Array;
};
export type MessageIndex = {
  channelId: number;
  records: [logTime: bigint, offset: bigint][];
};
export type ChunkIndex = {
  messageStartTime: bigint;
  messageEndTime: bigint;
  chunkStartOffset: bigint;
  chunkLength: bigint;
  messageIndexOffsets: Map<number, bigint>;
  messageIndexLength: bigint;
  compression: string;
  compressedSize: bigint;
  uncompressedSize: bigint;
};
export type Attachment = {
  name: string;
  logTime: bigint;
  createTime: bigint;
  mediaType: string;
  data: Uint8Array;
};
export type AttachmentIndex = {
  offset: bigint;
  length: bigint;
  logTime: bigint;
  createTime: bigint;
  dataSize: bigint;
  name: string;
  mediaType: string;
};
export type Statistics = {
  messageCount: bigint;
  schemaCount: number;
  channelCount: number;
  attachmentCount: number;
  metadataCount: number;
  chunkCount: number;
  messageStartTime: bigint;
  messageEndTime: bigint;
  channelMessageCounts: Map<number, bigint>;
};
export type Metadata = {
  name: string;
  metadata: Map<string, string>;
};
export type MetadataIndex = {
  offset: bigint;
  length: bigint;
  name: string;
};
export type SummaryOffset = {
  groupOpcode: number;
  groupStart: bigint;
  groupLength: bigint;
};
export type DataEnd = {
  dataSectionCrc: number;
};
export type UnknownRecord = {
  opcode: number;
  data: Uint8Array;
};

export type McapRecords = {
  Header: Header;
  Footer: Footer;
  Schema: Schema;
  Channel: Channel;
  Message: Message;
  Chunk: Chunk;
  MessageIndex: MessageIndex;
  ChunkIndex: ChunkIndex;
  Attachment: Attachment;
  AttachmentIndex: AttachmentIndex;
  Statistics: Statistics;
  Metadata: Metadata;
  MetadataIndex: MetadataIndex;
  SummaryOffset: SummaryOffset;
  DataEnd: DataEnd;
  Unknown: UnknownRecord;
};

export type TypedMcapRecords = {
  [R in keyof McapRecords]: McapRecords[R] & { type: R };
};

type Values<T> = T[keyof T];
export type TypedMcapRecord = Values<TypedMcapRecords>;
export type McapRecord = Values<McapRecords>;

export type DecompressHandlers = {
  [compression: string]: (buffer: Uint8Array, decompressedSize: bigint) => Uint8Array;
};

/**
 * IReadable describes a random-access reader interface.
 */
export interface IReadable {
  size(): Promise<bigint>;
  read(offset: bigint, size: bigint): Promise<Uint8Array>;

  /**
   * Optional capability flag advertised by implementations whose `read()` is safe to invoke
   * concurrently (e.g. via `Promise.all`).
   *
   * When `true`, consumers may:
   * - Issue multiple `read()` calls without awaiting each one sequentially.
   * - Retain the returned `Uint8Array` across subsequent `read()` calls; the bytes will not be
   *   mutated or aliased by any later read.
   *
   * When omitted or `false`, consumers MUST:
   * - Serialize `read()` calls (await each one before issuing the next).
   * - Treat the returned `Uint8Array` as valid only until the next `read()` call — copy the bytes
   *   out before the next read if they need to outlive it.
   *
   * Only used by McapIndexedReader to optimize prefetched message index reads when `prefetchMessageIndexes` is `true`.
   *
   * Defaults to `false` (i.e. omitted) for safety; existing implementations that reuse an internal
   * buffer do not need to opt in.
   */
  readonly supportsConcurrentReads?: boolean;
}
