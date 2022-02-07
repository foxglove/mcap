import { Schema } from "inspector";

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
export type ChannelInfo = {
  id: number;
  topic: string;
  messageEncoding: string;
  schemaId: number;
  metadata: [key: string, value: string][];
};
export type Message = {
  channelId: number;
  sequence: number;
  publishTime: bigint;
  logTime: bigint;
  data: Uint8Array;
};
export type Chunk = {
  startTime: bigint;
  endTime: bigint;
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
  startTime: bigint;
  endTime: bigint;
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
  createdAt: bigint;
  logTime: bigint;
  contentType: string;
  data: Uint8Array;
};
export type AttachmentIndex = {
  offset: bigint;
  length: bigint;
  logTime: bigint;
  dataSize: bigint;
  name: string;
  contentType: string;
};
export type Statistics = {
  messageCount: bigint;
  channelCount: number;
  attachmentCount: number;
  chunkCount: number;
  channelMessageCounts: Map<number, bigint>;
};
export type Metadata = {
  name: string;
  metadata: [key: string, value: string][];
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
  ChannelInfo: ChannelInfo;
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

export interface McapStreamReader {
  done(): boolean;
  bytesRemaining(): number;
  append(data: Uint8Array): void;
  nextRecord(): TypedMcapRecord | undefined;
}

export type DecompressHandlers = {
  [compression: string]: (buffer: Uint8Array, decompressedSize: bigint) => Uint8Array;
};
