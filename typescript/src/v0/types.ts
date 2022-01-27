export type McapMagic = {
  specVersion: "0";
};
export type Header = {
  profile: string;
  library: string;
  metadata: [key: string, value: string][];
};
export type Footer = {
  summaryStart: bigint;
  summaryOffsetStart: bigint;
  crc: number;
};
export type ChannelInfo = {
  channelId: number;
  topicName: string;
  messageEncoding: string;
  schemaFormat: string;
  schema: string;
  schemaName: string;
  userData: [key: string, value: string][];
};
export type Message = {
  channelId: number;
  sequence: number;
  publishTime: bigint;
  recordTime: bigint;
  messageData: Uint8Array;
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
  count: number;
  records: [recordTime: bigint, offset: bigint][];
};
export type ChunkIndex = {
  startTime: bigint;
  endTime: bigint;
  chunkStart: bigint;
  chunkLength: bigint;
  messageIndexOffsets: Map<number, bigint>;
  messageIndexLength: bigint;
  compression: string;
  compressedSize: bigint;
  uncompressedSize: bigint;
};
export type Attachment = {
  name: string;
  recordTime: bigint;
  contentType: string;
  data: Uint8Array;
};
export type AttachmentIndex = {
  recordTime: bigint;
  attachmentSize: bigint;
  name: string;
  contentType: string;
  offset: bigint;
};
export type Statistics = {
  messageCount: bigint;
  channelCount: number;
  attachmentCount: number;
  chunkCount: number;
  channelMessageCounts: Map<number, bigint>;
};
export type Metadata = {
  metadata: [key: string, value: string][];
};
export type MetadataIndex = {
  offset: bigint;
  length: bigint;
};
export type SummaryOffset = {
  groupOpcode: number;
  groupStart: bigint;
  groupLength: bigint;
};
export type UnknownRecord = {
  opcode: number;
  data: Uint8Array;
};

export type McapRecords = {
  Header: Header;
  Footer: Footer;
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
