export type McapMagic = {
  type: "Magic";
  specVersion: "0";
};
export type Header = {
  profile: string;
  library: string;
  metadata: [key: string, value: string][];
};
export type Footer = {
  indexOffset: bigint;
  indexCrc: number;
};
export type ChannelInfo = {
  channelId: number;
  topicName: string;
  encoding: string;
  schemaName: string;
  schema: string;
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
  chunkOffset: bigint;
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
export type UnknownRecord = {
  type: "Unknown";
  opcode: number;
  data: Uint8Array;
};

export type McapRecord =
  | Header
  | Footer
  | ChannelInfo
  | Message
  | Chunk
  | MessageIndex
  | ChunkIndex
  | Attachment
  | AttachmentIndex
  | Statistics
  | UnknownRecord;

export interface McapStreamReader {
  done(): boolean;
  bytesRemaining(): number;
  append(data: Uint8Array): void;
  nextRecord(): McapRecord | undefined;
}
