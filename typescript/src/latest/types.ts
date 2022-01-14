export type McapMagic = {
  type: "Magic";
  specVersion: "0";
};
export type Header = {
  type: "Header";
  profile: string;
  library: string;
  metadata: [key: string, value: string][];
};
export type Footer = {
  type: "Footer";
  indexOffset: bigint;
  indexCrc: number;
};
export type ChannelInfo = {
  type: "ChannelInfo";
  channelId: number;
  topicName: string;
  encoding: string;
  schemaName: string;
  schema: string;
  userData: [key: string, value: string][];
};
export type Message = {
  type: "Message";
  channelInfo: ChannelInfo;
  sequence: number;
  publishTime: bigint;
  recordTime: bigint;
  messageData: ArrayBuffer;
};
export type Chunk = {
  type: "Chunk";
  uncompressedSize: bigint;
  uncompressedCrc: number;
  compression: string;
  records: ArrayBuffer;
};
export type MessageIndex = {
  type: "MessageIndex";
  channelId: number;
  count: number;
  records: [recordTime: bigint, offset: bigint][];
};
export type ChunkIndex = {
  type: "ChunkIndex";
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
  type: "Attachment";
  name: string;
  recordTime: bigint;
  contentType: string;
  data: ArrayBuffer;
};
export type AttachmentIndex = {
  type: "AttachmentIndex";
  recordTime: bigint;
  attachmentSize: bigint;
  name: string;
  contentType: string;
  offset: bigint;
};
export type Statistics = {
  type: "Statistics";
  messageCount: bigint;
  channelCount: number;
  attachmentCount: number;
  chunkCount: number;
  channelMessageCounts: Map<number, bigint>;
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
  | Statistics;

export interface McapLatestStreamReader {
  done(): boolean;
  bytesRemaining(): number;
  append(data: Uint8Array): void;
  nextRecord(): McapRecord | undefined;
}
