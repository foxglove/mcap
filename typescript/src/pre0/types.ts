export type McapMagic = {
  type: "Magic";
  formatVersion: 1;
};
export type ChannelInfo = {
  type: "ChannelInfo";
  id: number;
  topic: string;
  encoding: string;
  schemaName: string;
  schema: string;
  data: ArrayBuffer;
};
export type Message = {
  type: "Message";
  channelInfo: ChannelInfo;
  timestamp: bigint;
  data: ArrayBuffer;
};
export type Chunk = {
  type: "Chunk";
  compression: string;
  decompressedSize: bigint;
  decompressedCrc: number;
  data: ArrayBuffer;
};
export type Footer = {
  type: "Footer";
  indexPos: bigint;
  indexCrc: number;
};

export type McapRecord = ChannelInfo | Message | Chunk | Footer;
