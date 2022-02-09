export type McapMagic = {
  type: "Magic";
  formatVersion: 1;
};
export type Channel = {
  type: "Channel";
  id: number;
  topic: string;
  encoding: string;
  schemaName: string;
  schema: string;
  data: ArrayBuffer;
};
export type Message = {
  type: "Message";
  channel: Channel;
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

export type McapRecord = Channel | Message | Chunk | Footer;
