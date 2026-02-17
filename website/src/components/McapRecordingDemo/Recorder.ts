// cspell:word millis

import { Time, fromNanoSec } from "@foxglove/rostime";
import {
  CompressedImage,
  CompressedVideo,
  PoseInFrame,
  RawAudio,
} from "@foxglove/schemas";
import { foxgloveMessageSchemas } from "@foxglove/schemas/internal";
import zstd from "@foxglove/wasm-zstd";
import { McapWriter } from "@mcap/core";
import { EventEmitter } from "eventemitter3";
import Queue from "promise-queue";

import { ProtobufChannelInfo, addProtobufChannel } from "./addProtobufChannel.ts";
import { CompressedVideoFrame } from "./videoCapture.ts";

export type ProtobufObject<Message> = {
  [K in keyof Message]: Message[K] extends { sec: number; nsec: number }
    ? { seconds: number | bigint; nanos: number }
    : Message[K];
};
export function toProtobufTime({ sec, nsec }: Time): {
  seconds: number | bigint;
  nanos: number;
} {
  return { seconds: sec, nanos: nsec };
}

export type MouseEventMessage = {
  clientX: number;
  clientY: number;
};
const MouseEventSchema = {
  type: "object",
  properties: {
    clientX: { type: "number" },
    clientY: { type: "number" },
  },
};

type RecorderEvents = {
  update: () => void;
};

export class Recorder extends EventEmitter<RecorderEvents> {
  #textEncoder = new TextEncoder();
  #writer?: McapWriter;
  /** Used to ensure all operations on the McapWriter are sequential */
  #queue = new Queue(/*maxPendingPromises=*/ 1);
  #audioChannel?: ProtobufChannelInfo;
  #audioChannelSeq = 0;
  #av1Channel?: ProtobufChannelInfo;
  #av1ChannelSeq = 0;
  #h264Channel?: ProtobufChannelInfo;
  #h264ChannelSeq = 0;
  #h265Channel?: ProtobufChannelInfo;
  #h265ChannelSeq = 0;
  #jpegChannel?: ProtobufChannelInfo;
  #jpegChannelSeq = 0;
  #mouseChannelId?: number;
  #mouseChannelSeq = 0;
  #poseChannel?: ProtobufChannelInfo;
  #poseChannelSeq = 0;
  #vp9Channel?: ProtobufChannelInfo;
  #vp9ChannelSeq = 0;

  #blobParts: Uint8Array<ArrayBuffer>[] = [];
  bytesWritten = 0n;
  messageCount = 0n;
  chunkCount = 0;

  constructor() {
    super();
    this.#reinitializeWriter();
  }

  #reinitializeWriter() {
    void this.#queue.add(async () => {
      await zstd.isLoaded;
      this.#blobParts = [];
      this.bytesWritten = 0n;
      this.messageCount = 0n;
      this.chunkCount = 0;
      this.#writer = new McapWriter({
        chunkSize: 5 * 1024,
        compressChunk(data) {
          return { compression: "zstd", compressedData: new Uint8Array(zstd.compress(data)) };
        },
        writable: {
          position: () => this.bytesWritten,
          write: async (buffer: Uint8Array) => {
            this.#blobParts.push(new Uint8Array(buffer));
            this.bytesWritten += BigInt(buffer.byteLength);
            this.#emit();
          },
        },
      });
      await this.#writer.start({
        library: "MCAP web demo",
        profile: "",
      });

      this.#emit();
    });
    // Channels are lazily added later
    this.#audioChannel = undefined;
    this.#audioChannelSeq = 0;
    this.#av1Channel = undefined;
    this.#av1ChannelSeq = 0;
    this.#h264Channel = undefined;
    this.#h264ChannelSeq = 0;
    this.#h265Channel = undefined;
    this.#h265ChannelSeq = 0;
    this.#jpegChannel = undefined;
    this.#jpegChannelSeq = 0;
    this.#mouseChannelId = undefined;
    this.#mouseChannelSeq = 0;
    this.#poseChannel = undefined;
    this.#poseChannelSeq = 0;
  }

  #time(): bigint {
    const milliseconds = +new Date();
    return BigInt(milliseconds) * 1000000n;
  }

  #emit() {
    this.chunkCount = this.#writer?.statistics?.chunkCount ?? 0;
    this.messageCount = this.#writer?.statistics?.messageCount ?? 0n;
    this.emit("update");
  }

  async addMouseEvent(msg: MouseEventMessage): Promise<void> {
    void this.#queue.add(async () => {
      if (!this.#writer) {
        return;
      }
      if (this.#mouseChannelId == undefined) {
        const mouseSchemaId = await this.#writer.registerSchema({
          name: "MouseEvent",
          encoding: "jsonschema",
          data: this.#textEncoder.encode(JSON.stringify(MouseEventSchema)),
        });
        this.#mouseChannelId = await this.#writer.registerChannel({
          topic: "mouse",
          messageEncoding: "json",
          schemaId: mouseSchemaId,
          metadata: new Map(),
        });
      }
      const now = this.#time();
      await this.#writer.addMessage({
        sequence: this.#mouseChannelSeq++,
        channelId: this.#mouseChannelId,
        logTime: now,
        publishTime: now,
        data: this.#textEncoder.encode(JSON.stringify(msg)),
      });
      this.messageCount++;
      this.#emit();
    });
  }

  async addPose(msg: ProtobufObject<PoseInFrame>): Promise<void> {
    void this.#queue.add(async () => {
      if (!this.#writer) {
        return;
      }
      if (!this.#poseChannel) {
        this.#poseChannel = await addProtobufChannel(
          this.#writer,
          "pose",
          foxgloveMessageSchemas.PoseInFrame,
        );
      }
      const now = this.#time();
      const { id, rootType } = this.#poseChannel;
      await this.#writer.addMessage({
        sequence: this.#poseChannelSeq++,
        channelId: id,
        logTime: now,
        publishTime: now,
        data: rootType.encode(msg).finish(),
      });
      this.messageCount++;
      this.#emit();
    });
  }

  async addJpegFrame(blob: Blob): Promise<void> {
    void this.#queue.add(async () => {
      if (!this.#writer) {
        return;
      }
      if (!this.#jpegChannel) {
        this.#jpegChannel = await addProtobufChannel(
          this.#writer,
          "camera_jpeg",
          foxgloveMessageSchemas.CompressedImage,
        );
      }
      const { id, rootType } = this.#jpegChannel;
      const now = this.#time();
      const msg: ProtobufObject<CompressedImage> = {
        timestamp: toProtobufTime(fromNanoSec(now)),
        frame_id: "camera",
        data: new Uint8Array(await blob.arrayBuffer()),
        format: blob.type,
      };
      await this.#writer.addMessage({
        sequence: this.#jpegChannelSeq++,
        channelId: id,
        logTime: now,
        publishTime: now,
        data: rootType.encode(msg).finish(),
      });
      this.messageCount++;
      this.#emit();
    });
  }

  async addVideoFrame(frame: CompressedVideoFrame): Promise<void> {
    void this.#queue.add(async () => {
      if (!this.#writer) {
        return;
      }
      let channel: ProtobufChannelInfo;
      let sequence: number;
      switch (frame.format) {
        case "h264":
          channel = this.#h264Channel ??= await addProtobufChannel(
            this.#writer,
            "camera_h264",
            foxgloveMessageSchemas.CompressedVideo,
          );
          sequence = this.#h264ChannelSeq++;
          break;
        case "h265":
          channel = this.#h265Channel ??= await addProtobufChannel(
            this.#writer,
            "camera_h265",
            foxgloveMessageSchemas.CompressedVideo,
          );
          sequence = this.#h265ChannelSeq++;
          break;
        case "vp9":
          channel = this.#vp9Channel ??= await addProtobufChannel(
            this.#writer,
            "camera_vp9",
            foxgloveMessageSchemas.CompressedVideo,
          );
          sequence = this.#vp9ChannelSeq++;
          break;
        case "av1":
          channel = this.#av1Channel ??= await addProtobufChannel(
            this.#writer,
            "camera_av1",
            foxgloveMessageSchemas.CompressedVideo,
          );
          sequence = this.#av1ChannelSeq++;
          break;
      }
      const { id, rootType } = channel;
      const now = this.#time();
      const msg: ProtobufObject<CompressedVideo> = {
        timestamp: toProtobufTime(fromNanoSec(now)),
        frame_id: "camera",
        data: frame.data,
        format: frame.format,
      };
      const data = rootType.encode(msg).finish();
      frame.release();
      await this.#writer.addMessage({
        sequence,
        channelId: id,
        logTime: now,
        publishTime: now,
        data,
      });
      this.messageCount++;
      this.#emit();
    });
  }

  async addAudioData(data: RawAudio): Promise<void> {
    void this.#queue.add(async () => {
      if (!this.#writer) {
        return;
      }
      const channel = (this.#audioChannel ??= await addProtobufChannel(
        this.#writer,
        "microphone",
        foxgloveMessageSchemas.RawAudio,
      ));
      const sequence = this.#audioChannelSeq++;
      const { id, rootType } = channel;
      const msg: ProtobufObject<RawAudio> = {
        timestamp: toProtobufTime(data.timestamp),
        data: data.data,
        format: data.format,
        sample_rate: data.sample_rate,
        number_of_channels: data.number_of_channels,
      };
      const encodedMsg = rootType.encode(msg).finish();
      const now = this.#time();
      await this.#writer.addMessage({
        sequence,
        channelId: id,
        logTime: now,
        publishTime: now,
        data: encodedMsg,
      });
      this.messageCount++;
      this.#emit();
    });
  }

  async closeAndRestart(): Promise<Blob> {
    return await this.#queue.add(async () => {
      await this.#writer?.end();
      const blob = new Blob(this.#blobParts);
      this.#reinitializeWriter();
      return blob;
    });
  }
}
