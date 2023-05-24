import { Time, fromNanoSec } from "@foxglove/rostime";
import { PoseInFrame, CompressedImage } from "@foxglove/schemas";
import { foxgloveMessageSchemas } from "@foxglove/schemas/internal";
import zstd from "@foxglove/wasm-zstd";
import { McapWriter } from "@mcap/core";
import { EventEmitter } from "eventemitter3";
import Queue from "promise-queue";
import { ProtobufChannelInfo, addProtobufChannel } from "./addProtobufChannel";

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
  #mouseChannelId?: Promise<number>;
  #mouseChannelSeq = 0;
  #poseChannel?: Promise<ProtobufChannelInfo>;
  #poseChannelSeq = 0;
  #cameraChannel?: Promise<ProtobufChannelInfo>;
  #cameraChannelSeq = 0;

  #blobParts: Uint8Array[] = [];
  bytesWritten = 0n;
  messageCount = 0;
  chunkCount = 0;

  constructor() {
    super();
    this.#reinitializeWriter();
  }

  #reinitializeWriter() {
    const promise = this.#queue.add(async () => {
      await zstd.isLoaded;
      this.#blobParts = [];
      this.bytesWritten = 0n;
      this.messageCount = 0;
      this.chunkCount = 0;
      this.#writer = new McapWriter({
        chunkSize: 5 * 1024,
        compressChunk(data) {
          return { compression: "zstd", compressedData: zstd.compress(data) };
        },
        writable: {
          position: () => this.bytesWritten,
          write: async (buffer: Uint8Array) => {
            this.#blobParts.push(buffer);
            this.bytesWritten += BigInt(buffer.byteLength);
            this.chunkCount++;
            this.#emit();
          },
        },
      });
      await this.#writer.start({
        library: "MCAP web demo",
        profile: "",
      });

      const mouseSchemaId = await this.#writer.registerSchema({
        name: "MouseEvent",
        encoding: "jsonschema",
        data: this.#textEncoder.encode(JSON.stringify(MouseEventSchema)),
      });
      const mouseChannelId = await this.#writer.registerChannel({
        topic: "mouse",
        messageEncoding: "json",
        schemaId: mouseSchemaId,
        metadata: new Map(),
      });

      const poseChannel = await addProtobufChannel(
        this.#writer,
        "pose",
        foxgloveMessageSchemas.PoseInFrame
      );
      const cameraChannel = await addProtobufChannel(
        this.#writer,
        "camera",
        foxgloveMessageSchemas.CompressedImage
      );

      this.#emit();
      return { mouseChannelId, poseChannel, cameraChannel };
    });
    this.#mouseChannelId = promise.then(({ mouseChannelId }) => mouseChannelId);
    this.#poseChannel = promise.then(({ poseChannel }) => poseChannel);
    this.#cameraChannel = promise.then(({ cameraChannel }) => cameraChannel);
  }

  #time(): bigint {
    const milliseconds = +new Date();
    return BigInt(milliseconds) * 1000000n;
  }

  #emit() {
    this.emit("update");
  }

  async addMouseEvent(msg: MouseEventMessage): Promise<void> {
    void this.#queue.add(async () => {
      if (!this.#writer || !this.#mouseChannelId) {
        return;
      }
      const now = this.#time();
      await this.#writer.addMessage({
        sequence: this.#mouseChannelSeq++,
        channelId: await this.#mouseChannelId,
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
      if (!this.#writer || !this.#poseChannel) {
        return;
      }
      const now = this.#time();
      const { id, rootType } = await this.#poseChannel;
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

  async addCameraImage(blob: Blob): Promise<void> {
    void this.#queue.add(async () => {
      if (!this.#writer || !this.#cameraChannel) {
        return;
      }
      const { id, rootType } = await this.#cameraChannel;
      const now = this.#time();
      const msg: ProtobufObject<CompressedImage> = {
        timestamp: toProtobufTime(fromNanoSec(now)),
        frame_id: "camera",
        data: new Uint8Array(await blob.arrayBuffer()),
        format: blob.type,
      };
      await this.#writer.addMessage({
        sequence: this.#cameraChannelSeq++,
        channelId: id,
        logTime: now,
        publishTime: now,
        data: rootType.encode(msg).finish(),
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
