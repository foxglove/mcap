import { LogLevel as FoxLevel } from "@foxglove/schemas";
import {
  MessageDefinition,
  Subscription,
  Field,
  ULog,
  ParsedMessage,
  MessageType,
  LogLevel,
} from "@foxglove/ulog";
import { FileReader } from "@foxglove/ulog/node";
import { McapIndexedReader, TempBuffer, McapWriter } from "@mcap/core";
import { Metadata } from "@mcap/core/src/types";
import { protobufFromBinaryDescriptor } from "@mcap/support";
import Long from "long";

import { convertULogFileToMCAP } from "./convertULogFileToMCAP.ts";

type MockMessage = ParsedMessage & { topic: string; multiId?: number };

function createULogMock({
  messageFields,
  subscriptions,
  timestamp = 0n,
  messages = [],
}: {
  messageFields: Map<string, Field[]>;
  subscriptions: { name: string; multiId?: number }[];
  timestamp?: bigint;
  messages?: MockMessage[];
}): jest.Mocked<ULog> {
  const msgIds = new Map<string, number>();
  const definitions = new Map<string, MessageDefinition>();
  messageFields.forEach((fields, name) => {
    definitions.set(name, {
      name,
      fields,
      format: "not read",
    } as MessageDefinition);
  });
  const subscriptionMap = new Map<number, Subscription>();
  subscriptions.forEach((subscription, index) => {
    msgIds.set(`${subscription.name}/${subscription.multiId ?? 0}`, index + 1);
    subscriptionMap.set(index + 1, {
      multiId: subscription.multiId ?? 0,
      ...definitions.get(subscription.name)!,
    });
  });
  const ulogMock: jest.Mocked<ULog> = {
    open: jest.fn().mockResolvedValue(undefined),
    header: {
      timestamp,
      definitions,
      version: 1,
    },
    subscriptions: subscriptionMap,
    readMessages: jest.fn().mockImplementation(async function* () {
      for (const msg of messages) {
        const { topic, multiId, ...messageContent } = msg;
        yield {
          type: MessageType.Data,
          msgId: msgIds.get(`${topic}/${multiId ?? 0}`)!,
          value: messageContent,
        };
      }
    }),
  } as unknown as jest.Mocked<ULog>;
  return ulogMock;
}

describe("Create MCAP files from ULog", () => {
  describe("Mocked MCAP File Writes", () => {
    const topicFixture = new Map([
      [
        "sensor_data",
        [
          { name: "timestamp", type: "uint64_t", isComplex: false },
          { name: "value", type: "float", isComplex: false },
        ],
      ],
      [
        "item_definition",
        [
          { name: "enabled", type: "bool", isComplex: false },
          { name: "matrix", type: "float", arrayLength: 4, isComplex: false },
        ],
      ],
      ["item_list", [{ name: "items", type: "item_definition", arrayLength: 2, isComplex: true }]],
    ]);

    const messageFixture = [
      {
        topic: "sensor_data",
        timestamp: 1000n,
        value: 42.0,
      } as MockMessage,
      {
        topic: "item_list",
        timestamp: 2000n,
        items: [
          { enabled: true, matrix: [1.0, 0.0, 0.0, 0.0] },
          { enabled: false, matrix: [0.0, 1.0, 0.0, 0.0] },
        ],
      } as MockMessage,
      {
        topic: "sensor_data",
        timestamp: 3000n,
        value: 84.0,
      } as MockMessage,
    ];

    it("should throw an error for missing ULog definitions", async () => {
      const ulogWithoutHeader = {
        open: jest.fn().mockResolvedValue(undefined),
        header: undefined,
        subscriptions: new Map<number, Subscription>(),
        readMessages: jest.fn(),
      } as unknown as jest.Mocked<ULog>;

      const buffer = new TempBuffer();
      await expect(
        convertULogFileToMCAP(ulogWithoutHeader, new McapWriter({ writable: buffer })),
      ).rejects.toThrow("Invalid ULog file: missing header");
    });

    it("should add channels for all subscriptions plus log messages", async () => {
      const mockULog = createULogMock({
        messageFields: topicFixture,
        subscriptions: [{ name: "sensor_data" }, { name: "item_list" }],
      });

      const mockOutputFile = new TempBuffer();
      await convertULogFileToMCAP(mockULog, new McapWriter({ writable: mockOutputFile }));

      const mcapReader = await McapIndexedReader.Initialize({
        readable: mockOutputFile,
      });
      const channelNames = Array.from(mcapReader.channelsById.values())
        .map((ch) => ch.topic)
        .sort();
      expect(channelNames).toStrictEqual(["item_list", "log_message", "sensor_data"]);
    });

    it("should add messages to MCAP with same content", async () => {
      const mockULog = createULogMock({
        messageFields: topicFixture,
        subscriptions: [{ name: "sensor_data" }, { name: "item_list" }],
        messages: messageFixture,
      });

      const mockOutputFile = new TempBuffer();
      await convertULogFileToMCAP(mockULog, new McapWriter({ writable: mockOutputFile }));

      const mcapReader = await McapIndexedReader.Initialize({
        readable: mockOutputFile,
      });
      const logTimes = [];
      const messageData = [];
      const topics = [];
      const sequence = [];
      for await (const msg of mcapReader.readMessages()) {
        const channel = mcapReader.channelsById.get(msg.channelId);
        const schema = mcapReader.schemasById.get(channel!.schemaId);
        const protobufSchema = protobufFromBinaryDescriptor(schema!.data).lookupType(schema!.name);
        logTimes.push(msg.publishTime);
        topics.push(mcapReader.channelsById.get(msg.channelId)?.topic);
        messageData.push(protobufSchema.toObject(protobufSchema.decode(msg.data)));
        sequence.push(msg.sequence);
      }
      expect(messageData.length).toBe(messageFixture.length);
      expect(logTimes).toStrictEqual([1000000n, 2000000n, 3000000n]);
      expect(sequence).toStrictEqual([0, 0, 1]);
      expect(topics).toStrictEqual(["sensor_data", "item_list", "sensor_data"]);
      expect(messageData).toStrictEqual([
        { value: 42.0 },
        {
          items: [
            { enabled: true, matrix: [1.0, 0.0, 0.0, 0.0] },
            { enabled: false, matrix: [0.0, 1.0, 0.0, 0.0] },
          ],
        },
        { value: 84.0 },
      ]);
    });

    it("should handle string fields", async () => {
      const mockULog = createULogMock({
        messageFields: new Map([
          [
            "text_topic",
            [
              { name: "timestamp", type: "uint64_t", isComplex: false },
              { name: "text", type: "char", arrayLength: 10, isComplex: false },
            ],
          ],
        ]),
        subscriptions: [{ name: "text_topic" }],
        messages: [
          {
            topic: "text_topic",
            timestamp: 1000n,
            text: "Message 1!",
          } as MockMessage,
          {
            topic: "text_topic",
            timestamp: 2000n,
            text: "Message 2!",
          } as MockMessage,
        ],
      });

      const mockOutputFile = new TempBuffer();
      await convertULogFileToMCAP(mockULog, new McapWriter({ writable: mockOutputFile }));

      const mcapReader = await McapIndexedReader.Initialize({
        readable: mockOutputFile,
      });
      const logTimes = [];
      const messageData = [];
      for await (const msg of mcapReader.readMessages()) {
        const channel = mcapReader.channelsById.get(msg.channelId);
        const schema = mcapReader.schemasById.get(channel!.schemaId);
        const protobufSchema = protobufFromBinaryDescriptor(schema!.data).lookupType(schema!.name);
        logTimes.push(msg.publishTime);
        messageData.push(protobufSchema.toObject(protobufSchema.decode(msg.data)));
      }
      expect(logTimes).toStrictEqual([1000000n, 2000000n]);
      expect(messageData).toStrictEqual([{ text: "Message 1!" }, { text: "Message 2!" }]);
    });

    it("should add messages with correct timestamps", async () => {
      const mockULog = createULogMock({
        messageFields: topicFixture,
        subscriptions: [{ name: "sensor_data" }, { name: "item_list" }],
        messages: messageFixture,
      });

      const mockOutputFile = new TempBuffer();
      await convertULogFileToMCAP(mockULog, new McapWriter({ writable: mockOutputFile }), {
        startTime: 1735689600000000n,
      });

      const mcapReader = await McapIndexedReader.Initialize({
        readable: mockOutputFile,
      });
      const logTimes = [];
      for await (const msg of mcapReader.readMessages()) {
        logTimes.push(msg.publishTime);
      }
      expect(logTimes.length).toBe(messageFixture.length);
      expect(logTimes).toStrictEqual([
        1735689600001000000n,
        1735689600002000000n,
        1735689600003000000n,
      ]);
    });

    it("should add separate channels to MCAP for distinct multiIds", async () => {
      const mockULog = createULogMock({
        messageFields: topicFixture,
        subscriptions: [
          { name: "sensor_data", multiId: 0 },
          { name: "sensor_data", multiId: 1 },
          { name: "item_list" },
        ],
        messages: [
          {
            topic: "sensor_data",
            timestamp: 1000n,
            value: 42.0,
          } as MockMessage,
          {
            topic: "sensor_data",
            multiId: 1,
            timestamp: 1000n,
            value: 36.0,
          } as MockMessage,
          {
            topic: "item_list",
            timestamp: 2000n,
            items: [
              { enabled: true, matrix: [1.0, 0.0, 0.0, 0.0] },
              { enabled: false, matrix: [0.0, 1.0, 0.0, 0.0] },
            ],
          } as MockMessage,
          {
            topic: "sensor_data",
            timestamp: 3000n,
            value: 84.0,
          } as MockMessage,
          {
            topic: "sensor_data",
            multiId: 1,
            timestamp: 3000n,
            value: 64.0,
          } as MockMessage,
        ],
      });

      const mockOutputFile = new TempBuffer();
      await convertULogFileToMCAP(mockULog, new McapWriter({ writable: mockOutputFile }));

      const mcapReader = await McapIndexedReader.Initialize({
        readable: mockOutputFile,
      });
      const logTimes = [];
      const messageData = [];
      const topics = [];
      const sequence = [];
      for await (const msg of mcapReader.readMessages()) {
        const channel = mcapReader.channelsById.get(msg.channelId);
        const schema = mcapReader.schemasById.get(channel!.schemaId);
        const protobufSchema = protobufFromBinaryDescriptor(schema!.data).lookupType(schema!.name);
        logTimes.push(msg.publishTime);
        topics.push(mcapReader.channelsById.get(msg.channelId)?.topic);
        messageData.push(protobufSchema.toObject(protobufSchema.decode(msg.data)));
        sequence.push(msg.sequence);
      }
      expect(messageData.length).toBe(5);
      expect(logTimes).toStrictEqual([1000000n, 1000000n, 2000000n, 3000000n, 3000000n]);
      expect(sequence).toStrictEqual([0, 0, 0, 1, 1]);
      expect(topics).toStrictEqual([
        "sensor_data/0",
        "sensor_data/1",
        "item_list",
        "sensor_data/0",
        "sensor_data/1",
      ]);
      expect(messageData).toStrictEqual([
        { value: 42.0 },
        { value: 36.0 },
        {
          items: [
            { enabled: true, matrix: [1.0, 0.0, 0.0, 0.0] },
            { enabled: false, matrix: [0.0, 1.0, 0.0, 0.0] },
          ],
        },
        { value: 84.0 },
        { value: 64.0 },
      ]);
    });

    it("should write logs to a separate log channel", async () => {
      const mockULog = {
        open: jest.fn().mockResolvedValue(undefined),
        header: {
          timestamp: 0n,
          definitions: new Map<string, MessageDefinition>(),
          version: 1,
        },
        subscriptions: new Map<number, Subscription>(),
        readMessages: jest.fn().mockImplementation(async function* () {
          const msgData = [
            { type: MessageType.Log, logLevel: LogLevel.Debug, message: "one" },
            { type: MessageType.LogTagged, tag: 2, logLevel: LogLevel.Info, message: "two" },
            { type: MessageType.Log, logLevel: LogLevel.Warning, message: "three" },
            { type: MessageType.LogTagged, tag: 4, logLevel: LogLevel.Err, message: "four" },
          ];
          let timestamp = 1000n;
          for (const msg of msgData) {
            yield {
              timestamp,
              ...msg,
            };
            timestamp += 1000n;
          }
        }),
      } as unknown as jest.Mocked<ULog>;

      const mockOutputFile = new TempBuffer();
      await convertULogFileToMCAP(mockULog, new McapWriter({ writable: mockOutputFile }));

      const mcapReader = await McapIndexedReader.Initialize({
        readable: mockOutputFile,
      });
      const logTimes = [];
      const messageData = [];
      const topics = [];
      const sequence = [];
      for await (const msg of mcapReader.readMessages()) {
        const channel = mcapReader.channelsById.get(msg.channelId);
        const schema = mcapReader.schemasById.get(channel!.schemaId);
        const protobufSchema = protobufFromBinaryDescriptor(schema!.data).lookupType(schema!.name);
        logTimes.push(msg.publishTime);
        topics.push(mcapReader.channelsById.get(msg.channelId)?.topic);
        messageData.push(protobufSchema.toObject(protobufSchema.decode(msg.data)));
        sequence.push(msg.sequence);
      }
      expect(messageData.length).toBe(4);
      expect(logTimes).toStrictEqual([1000000n, 2000000n, 3000000n, 4000000n]);
      expect(sequence).toStrictEqual([0, 1, 2, 3]);
      expect(topics).toStrictEqual(["log_message", "log_message", "log_message", "log_message"]);
      expect(messageData).toStrictEqual([
        { timestamp: { sec: 0, nsec: 1000000 }, level: FoxLevel.DEBUG, message: "one" },
        { timestamp: { sec: 0, nsec: 2000000 }, level: FoxLevel.INFO, message: "two" },
        { timestamp: { sec: 0, nsec: 3000000 }, level: FoxLevel.WARNING, message: "three" },
        { timestamp: { sec: 0, nsec: 4000000 }, level: FoxLevel.ERROR, message: "four" },
      ]);
    });

    it("should add metadata to the mcap file", async () => {
      const mockULog = createULogMock({
        messageFields: topicFixture,
        subscriptions: [{ name: "sensor_data" }, { name: "item_list" }],
      });

      const metadataFields = new Map<string, string>();
      metadataFields.set("foo", "bar");
      const metadata = { name: "foxglove", metadata: metadataFields } as Metadata;

      const mockOutputFile = new TempBuffer();
      await convertULogFileToMCAP(mockULog, new McapWriter({ writable: mockOutputFile }), {
        metadata: [metadata],
      });

      const mcapReader = await McapIndexedReader.Initialize({
        readable: mockOutputFile,
      });

      const storedMetadata = [];

      for await (const m of mcapReader.readMetadata()) {
        storedMetadata.push(m);
      }

      expect(storedMetadata).toHaveLength(1);
      expect(storedMetadata[0]!).toStrictEqual({ type: "Metadata", ...metadata });
    });

    it("should handle bigint conversions to integer", async () => {
      const mockULog = createULogMock({
        messageFields: new Map([
          [
            "sensor_data",
            [
              { name: "timestamp", type: "uint64_t", isComplex: false },
              { name: "value", type: "uint64_t", isComplex: false },
            ],
          ],
        ]),
        subscriptions: [{ name: "sensor_data" }],
        messages: [
          {
            topic: "sensor_data",
            timestamp: 1000n,
            value: 18446744073709551615n,
          } as MockMessage,
        ],
      });

      const mockOutputFile = new TempBuffer();
      await convertULogFileToMCAP(mockULog, new McapWriter({ writable: mockOutputFile }));
      const mcapReader = await McapIndexedReader.Initialize({
        readable: mockOutputFile,
      });
      const messageData = [];
      for await (const msg of mcapReader.readMessages()) {
        const channel = mcapReader.channelsById.get(msg.channelId);
        const schema = mcapReader.schemasById.get(channel!.schemaId);
        const protobufSchema = protobufFromBinaryDescriptor(schema!.data).lookupType(schema!.name);
        messageData.push(protobufSchema.toObject(protobufSchema.decode(msg.data)));
      }
      expect(messageData.length).toBe(1);
      expect(messageData).toStrictEqual([{ value: Long.fromString("18446744073709551615", true) }]);
    });
  });

  it("should perform full ULog to MCAP conversion with sample files", async () => {
    const inputFileHandle = new FileReader(__dirname + "/../fixtures/test_ulog.ulg");

    const mockOutputFile = new TempBuffer();
    await convertULogFileToMCAP(
      new ULog(inputFileHandle),
      new McapWriter({ writable: mockOutputFile }),
    );

    const mcapReader = await McapIndexedReader.Initialize({
      readable: mockOutputFile,
    });

    const channelTopics = Array.from(mcapReader.channelsById.values()).map((ch) => ch.topic);
    expect(channelTopics.length).toBe(115);
  });
});
