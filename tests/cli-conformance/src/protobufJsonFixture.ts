/* cspell:words varint */

import {
  MCAP_MAGIC,
  mcapString,
  prefixedBytes,
  record,
  uint16,
  uint32,
  uint64,
} from "./mcapFixtureHelpers.ts";

type ProtobufJsonMessage = {
  sequence: number;
  logTime: bigint | number;
  publishTime: bigint | number;
  snakeCase: string;
  zeroValue?: number;
  count: number;
};

type ProtobufJsonMetadata = {
  name: string;
  metadata: Record<string, string>;
};

type ProtobufJsonAttachment = {
  name: string;
  mediaType: string;
  logTime: bigint | number;
  createTime: bigint | number;
  data: number[];
};

export type ProtobufJsonFixtureOptions = {
  messages: ProtobufJsonMessage[];
  metadata?: ProtobufJsonMetadata[];
  attachments?: ProtobufJsonAttachment[];
  chunkedMessages?: boolean;
};

export function makeProtobufJsonMcap(options: ProtobufJsonFixtureOptions): Buffer {
  const descriptor = sampleDescriptorSet();
  const messageRecords = options.messages.map(messageRecord);
  const records = [
    record(0x01, Buffer.concat([mcapString(""), mcapString("")])),
    record(
      0x03,
      Buffer.concat([
        uint16(1),
        mcapString("test.Sample"),
        mcapString("protobuf"),
        prefixedBytes(descriptor),
      ]),
    ),
    record(
      0x04,
      Buffer.concat([uint16(1), uint16(1), mcapString("proto"), mcapString("protobuf"), uint32(0)]),
    ),
    ...(options.chunkedMessages ? [chunkRecord(messageRecords, options.messages)] : messageRecords),
    ...(options.metadata ?? []).map((metadata) =>
      record(0x0c, Buffer.concat([mcapString(metadata.name), mcapStringMap(metadata.metadata)])),
    ),
    ...(options.attachments ?? []).map((attachment) => {
      const data = Buffer.from(attachment.data);
      return record(
        0x09,
        Buffer.concat([
          uint64(attachment.logTime),
          uint64(attachment.createTime),
          mcapString(attachment.name),
          mcapString(attachment.mediaType),
          uint64(data.length),
          data,
          uint32(0),
        ]),
      );
    }),
    record(0x0f, uint32(0)),
    record(0x02, Buffer.concat([uint64(0), uint64(0), uint32(0)])),
  ];
  return Buffer.concat([MCAP_MAGIC, ...records, MCAP_MAGIC]);
}

function messageRecord(message: ProtobufJsonMessage): Buffer {
  return record(
    0x05,
    Buffer.concat([
      uint16(1),
      uint32(message.sequence),
      uint64(message.logTime),
      uint64(message.publishTime),
      sampleMessage(message),
    ]),
  );
}

function chunkRecord(records: Buffer[], messages: ProtobufJsonMessage[]): Buffer {
  const chunkRecords = Buffer.concat(records);
  const logTimes = messages.map((message) => BigInt(message.logTime));
  const messageStartTime = logTimes.reduce(
    (min, value) => (value < min ? value : min),
    logTimes[0] ?? 0n,
  );
  const messageEndTime = logTimes.reduce(
    (max, value) => (value > max ? value : max),
    logTimes[0] ?? 0n,
  );

  return record(
    0x06,
    Buffer.concat([
      uint64(messageStartTime),
      uint64(messageEndTime),
      uint64(chunkRecords.length),
      uint32(0),
      mcapString(""),
      uint64(chunkRecords.length),
      chunkRecords,
    ]),
  );
}

function mcapStringMap(values: Record<string, string>): Buffer {
  const entries = Object.entries(values).map(([key, value]) =>
    Buffer.concat([mcapString(key), mcapString(value)]),
  );
  return prefixedBytes(Buffer.concat(entries));
}

function sampleDescriptorSet(): Buffer {
  // FileDescriptorSet for:
  //
  // syntax = "proto3";
  // package test;
  // message Sample {
  //   string snake_case = 1;
  //   uint32 zero_value = 2;
  //   uint32 count = 3;
  // }
  const sampleMessageDescriptor = Buffer.concat([
    protoString(1, "Sample"),
    protoField(2, fieldDescriptor("snake_case", 1, 9, "snakeCase")),
    protoField(2, fieldDescriptor("zero_value", 2, 13, "zeroValue")),
    protoField(2, fieldDescriptor("count", 3, 13, "count")),
  ]);
  const fileDescriptor = Buffer.concat([
    protoString(1, "sample.proto"),
    protoString(2, "test"),
    protoField(4, sampleMessageDescriptor),
    protoString(12, "proto3"),
  ]);
  return protoField(1, fileDescriptor);
}

function fieldDescriptor(
  name: string,
  fieldNumber: number,
  type: number,
  jsonName: string,
): Buffer {
  return Buffer.concat([
    protoString(1, name),
    protoUint64(3, fieldNumber),
    protoUint64(4, 1), // LABEL_OPTIONAL
    protoUint64(5, type),
    protoString(10, jsonName),
  ]);
}

function sampleMessage(message: ProtobufJsonMessage): Buffer {
  const fields = [protoString(1, message.snakeCase)];
  if (message.zeroValue != undefined) {
    fields.push(protoUint64(2, message.zeroValue));
  }
  fields.push(protoUint64(3, message.count));
  return Buffer.concat(fields);
}

function protoField(fieldNumber: number, value: Buffer): Buffer {
  return Buffer.concat([varint((fieldNumber << 3) | 2), varint(value.length), value]);
}

function protoString(fieldNumber: number, value: string): Buffer {
  return protoField(fieldNumber, Buffer.from(value, "utf8"));
}

function protoUint64(fieldNumber: number, value: number): Buffer {
  return Buffer.concat([varint((fieldNumber << 3) | 0), varint(value)]);
}

function varint(value: number): Buffer {
  const bytes: number[] = [];
  let remaining = value;
  while (remaining >= 0x80) {
    bytes.push((remaining & 0x7f) | 0x80);
    remaining = Math.floor(remaining / 0x80);
  }
  bytes.push(remaining);
  return Buffer.from(bytes);
}
