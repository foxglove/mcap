/* cspell:words varint */

const MCAP_MAGIC = Buffer.from([0x89, 0x4d, 0x43, 0x41, 0x50, 0x30, 0x0d, 0x0a]);

type ProtobufJsonMessage = {
  sequence: number;
  logTime: number;
  publishTime: number;
  snakeCase: string;
  zeroValue?: number;
  count: number;
};

export type ProtobufJsonFixtureOptions = {
  messages: ProtobufJsonMessage[];
};

export function makeProtobufJsonMcap(options: ProtobufJsonFixtureOptions): Buffer {
  const descriptor = sampleDescriptorSet();
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
    ...options.messages.map((message) =>
      record(
        0x05,
        Buffer.concat([
          uint16(1),
          uint32(message.sequence),
          uint64(message.logTime),
          uint64(message.publishTime),
          sampleMessage(message),
        ]),
      ),
    ),
    record(0x0f, uint32(0)),
    record(0x02, Buffer.concat([uint64(0), uint64(0), uint32(0)])),
  ];
  return Buffer.concat([MCAP_MAGIC, ...records, MCAP_MAGIC]);
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

function record(opcode: number, body: Buffer): Buffer {
  return Buffer.concat([Buffer.from([opcode]), uint64(body.length), body]);
}

function mcapString(value: string): Buffer {
  const bytes = Buffer.from(value, "utf8");
  return Buffer.concat([uint32(bytes.length), bytes]);
}

function prefixedBytes(value: Buffer): Buffer {
  return Buffer.concat([uint32(value.length), value]);
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

function uint16(value: number): Buffer {
  const out = Buffer.alloc(2);
  out.writeUInt16LE(value);
  return out;
}

function uint32(value: number): Buffer {
  const out = Buffer.alloc(4);
  out.writeUInt32LE(value);
  return out;
}

function uint64(value: number): Buffer {
  const out = Buffer.alloc(8);
  out.writeBigUInt64LE(BigInt(value));
  return out;
}
