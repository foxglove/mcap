import {
  float32,
  int32,
  MCAP_MAGIC,
  mcapString,
  prefixedBytes,
  record,
  uint16,
  uint32,
  uint64,
} from "./mcapFixtureHelpers.ts";

type Ros1JsonMessage = {
  sequence: number;
  logTime: bigint | number;
  publishTime: bigint | number;
  headerSeq: number;
  stampSec: number;
  stampNsec: number;
  frameId: string;
  durationSec: number;
  durationNsec: number;
  values: number[];
};

export type Ros1JsonFixtureOptions = {
  messages: Ros1JsonMessage[];
};

export function makeRos1JsonMcap(options: Ros1JsonFixtureOptions): Buffer {
  const schema = Buffer.from(
    [
      "Header header",
      "float32 nan_value",
      "float32 pos_inf",
      "float32 neg_inf",
      "duration offset",
      "int32[] values",
      "================================================================================",
      "MSG: std_msgs/Header",
      "uint32 seq",
      "time stamp",
      "string frame_id",
      "",
    ].join("\n"),
    "utf8",
  );

  const records = [
    record(0x01, Buffer.concat([mcapString(""), mcapString("")])),
    record(
      0x03,
      Buffer.concat([
        uint16(1),
        mcapString("demo/JsonEdgeCases"),
        mcapString("ros1msg"),
        prefixedBytes(schema),
      ]),
    ),
    record(
      0x04,
      Buffer.concat([uint16(1), uint16(1), mcapString("ros1"), mcapString("ros1"), uint32(0)]),
    ),
    ...options.messages.map((message) =>
      record(
        0x05,
        Buffer.concat([
          uint16(1),
          uint32(message.sequence),
          uint64(message.logTime),
          uint64(message.publishTime),
          ros1Message(message),
        ]),
      ),
    ),
    record(0x0f, uint32(0)),
    record(0x02, Buffer.concat([uint64(0), uint64(0), uint32(0)])),
  ];
  return Buffer.concat([MCAP_MAGIC, ...records, MCAP_MAGIC]);
}

function ros1Message(message: Ros1JsonMessage): Buffer {
  return Buffer.concat([
    uint32(message.headerSeq),
    uint32(message.stampSec),
    uint32(message.stampNsec),
    ros1String(message.frameId),
    float32(Number.NaN),
    float32(Number.POSITIVE_INFINITY),
    float32(Number.NEGATIVE_INFINITY),
    int32(message.durationSec),
    int32(message.durationNsec),
    uint32(message.values.length),
    ...message.values.map((value) => int32(value)),
  ]);
}

function ros1String(value: string): Buffer {
  const bytes = Buffer.from(value, "utf8");
  return Buffer.concat([uint32(bytes.length), bytes]);
}
