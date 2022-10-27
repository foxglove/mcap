import { Grid, FrameTransform, NumericType } from "@foxglove/schemas";
import {
  Grid as GridSchema,
  FrameTransform as FrameTransformSchema,
} from "@foxglove/schemas/jsonschema";
import { McapWriter, IWritable } from "@mcap/core";
import { open, FileHandle } from "fs/promises";

import { Message } from "../../../core/src/types";

const QUAT_IDENTITY = { x: 0, y: 0, z: 0, w: 1 };

// Mcap IWritable interface for nodejs FileHandle
class FileHandleWritable implements IWritable {
  private handle: FileHandle;
  private totalBytesWritten = 0;

  constructor(handle: FileHandle) {
    this.handle = handle;
  }

  async write(buffer: Uint8Array): Promise<void> {
    const written = await this.handle.write(buffer);
    this.totalBytesWritten += written.bytesWritten;
  }

  position(): bigint {
    return BigInt(this.totalBytesWritten);
  }
}
function roundUp(numToRound: number, multiple: number) {
  if (multiple !== 0 && (multiple & (multiple - 1)) === 0) {
    return (numToRound + multiple - 1) & -multiple;
  } else {
    throw Error(`invalid multiple ${multiple}, num to round ${numToRound}`);
  }
}
const scriptParameters = {
  mcapTimeLength: 3000, //ms
  gridMessageFrequency: 5, //hz
};

const gridParameters = {
  cell_size: { x: 10, y: 10 },
  column_count: 256,
  row_count: 256,
  cell_stride: 4,
  row_stride: 0,
};

gridParameters.row_stride = roundUp(gridParameters.column_count * gridParameters.cell_stride, 2);
console.log(`row stride ${gridParameters.row_stride}`);

interface GridDataFuncParams {
  x: number;
  y: number;
  i: number;
  rows: number;
  cols: number;
  time: number;
}

const fieldDataFuncs = {
  sinpluscos: ({ x, y }: GridDataFuncParams): number => {
    return Math.sin(y) + Math.cos(x);
  },
  sinmultcos: ({ x, y }: GridDataFuncParams): number => {
    return Math.sin(y) * Math.cos(x);
  },
  sintancos: ({ x, y }: GridDataFuncParams): number => {
    return Math.sin(Math.cos(Math.tan(x))) + Math.sin(Math.cos(Math.tan(y)));
  },
} as { [k: string]: (x: GridDataFuncParams) => number };

function makeNewGridFromParamsWithoutData() {
  const { cell_size, cell_stride, column_count, row_count, row_stride } = gridParameters;

  const defaultGrid = {
    timestamp: { sec: 0, nsec: 0 },
    frame_id: "sensor",
    pose: {
      position: {
        x: -0.5 * cell_size.x * row_count + 2,
        y: -0.5 * cell_size.y * column_count + 2,
        z: 0,
      },
      orientation: QUAT_IDENTITY,
    },
    cell_size,
    column_count,
    cell_stride,
    row_stride,
    fields: [{ name: "sintancos", offset: 0, type: NumericType.FLOAT32 }],
  } as Omit<Grid, "data">;

  return defaultGrid;
}

function addGridData(grid: Omit<Grid, "data">, row_count: number, time: number) {
  const { column_count, fields, cell_stride, row_stride } = grid;
  const data = new Uint8Array(row_stride * row_count);
  const view = new DataView(data.buffer, data.byteOffset, data.byteLength);

  for (let y = 0; y < row_count; y++) {
    for (let x = 0; x < column_count; x++) {
      const i = y * row_stride + x * cell_stride;
      for (const field of fields) {
        const { name, offset, type } = field;
        if (type !== NumericType.FLOAT32) {
          throw new Error("unsupported numeric types");
        }
        const dataFunc = fieldDataFuncs[name];
        if (dataFunc) {
          const value = dataFunc({
            x,
            y,
            i,
            rows: row_count,
            cols: column_count,
            time,
          });
          view.setFloat32(i + offset, value);
        } else {
          throw new Error("missing data func " + name);
        }
      }
    }
  }

  (grid as Grid).data = data;
}

async function addMessageAtTime(
  mcapFile: McapWriter,
  gridChannelId: number,
  getMessageData: (time: number) => Message["data"],
  time: bigint,
): Promise<void> {
  const message = getMessageData(Number(time));
  console.log(`writing message at ${time}`);
  await mcapFile.addMessage({
    channelId: gridChannelId,
    sequence: 0,
    publishTime: time,
    logTime: time,
    data: message,
  });
}

async function main() {
  const mcapFilePath = "output.mcap";
  const fileHandle = await open(mcapFilePath, "w");
  const fileHandleWritable = new FileHandleWritable(fileHandle);

  const mcapFile = new McapWriter({
    writable: fileHandleWritable,
    useStatistics: false,
    useChunks: true,
    useChunkIndex: true,
  });

  await mcapFile.start({
    profile: "",
    library: "mcap example",
  });
  const tfSchemaId = await mcapFile.registerSchema({
    name: "foxglove.FrameTransform",
    encoding: "jsonschema",
    data: Buffer.from(JSON.stringify(FrameTransformSchema)),
  });

  const tfChannelId = await mcapFile.registerChannel({
    schemaId: tfSchemaId,
    topic: "tf",
    messageEncoding: "json",
    metadata: new Map(),
  });

  await mcapFile.addMessage({
    channelId: tfChannelId,
    sequence: 0,
    publishTime: 0n,
    logTime: 0n,
    data: Buffer.from(
      JSON.stringify({
        timestamp: { sec: 0, nsec: 0 },
        parent_frame_id: "base_link",
        child_frame_id: "sensor",
        translation: { x: 0, y: 0, z: 1 },
        rotation: QUAT_IDENTITY,
      } as FrameTransform),
    ),
  });

  const gridSchemaId = await mcapFile.registerSchema({
    name: "foxglove.Grid",
    encoding: "jsonschema",
    data: Buffer.from(JSON.stringify(GridSchema)),
  });

  const gridChannelId = await mcapFile.registerChannel({
    schemaId: gridSchemaId,
    topic: "grid",
    messageEncoding: "json",
    metadata: new Map(),
  });

  const { mcapTimeLength, gridMessageFrequency } = scriptParameters;

  const timeBetweenMessages = 1000 / gridMessageFrequency; // 1s (ms) / frequency
  let currTime = 0;
  const getGridMessageData = (time: number) => {
    console.time('generate message')
    const grid = makeNewGridFromParamsWithoutData();
    addGridData(grid, gridParameters.row_count, time);
    console.timeEnd('generate message');

    console.time('stringify');
    const data = Buffer.from(JSON.stringify(grid));
    console.timeEnd('stringify');
    return data;
  };
  let count = 0;
  const proms = [];
  while (currTime < mcapTimeLength) {
    console.log(`Adding grid ${count}`);
    proms.push(addMessageAtTime(mcapFile, gridChannelId, getGridMessageData, BigInt(currTime)));
    count++;
    currTime += timeBetweenMessages;
  }
  await Promise.all(proms);

  await mcapFile.end();
}

void main();
