import { NumericType } from "@foxglove/schemas";
import type { Grid } from "@foxglove/schemas";
import { McapWriter } from "@mcap/core";
import { FileHandleWritable } from "@mcap/nodejs";
import { Builder } from "flatbuffers";
import fs from "node:fs";
import { open } from "node:fs/promises";

import { buildGridMessage, buildTfMessage } from "./flatbufferUtils.ts";

const QUAT_IDENTITY = { x: 0, y: 0, z: 0, w: 1 };

function nextPowerOfTwo(numToRound: number) {
  let nextPower = 1;
  while (nextPower < numToRound) {
    nextPower *= 2;
  }
  return nextPower;
}
const scriptParameters = {
  mcapTimeLength: 10000, //ms
  gridMessageFrequency: 20, //hz
};

const gridParameters = {
  cell_size: { x: 0.1, y: 0.1 },
  column_count: 512,
  row_count: 512,
  cell_stride: 4,
  row_stride: 0,
};

// set row stride to be next largest power of 2
gridParameters.row_stride = nextPowerOfTwo(
  gridParameters.column_count * gridParameters.cell_stride,
);
console.log(`row stride ${gridParameters.row_stride}`);

interface GridDataFuncParams {
  x: number;
  y: number;
  i: number;
  rows: number;
  cols: number;
  time: number;
}

// functions to generate data for a grid message based off of time, placement and grid params
const fieldDataFuncs: { [k: string]: (x: GridDataFuncParams) => number } = {
  sinPlusCos: ({ x, y }: GridDataFuncParams): number => {
    return Math.sin(y) + Math.cos(x);
  },
  sinTimesCos: ({ x, y }: GridDataFuncParams): number => {
    return Math.sin(y) * Math.cos(x);
  },
  growingSinTanCos: ({ x, y, rows, cols, time }: GridDataFuncParams): number => {
    const z = time * 0.0000000001;
    return (
      z +
      Math.sin(Math.cos(Math.tan((x / cols) * Math.PI + z))) +
      Math.sin(Math.cos(Math.tan((y / rows) * Math.PI)))
    );
  },
  sinTanCos: ({ x, y, rows, cols, time }: GridDataFuncParams): number => {
    return (
      Math.sin(Math.cos(Math.tan((x / cols) * Math.PI + time))) +
      Math.sin(Math.cos(Math.tan((y / rows) * Math.PI)))
    );
  },
};

function makeNewGrid() {
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
    fields: [{ name: "sinTanCos", offset: 0, type: NumericType.FLOAT32 }],
  } as Omit<Grid, "data">;

  return defaultGrid;
}

// adds data to the grid based on the grid parameters and fields
function getGridData(grid: Omit<Grid, "data">, row_count: number, time: number): Uint8Array {
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
          view.setFloat32(i + offset, value, true);
        } else {
          throw new Error(`missing data func for field: ${name}`);
        }
      }
    }
  }

  return data;
}

async function main() {
  const mcapFilePath = "flatbuffer.mcap";
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
  const FrameTransformSchemaBuffer = fs.readFileSync(
    `${import.meta.dirname}/../../flatbuffer/bin/FrameTransform.bfbs`,
  );
  const tfSchemaId = await mcapFile.registerSchema({
    name: "foxglove.FrameTransform",
    encoding: "flatbuffer",
    data: new Uint8Array(FrameTransformSchemaBuffer),
  });

  const tfChannelId = await mcapFile.registerChannel({
    schemaId: tfSchemaId,
    topic: "tf",
    messageEncoding: "flatbuffer",
    metadata: new Map(),
  });
  const tfJson = {
    timestamp: { sec: 0, nsec: 0 },
    parent_frame_id: "base_link",
    child_frame_id: "sensor",
    translation: { x: 0, y: 0, z: 1 },
    rotation: QUAT_IDENTITY,
  };

  const tfBuilder = new Builder();
  const tf = buildTfMessage(tfBuilder, tfJson);
  tfBuilder.finish(tf);

  await mcapFile.addMessage({
    channelId: tfChannelId,
    sequence: 0,
    publishTime: 0n,
    logTime: 0n,
    data: tfBuilder.asUint8Array(),
  });

  const binaryGridSchema = fs.readFileSync(`${import.meta.dirname}/../../flatbuffer/bin/Grid.bfbs`);

  const gridSchemaId = await mcapFile.registerSchema({
    name: "foxglove.Grid",
    encoding: "flatbuffer",
    data: new Uint8Array(binaryGridSchema),
  });

  const gridChannelId = await mcapFile.registerChannel({
    schemaId: gridSchemaId,
    topic: "grid",
    messageEncoding: "flatbuffer",
    metadata: new Map(),
  });

  const { mcapTimeLength, gridMessageFrequency } = scriptParameters;

  const msTimeBetweenMessages = (1 / gridMessageFrequency) * 1000; // interval length = (1 / frequency) * 1000 (ms in a second)
  let currTime = 0;
  const getGridMessageData = (time: number) => {
    const grid = makeNewGrid();
    const data = getGridData(grid, gridParameters.row_count, time);
    (grid as Grid).data = data;

    const gridBuilder = new Builder();
    const fbGrid = buildGridMessage(gridBuilder, grid as Grid);
    gridBuilder.finish(fbGrid);

    return gridBuilder.asUint8Array();
  };
  let count = 0;
  while (currTime <= mcapTimeLength) {
    console.log(`Adding grid ${count}`);
    const nsTime = BigInt(currTime) * 1_000_000n;
    const message = getGridMessageData(currTime);
    await mcapFile.addMessage({
      channelId: gridChannelId,
      sequence: 0,
      publishTime: nsTime,
      logTime: nsTime,
      data: message,
    });
    count++;
    currTime += msTimeBetweenMessages;
  }

  await mcapFile.end();
}

void main();
