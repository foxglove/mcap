#!/usr/bin/env -S npx tsx
/**
 * MCAP write benchmark for TypeScript.
 */

import { open, readFile, stat } from "fs/promises";
import { crc32 } from "zlib";

import { McapWriter } from "../../typescript/core/src/index.ts";
import { FileHandleWritable } from "../../typescript/nodejs/src/index.ts";

// Shared payload blob parameters; must match gen_blob.py and the other
// language benches. Message i's payload is the window of the blob starting
// at (i * BLOB_STRIDE) % BLOB_WINDOW_SPAN, so all implementations feed
// identical bytes to their writers.
const BLOB_SIZE = 16777216;
const BLOB_MAX_PAYLOAD = 524288;
const BLOB_WINDOW_SPAN = BLOB_SIZE - BLOB_MAX_PAYLOAD;
const BLOB_STRIDE = 7919;

function payloadOffset(msgIndex: number): number {
  return (msgIndex * BLOB_STRIDE) % BLOB_WINDOW_SPAN;
}

async function main(): Promise<number> {
  if (process.argv.length !== 7) {
    process.stderr.write(
      `Usage: ${process.argv[1]} <output_file> <mode> <num_messages> <payload_size> <blob_file>\n`,
    );
    return 1;
  }

  const blob: Uint8Array = await readFile(process.argv[6]!);
  if (blob.byteLength !== BLOB_SIZE) {
    process.stderr.write(
      `Blob file ${process.argv[6]!} is not exactly ${BLOB_SIZE} bytes\n`,
    );
    return 1;
  }

  const filename = process.argv[2]!;
  const mode = process.argv[3]!;
  const isMixed = process.argv[5] === "mixed";
  const numMessages = isMixed ? 3750 : parseInt(process.argv[4]!, 10);
  const payloadSize = isMixed ? 0 : parseInt(process.argv[5]!, 10);

  if (!isMixed && (numMessages <= 0 || payloadSize <= 0)) {
    process.stderr.write("num_messages and payload_size must be positive\n");
    return 1;
  }
  if (!isMixed && payloadSize > BLOB_MAX_PAYLOAD) {
    process.stderr.write(`payload_size must be <= ${BLOB_MAX_PAYLOAD}\n`);
    return 1;
  }

  let useChunks = true;
  let compressChunk:
    | ((data: Uint8Array) => {
        compression: string;
        compressedData: Uint8Array;
      })
    | undefined;

  if (mode === "unchunked") {
    useChunks = false;
  } else if (mode === "chunked") {
    // chunked, no compression
  } else if (mode === "zstd") {
    const zstdMod = await import("@foxglove/wasm-zstd");
    const zstd = zstdMod.default;
    await zstd.isLoaded;
    compressChunk = (data: Uint8Array) => ({
      compression: "zstd",
      compressedData: new Uint8Array(zstd.compress(data)),
    });
  } else if (mode === "lz4") {
    process.stderr.write(
      "LZ4 compression is not available in the TypeScript MCAP library\n",
    );
    return 1;
  } else {
    process.stderr.write(`Unknown mode: ${mode}\n`);
    return 1;
  }

  const fileHandle = await open(filename, "w");
  const writer = new McapWriter({
    writable: new FileHandleWritable(fileHandle),
    useChunks,
    chunkSize: 786432,
    compressChunk,
  });

  await writer.start({ profile: "bench", library: "ts-bench" });

  let tStart: bigint;
  let tEnd: bigint;
  let payloadCrc = 0;

  if (isMixed) {
    // Mixed-payload mode: simulate a 10-second robot recording.
    const channelDefs = [
      {
        topic: "/imu",
        schema: "IMU",
        sizes: [96],
        periodNs: 5_000_000n,
        count: 2000,
      },
      {
        topic: "/odom",
        schema: "Odometry",
        sizes: [296],
        periodNs: 20_000_000n,
        count: 500,
      },
      {
        topic: "/tf",
        schema: "TFMessage",
        sizes: [80, 160, 320, 800, 1600],
        periodNs: 10_000_000n,
        count: 1000,
      },
      {
        topic: "/lidar",
        schema: "PointCloud2",
        sizes: [230400],
        periodNs: 100_000_000n,
        count: 100,
      },
      {
        topic: "/camera/compressed",
        schema: "CompressedImage",
        sizes: [524288],
        periodNs: 66_666_667n,
        count: 150,
      },
    ];

    const schemaData = new TextEncoder().encode('{"type":"object"}');
    const channelIds: number[] = [];
    for (const def of channelDefs) {
      const sid = await writer.registerSchema({
        name: def.schema,
        encoding: "jsonschema",
        data: schemaData,
      });
      const cid = await writer.registerChannel({
        topic: def.topic,
        schemaId: sid,
        messageEncoding: "json",
        metadata: new Map(),
      });
      channelIds.push(cid);
    }

    // Pre-generate sorted message schedule.
    const schedule: { timestamp: bigint; channelIndex: number }[] = [];
    for (let ci = 0; ci < channelDefs.length; ci++) {
      const def = channelDefs[ci]!;
      for (let m = 0; m < def.count; m++) {
        schedule.push({
          timestamp: BigInt(m) * def.periodNs,
          channelIndex: ci,
        });
      }
    }
    schedule.sort((a, b) => {
      if (a.timestamp < b.timestamp) return -1;
      if (a.timestamp > b.timestamp) return 1;
      return a.channelIndex - b.channelIndex;
    });

    // Not timed: CRC of the payload stream for cross-language verification.
    {
      const crcSeq = new Array(channelDefs.length).fill(0) as number[];
      for (let i = 0; i < schedule.length; i++) {
        const ci = schedule[i]!.channelIndex;
        const def = channelDefs[ci]!;
        const seq = crcSeq[ci]!;
        crcSeq[ci] = seq + 1;
        const sz = def.sizes[seq % def.sizes.length]!;
        const off = payloadOffset(i);
        payloadCrc = crc32(blob.subarray(off, off + sz), payloadCrc);
      }
    }

    // Per-channel sequence counters for payload size cycling.
    const chanSeq = new Array(channelDefs.length).fill(0) as number[];

    // Time the message-writing loop + end.
    tStart = process.hrtime.bigint();

    for (let i = 0; i < schedule.length; i++) {
      const msg = schedule[i]!;
      const ci = msg.channelIndex;
      const def = channelDefs[ci]!;
      const seq = chanSeq[ci]!;
      chanSeq[ci] = seq + 1;
      const sz = def.sizes[seq % def.sizes.length]!;
      const off = payloadOffset(i);
      const data = blob.subarray(off, off + sz);
      await writer.addMessage({
        channelId: channelIds[ci]!,
        sequence: seq,
        logTime: msg.timestamp,
        publishTime: msg.timestamp,
        data,
      });
    }

    await writer.end();
    tEnd = process.hrtime.bigint();
    await fileHandle.close();
  } else {
    // Fixed-payload mode.
    const schemaId = await writer.registerSchema({
      name: "BenchMsg",
      encoding: "jsonschema",
      data: new TextEncoder().encode('{"type":"object"}'),
    });

    const channelId = await writer.registerChannel({
      topic: "/bench",
      schemaId,
      messageEncoding: "json",
      metadata: new Map(),
    });

    // Not timed: CRC of the payload stream for cross-language verification.
    for (let i = 0; i < numMessages; i++) {
      const off = payloadOffset(i);
      payloadCrc = crc32(blob.subarray(off, off + payloadSize), payloadCrc);
    }

    // Time the message-writing loop + end
    tStart = process.hrtime.bigint();

    for (let i = 0; i < numMessages; i++) {
      const logTime = BigInt(i) * 1000n;
      const off = payloadOffset(i);
      await writer.addMessage({
        channelId,
        sequence: i,
        logTime,
        publishTime: logTime,
        data: blob.subarray(off, off + payloadSize),
      });
    }

    await writer.end();
    tEnd = process.hrtime.bigint();
    await fileHandle.close();
  }

  const elapsedNs = tEnd - tStart;
  const wallSec = Number(elapsedNs) / 1e9;
  const fileSize = (await stat(filename)).size;

  const peakRssKb = process.resourceUsage().maxRSS;

  // TSV output: op lang mode num_msgs payload_size file_size elapsed_ns wall_sec peak_rss_kb payload_crc32
  const payloadSizeStr = isMixed ? "mixed" : String(payloadSize);
  process.stdout.write(
    `write\ttypescript\t${mode}\t${numMessages}\t${payloadSizeStr}\t${fileSize}\t${elapsedNs}\t${wallSec.toFixed(
      6,
    )}\t${peakRssKb}\t${payloadCrc}\n`,
  );

  return 0;
}

main().then((code) => process.exit(code));
