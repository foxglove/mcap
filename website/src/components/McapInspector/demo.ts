// cspell:words navsat
import { McapWriter } from "@mcap/core";
/** A real MCAP, passed through the same loader as user-selected files. */
export async function createDemo(): Promise<File> {
  const parts: Uint8Array<ArrayBuffer>[] = [];
  let size = 0n;
  const writer = new McapWriter({
    useChunks: true,
    chunkSize: 6000,
    writable: {
      position: () => size,
      write: async (data) => {
        parts.push(new Uint8Array(data));
        size += BigInt(data.length);
      },
    },
  });
  await writer.start({ profile: "", library: "mcap-chunk-viewer demo" });
  const topics: [string, number][] = [
    ["/camera/image", 10],
    ["/camera/info", 10],
    ["/imu", 200],
    ["/imu/mag", 50],
    ["/tf", 30],
    ["/odom", 30],
    ["/scan", 5],
    ["/diagnostics", 2],
    ["/joint_states", 40],
    ["/navsat", 1],
    ["/pointcloud", 2],
    ["/cmd_vel", 20],
  ];
  const ids: number[] = [];
  for (const [topic] of topics) {
    ids.push(
      await writer.registerChannel({
        schemaId: 0,
        topic,
        messageEncoding: "json",
        metadata: new Map(),
      }),
    );
  }
  const origin = 1_720_000_000_000_000_000n;
  for (let tick = 0; tick < 4000; tick++) {
    for (let row = 0; row < topics.length; row++) {
      if (tick % Math.round(200 / topics[row]![1]) !== row % 2) {
        continue;
      }
      const logTime = origin + BigInt(tick) * 5_000_000n;
      await writer.addMessage({
        channelId: ids[row]!,
        sequence: tick,
        logTime,
        publishTime: logTime - 2_000_000n,
        data: new TextEncoder().encode(
          JSON.stringify({ value: Math.round(Math.sin(tick / 20) * 100) }),
        ),
      });
    }
  }
  await writer.end();
  return new File(parts, "demo-robot.mcap", {
    type: "application/octet-stream",
  });
}
