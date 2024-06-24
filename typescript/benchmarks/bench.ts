import { hrtime, memoryUsage } from "process";

const COUNT = 5;

type BenchmarkResult =
  | {
      name: string;
      gcExposed: true;
      samples: {
        duration: bigint;
        memoryUsage: {
          rss: number;
          heapTotal: number;
          heapUsed: number;
          external: number;
          arrayBuffers: number;
        };
      }[];
    }
  | {
      name: string;
      gcExposed: false;
      samples: {
        duration: bigint;
      }[];
    };

/** runs a benchmark and logs statistics about runtime and memory usage afterward.
 *
 * @param name A name for the benchmark.
 * @param run a routine that runs the benchmark code.
 */
export async function runBenchmark(name: string, run: () => Promise<void>): Promise<void> {
  let result: BenchmarkResult;
  if (global.gc != undefined) {
    result = {
      name,
      gcExposed: true,
      samples: [],
    };
    for (let i = 0; i < COUNT; i++) {
      global.gc();
      const before = hrtime.bigint();
      await run();
      const after = hrtime.bigint();
      result.samples.push({
        duration: after - before,
        memoryUsage: memoryUsage(),
      });
    }
  } else {
    result = {
      name,
      gcExposed: false,
      samples: [],
    };
    for (let i = 0; i < COUNT; i++) {
      const before = hrtime.bigint();
      await run();
      const after = hrtime.bigint();
      result.samples.push({ duration: after - before });
    }
  }
  printStats(result);
}

function humanReadableStatistics(values: number[], unit: string): string {
  const count = values.length;
  if (count < 1) {
    return "(No samples)";
  }
  if (count < 2) {
    return `${values[0]} ${unit}`;
  }
  const mean = values.reduce((a, b) => a + b, 0) / count;
  const stdDev = Math.sqrt(
    values.map((value) => (mean - value) ** 2).reduce((a, b) => a + b, 0) / (count - 1),
  );
  const stdErr = stdDev / Math.sqrt(count);
  return `${mean.toFixed(2)}±${stdErr.toFixed(2)} ${unit}`;
}

function printStats(result: BenchmarkResult) {
  let memoryResult = "(use --expose-gc to gather memory statistics)";
  if (result.gcExposed) {
    const used = humanReadableStatistics(
      result.samples.map((sample) => sample.memoryUsage.heapUsed / 2 ** 20),
      "MB/op",
    );
    const total = humanReadableStatistics(
      result.samples.map((sample) => sample.memoryUsage.heapTotal / 2 ** 20),
      "MB/op",
    );
    const arrayBuffers = humanReadableStatistics(
      result.samples.map((sample) => sample.memoryUsage.arrayBuffers / 2 ** 20),
      "MB/op",
    );
    memoryResult = `Heap Used: ${used}\tHeap Total: ${total}\tArrayBuffers: ${arrayBuffers}`;
  }
  const name = result.name;
  const timeStat = humanReadableStatistics(
    result.samples.map((r) => 1 / (Number(r.duration) / 1e9)),
    "op/s",
  );
  console.log(name);
  console.log(`\t${timeStat}\t${memoryResult}`);
}
