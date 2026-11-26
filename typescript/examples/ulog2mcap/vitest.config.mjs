import { createRequire } from "node:module";
import path from "node:path";
import { defineConfig } from "vitest/config";

const require = createRequire(import.meta.url);

export default defineConfig({
  resolve: {
    alias: [
      // Test against the @mcap/* sources so that no build is needed first
      {
        find: /^@mcap\/(.*)$/,
        replacement: path.join(import.meta.dirname, "../../$1/src/index.ts"),
      },
      // protobufjs loads the CommonJS build of long. Load the same build in the tests so that
      // decoded values and expected values share one Long class.
      { find: /^long$/, replacement: require.resolve("long") },
    ],
  },
  test: {
    include: ["scripts/**/*.test.ts"],
  },
});
