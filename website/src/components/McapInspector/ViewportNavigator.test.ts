import assert from "node:assert/strict";
import { test } from "node:test";

import { viewportPercentages } from "./ViewportNavigator.tsx";

void test("overview band represents viewport start and span, not elapsed progress", () => {
  assert.deepEqual(viewportPercentages(10, 5, 20), { left: 50, width: 25 });
  assert.deepEqual(viewportPercentages(0, 20, 20), { left: 0, width: 100 });
  assert.deepEqual(viewportPercentages(0, 0, 0), { left: 0, width: 100 });
});
