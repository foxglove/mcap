import { pathToFileURL } from "node:url";
import { readFileSync } from "node:fs";

const artifact = process.argv[2];
if (!artifact) {
  console.error("usage: node scripts/run-fixtures.mjs <fixtures.mjs>");
  process.exit(2);
}

const source = readFileSync(artifact, "utf8");
const generated = await import(pathToFileURL(artifact).href);
const names = Object.keys(generated.instantiateKotoba({})).filter((n) =>
  n.startsWith("test-"),
);
if (names.length === 0) {
  console.error("no test-* exports");
  process.exit(1);
}

let failed = 0;
for (const name of names.sort()) {
  const api = generated.instantiateKotoba({});
  let value;
  try {
    value = api[name]();
  } catch (err) {
    console.log(`FAIL ${name} threw ${err.message}`);
    failed += 1;
    continue;
  }
  if (value === 1n) {
    console.log(`PASS ${name}`);
  } else {
    console.log(`FAIL ${name} => ${value}`);
    failed += 1;
  }
}

if (!source.includes("kotobaArtifact")) {
  console.error("compiled artifact missing kotobaArtifact");
  process.exit(1);
}

console.log(`${names.length - failed}/${names.length} passed`);
process.exit(failed === 0 ? 0 : 1);
