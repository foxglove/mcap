import { normalizeText } from "./textNormalize.ts";

export function normalizeTable(value: string): string {
  const lines = normalizeText(value, { trim: true })
    .split("\n")
    .map((line) => line.trim())
    .filter((line) => line.length > 0);

  const rows = lines.map((line) => {
    const cells = line.includes("\t") ? line.split(/\t+/u) : line.split(/ {2,}/u);
    return cells.map((cell) => cell.trim().replaceAll(/[ \t]+/g, " "));
  });

  return rows.map((row) => row.join("\t")).join("\n");
}
