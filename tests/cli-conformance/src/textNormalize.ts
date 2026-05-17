const ANSI_PATTERN = new RegExp(`${String.fromCharCode(27)}\\[[0-9;?]*[ -/]*[@-~]`, "g");

export function stripAnsi(value: string): string {
  return value.replace(ANSI_PATTERN, "");
}

export function bufferToUtf8(buffer: Buffer): string {
  return buffer.toString("utf8");
}

export function normalizeText(
  value: string,
  options: {
    collapseWhitespace?: boolean;
    trim?: boolean;
    ignoreAnsi?: boolean;
  } = {},
): string {
  let normalized = options.ignoreAnsi === false ? value : stripAnsi(value);
  normalized = normalized.replaceAll("\r\n", "\n").replaceAll("\r", "\n");
  normalized = normalized
    .split("\n")
    .map((line) => line.trimEnd())
    .join("\n");
  if (options.collapseWhitespace === true) {
    normalized = normalized
      .split("\n")
      .map((line) => line.trim().replaceAll(/[ \t]+/g, " "))
      .join("\n");
  }
  if (options.trim !== false) {
    normalized = normalized.trim();
  }
  return normalized;
}
