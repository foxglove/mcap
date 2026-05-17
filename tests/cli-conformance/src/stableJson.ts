export function stableStringify(value: unknown, space = 2): string {
  return JSON.stringify(sortJsonValue(value), undefined, space);
}

function sortJsonValue(value: unknown): unknown {
  if (value == undefined || typeof value !== "object") {
    return value;
  }
  if (Array.isArray(value)) {
    return value.map(sortJsonValue);
  }
  return Object.fromEntries(
    Object.entries(value)
      .sort(([left], [right]) => left.localeCompare(right))
      .map(([key, entryValue]) => [key, sortJsonValue(entryValue)]),
  );
}
