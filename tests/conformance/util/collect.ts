/**
 * Collect all results of an AsyncIterable into an array (like Array.from(), but works with AsyncIterables).
 */
export async function collect<T>(iterable: AsyncIterable<T>): Promise<T[]> {
  const result: T[] = [];
  for await (const item of iterable) {
    result.push(item);
  }
  return result;
}
