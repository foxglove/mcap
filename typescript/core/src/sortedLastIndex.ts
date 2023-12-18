/**
 * Return the lowest index of `array` where an element can be inserted and maintain its sorted
 * order. This is a specialization of lodash's sortedIndexBy().
 */

export function sortedLastIndexBy(
  array: [bigint, bigint][],
  value: bigint,
  iteratee: (value: bigint) => bigint,
): number {
  let low = 0;
  let high = array.length;
  if (high === 0) {
    return 0;
  }

  const computedValue = iteratee(value);

  while (low < high) {
    const mid = (low + high) >>> 1;
    const computed = iteratee(array[mid]![0]);

    if (computed <= computedValue) {
      low = mid + 1;
    } else {
      high = mid;
    }
  }
  return high;
}
