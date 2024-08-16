import { NsTimestamp } from "./types";

/**
 * Return the lowest index of `array` where an element can be inserted and maintain its sorted
 * order. This is a specialization of lodash's sortedIndexBy().
 */
export function sortedIndexBy(
  array: [NsTimestamp, number][],
  value: NsTimestamp,
  iteratee: (value: NsTimestamp) => number,
): number {
  let low = 0;
  let high = array.length;
  if (high === 0) {
    return 0;
  }

  const computedValue = iteratee(value);

  while (low < high) {
    const mid = (low + high) >>> 1;
    const curComputedValue = iteratee(array[mid]![0]);

    if (curComputedValue < computedValue) {
      low = mid + 1;
    } else {
      high = mid;
    }
  }
  return high;
}
