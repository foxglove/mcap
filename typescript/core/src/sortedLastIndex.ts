import { NsTimestamp } from "./types";

/**
 * Return the lowest index of `array` where an element can be inserted and maintain its sorted
 * order. This is a specialization of lodash's sortedIndexBy().
 */
export function sortedLastIndexBy(
  array: [NsTimestamp, number][],
  value: NsTimestamp,
  compare: (a: NsTimestamp, b: NsTimestamp) => number,
): number {
  let low = 0;
  let high = array.length;
  if (high === 0) {
    return 0;
  }

  while (low < high) {
    const mid = (low + high) >>> 1;
    if (compare(array[mid]![0], value) <= 0) {
      low = mid + 1;
    } else {
      high = mid;
    }
  }
  return high;
}
