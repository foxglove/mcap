import { sortedIndexBy } from "./sortedIndexBy.ts";

describe("sortedIndexBy", () => {
  it("handles an empty array", () => {
    const array: [bigint, bigint][] = [];

    expect(sortedIndexBy(array, 0n, (x) => x)).toEqual(0);
    expect(sortedIndexBy(array, 42n, (x) => x)).toEqual(0);
  });

  it("handles a contiguous array", () => {
    const array: [bigint, bigint][] = [
      [1n, 42n],
      [2n, 42n],
      [3n, 42n],
    ];

    expect(sortedIndexBy(array, 0n, (x) => x)).toEqual(0);
    expect(sortedIndexBy(array, 1n, (x) => x)).toEqual(0);
    expect(sortedIndexBy(array, 2n, (x) => x)).toEqual(1);
    expect(sortedIndexBy(array, 3n, (x) => x)).toEqual(2);
    expect(sortedIndexBy(array, 4n, (x) => x)).toEqual(3);
  });

  it("handles a sparse array", () => {
    const array: [bigint, bigint][] = [
      [1n, 42n],
      [3n, 42n],
    ];

    expect(sortedIndexBy(array, 0n, (x) => x)).toEqual(0);
    expect(sortedIndexBy(array, 1n, (x) => x)).toEqual(0);
    expect(sortedIndexBy(array, 2n, (x) => x)).toEqual(1);
    expect(sortedIndexBy(array, 3n, (x) => x)).toEqual(1);
    expect(sortedIndexBy(array, 4n, (x) => x)).toEqual(2);
  });

  it("handles negation", () => {
    const array: [bigint, bigint][] = [
      [1n, 42n],
      [2n, 42n],
      [3n, 42n],
      [4n, 42n],
    ];

    expect(sortedIndexBy(array, 0n, (x) => -x)).toEqual(4);
    expect(sortedIndexBy(array, 1n, (x) => -x)).toEqual(4);
    expect(sortedIndexBy(array, 2n, (x) => -x)).toEqual(4);
    expect(sortedIndexBy(array, 3n, (x) => -x)).toEqual(0);
    expect(sortedIndexBy(array, 4n, (x) => -x)).toEqual(0);
    expect(sortedIndexBy(array, 5n, (x) => -x)).toEqual(0);
  });
});
