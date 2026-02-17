import { sortedLastIndexBy } from "./sortedLastIndex.ts";

describe("sortedLastIndexBy", () => {
  it("handles an empty array", () => {
    const array: [bigint, bigint][] = [];

    expect(sortedLastIndexBy(array, 0n, (x) => x)).toEqual(0);
    expect(sortedLastIndexBy(array, 42n, (x) => x)).toEqual(0);
  });

  it("handles a contiguous array", () => {
    const array: [bigint, bigint][] = [
      [1n, 42n],
      [2n, 42n],
      [3n, 42n],
    ];

    expect(sortedLastIndexBy(array, 0n, (x) => x)).toEqual(0);
    expect(sortedLastIndexBy(array, 1n, (x) => x)).toEqual(1);
    expect(sortedLastIndexBy(array, 2n, (x) => x)).toEqual(2);
    expect(sortedLastIndexBy(array, 3n, (x) => x)).toEqual(3);
    expect(sortedLastIndexBy(array, 4n, (x) => x)).toEqual(3);
  });

  it("handles a sparse array", () => {
    const array: [bigint, bigint][] = [
      [1n, 42n],
      [3n, 42n],
    ];

    expect(sortedLastIndexBy(array, 0n, (x) => x)).toEqual(0);
    expect(sortedLastIndexBy(array, 1n, (x) => x)).toEqual(1);
    expect(sortedLastIndexBy(array, 2n, (x) => x)).toEqual(1);
    expect(sortedLastIndexBy(array, 3n, (x) => x)).toEqual(2);
    expect(sortedLastIndexBy(array, 4n, (x) => x)).toEqual(2);
  });

  it("handles negation", () => {
    const array: [bigint, bigint][] = [
      [1n, 42n],
      [2n, 42n],
      [3n, 42n],
      [4n, 42n],
    ];

    expect(sortedLastIndexBy(array, 0n, (x) => -x)).toEqual(4);
    expect(sortedLastIndexBy(array, 1n, (x) => -x)).toEqual(4);
    expect(sortedLastIndexBy(array, 2n, (x) => -x)).toEqual(4);
    expect(sortedLastIndexBy(array, 3n, (x) => -x)).toEqual(4);
    expect(sortedLastIndexBy(array, 4n, (x) => -x)).toEqual(0);
    expect(sortedLastIndexBy(array, 5n, (x) => -x)).toEqual(0);
  });

  it("handles a contiguous array with duplicate times", () => {
    const array: [bigint, bigint][] = [
      [1n, 42n],
      [2n, 42n],
      [2n, 42n],
      [2n, 42n],
      [3n, 42n],
      [3n, 42n],
    ];

    expect(sortedLastIndexBy(array, 0n, (x) => x)).toEqual(0);
    expect(sortedLastIndexBy(array, 1n, (x) => x)).toEqual(1);
    expect(sortedLastIndexBy(array, 2n, (x) => x)).toEqual(4);
    expect(sortedLastIndexBy(array, 3n, (x) => x)).toEqual(6);
  });
});
