import { sortedLastIndexBy } from "./sortedLastIndex";

describe("sortedLastIndexBy", () => {
  it("handles an empty array", () => {
    const array: [number, number][] = [];

    expect(sortedLastIndexBy(array, 0, (x) => x)).toEqual(0);
    expect(sortedLastIndexBy(array, 42, (x) => x)).toEqual(0);
  });

  it("handles a contiguous array", () => {
    const array: [number, number][] = [
      [1, 42],
      [2, 42],
      [3, 42],
    ];

    expect(sortedLastIndexBy(array, 0, (x) => x)).toEqual(0);
    expect(sortedLastIndexBy(array, 1, (x) => x)).toEqual(1);
    expect(sortedLastIndexBy(array, 2, (x) => x)).toEqual(2);
    expect(sortedLastIndexBy(array, 3, (x) => x)).toEqual(3);
    expect(sortedLastIndexBy(array, 4, (x) => x)).toEqual(3);
  });

  it("handles a sparse array", () => {
    const array: [number, number][] = [
      [1, 42],
      [3, 42],
    ];

    expect(sortedLastIndexBy(array, 0, (x) => x)).toEqual(0);
    expect(sortedLastIndexBy(array, 1, (x) => x)).toEqual(1);
    expect(sortedLastIndexBy(array, 2, (x) => x)).toEqual(1);
    expect(sortedLastIndexBy(array, 3, (x) => x)).toEqual(2);
    expect(sortedLastIndexBy(array, 4, (x) => x)).toEqual(2);
  });

  it("handles negation", () => {
    const array: [number, number][] = [
      [1, 42],
      [2, 42],
      [3, 42],
      [4, 42],
    ];

    expect(sortedLastIndexBy(array, 0, (x) => -x)).toEqual(4);
    expect(sortedLastIndexBy(array, 1, (x) => -x)).toEqual(4);
    expect(sortedLastIndexBy(array, 2, (x) => -x)).toEqual(4);
    expect(sortedLastIndexBy(array, 3, (x) => -x)).toEqual(4);
    expect(sortedLastIndexBy(array, 4, (x) => -x)).toEqual(0);
    expect(sortedLastIndexBy(array, 5, (x) => -x)).toEqual(0);
  });

  it("handles a contiguous array with duplicate times", () => {
    const array: [number, number][] = [
      [1, 42],
      [2, 42],
      [2, 42],
      [2, 42],
      [3, 42],
      [3, 42],
    ];

    expect(sortedLastIndexBy(array, 0, (x) => x)).toEqual(0);
    expect(sortedLastIndexBy(array, 1, (x) => x)).toEqual(1);
    expect(sortedLastIndexBy(array, 2, (x) => x)).toEqual(4);
    expect(sortedLastIndexBy(array, 3, (x) => x)).toEqual(6);
  });
});
