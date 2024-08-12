import { sortedIndexBy } from "./sortedIndexBy";

describe("sortedIndexBy", () => {
  it("handles an empty array", () => {
    const array: [number, number][] = [];

    expect(sortedIndexBy(array, 0, (x) => x)).toEqual(0);
    expect(sortedIndexBy(array, 42, (x) => x)).toEqual(0);
  });

  it("handles a contiguous array", () => {
    const array: [number, number][] = [
      [1, 42],
      [2, 42],
      [3, 42],
    ];

    expect(sortedIndexBy(array, 0, (x) => x)).toEqual(0);
    expect(sortedIndexBy(array, 1, (x) => x)).toEqual(0);
    expect(sortedIndexBy(array, 2, (x) => x)).toEqual(1);
    expect(sortedIndexBy(array, 3, (x) => x)).toEqual(2);
    expect(sortedIndexBy(array, 4, (x) => x)).toEqual(3);
  });

  it("handles a sparse array", () => {
    const array: [number, number][] = [
      [1, 42],
      [3, 42],
    ];

    expect(sortedIndexBy(array, 0, (x) => x)).toEqual(0);
    expect(sortedIndexBy(array, 1, (x) => x)).toEqual(0);
    expect(sortedIndexBy(array, 2, (x) => x)).toEqual(1);
    expect(sortedIndexBy(array, 3, (x) => x)).toEqual(1);
    expect(sortedIndexBy(array, 4, (x) => x)).toEqual(2);
  });

  it("handles negation", () => {
    const array: [number, number][] = [
      [1, 42],
      [2, 42],
      [3, 42],
      [4, 42],
    ];

    expect(sortedIndexBy(array, 0, (x) => -x)).toEqual(4);
    expect(sortedIndexBy(array, 1, (x) => -x)).toEqual(4);
    expect(sortedIndexBy(array, 2, (x) => -x)).toEqual(4);
    expect(sortedIndexBy(array, 3, (x) => -x)).toEqual(0);
    expect(sortedIndexBy(array, 4, (x) => -x)).toEqual(0);
    expect(sortedIndexBy(array, 5, (x) => -x)).toEqual(0);
  });
});
