export function sortKvPairs(arr: BigUint64Array, reverse: boolean = false): void {
    quicksort(arr as unknown as BigInt[], reverse ? reverseCompare : defaultCompare);
}

function defaultCompare<T>(x1: T, y1: T, x2: T, y2: T): number {
    if (x1 < x2) return -1;
    if (x1 > x2) return 1;
    if (y1 < y2) return -1;
    if (y1 > y2) return 1;
    return 0;
}

function reverseCompare<T>(x1: T, y1: T, x2: T, y2: T): number {
    return -defaultCompare(x1, y1, x2, y2);
}

function quicksort<T>(
    arr: T[],
    compare: (x1: T, y1: T, x2: T, y2: T) => number = defaultCompare,
    left = 0,
    right = arr.length - 2
): void {
    while (left < right) {
        const pivotIndex = partition(arr, compare, left, right);
        if (pivotIndex - left < right - pivotIndex) {
            quicksort(arr, compare, left, pivotIndex - 2);
            left = pivotIndex + 2;
        } else {
            quicksort(arr, compare, pivotIndex + 2, right);
            right = pivotIndex - 2;
        }
    }
}

function partition<T>(
    arr: T[],
    compare: (x1: T, y1: T, x2: T, y2: T) => number,
    left: number,
    right: number
): number {
    const pivotIndex = right;
    const pivotX = arr[pivotIndex]!;
    const pivotY = arr[pivotIndex + 1]!;
    let i = left - 2;

    for (let j = left; j < right; j += 2) {
        const currentX = arr[j]!;
        const currentY = arr[j + 1]!;
        if (compare(currentX, currentY, pivotX, pivotY) < 0) {
            i += 2;
            swapPair(arr, i, j);
        }
    }

    swapPair(arr, i + 2, pivotIndex);
    return i + 2;
}

function swapPair<T>(arr: T[], i: number, j: number): void {
    const tempX = arr[i]!;
    const tempY = arr[i + 1]!;
    arr[i] = arr[j]!;
    arr[i + 1] = arr[j + 1]!;
    arr[j] = tempX;
    arr[j + 1] = tempY;
}
