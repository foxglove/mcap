const N32 = 2 ** 32;
// DataView.getBigUint64 was added to relatively recent versions of Safari. It's pretty easy to
// maintain this fallback code.
//
// eslint-disable-next-line @foxglove/no-boolean-parameters
export function getBigUint64(view: DataView, offset: number, littleEndian?: boolean): number {
  const lo =
    littleEndian === true
      ? view.getUint32(offset, littleEndian)
      : view.getUint32(offset + 4, littleEndian);
  const hi =
    littleEndian === true
      ? view.getUint32(offset + 4, littleEndian)
      : view.getUint32(offset, littleEndian);
  return hi * N32 + lo;
}
