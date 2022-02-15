// DataView.getBigUint64 was added to relatively recent versions of Safari. It's pretty easy to
// maintain this fallback code.
//
// eslint-disable-next-line @foxglove/no-boolean-parameters
export const getBigUint64: (this: DataView, offset: number, littleEndian?: boolean) => bigint =
  typeof DataView.prototype.getBigUint64 === "function"
    ? DataView.prototype.getBigUint64 // eslint-disable-line @typescript-eslint/unbound-method
    : function (this: DataView, offset, littleEndian): bigint {
        const lo =
          littleEndian === true
            ? this.getUint32(offset, littleEndian)
            : this.getUint32(offset + 4, littleEndian);
        const hi =
          littleEndian === true
            ? this.getUint32(offset + 4, littleEndian)
            : this.getUint32(offset, littleEndian);
        return (BigInt(hi) << 32n) | BigInt(lo);
      };
