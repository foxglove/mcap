import struct Foundation.Data

public struct CRC32 {
  private var state: UInt32 = 0xffff_ffff

  public mutating func reset() {
    state = 0xffff_ffff
  }

  public mutating func update<S: Sequence>(_ data: S) where S.Element == UInt8 {
    for byte in data {
      state ^= UInt32(byte)
      for _ in 0..<8 {
        state = (state >> 1) ^ ((state & 1) * 0xedb8_8320)
      }
    }
  }

  public var final: UInt32 {
    return state ^ 0xffff_ffff
  }
}
