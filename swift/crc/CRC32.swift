import struct Foundation.Data

// iOS seems to lack CRC intrinsics
// See also: https://stackoverflow.com/questions/45625725/does-clang-lack-crc32-for-armv8-aarch64
#if (arch(arm) || arch(arm64)) && !os(iOS)
  import _Builtin_intrinsics.arm.acle // cspell:disable-line
#endif

// swiftlint:disable identifier_name

public struct CRC32 {
  public static let polynomial: UInt32 = 0xEDB8_8320

  @usableFromInline
  internal var state: UInt32 = 0xFFFF_FFFF

  public init() {}

  public mutating func reset() {
    state = 0xFFFF_FFFF
  }

  public var final: UInt32 {
    state ^ 0xFFFF_FFFF
  }

  @inlinable
  public mutating func update(_ data: Data) {
    data.withUnsafeBytes { self.update($0) }
  }

  @inlinable
  public mutating func update(_ slice: Slice<UnsafeRawBufferPointer>) {
    self.update(UnsafeRawBufferPointer(rebasing: slice))
  }

  @inlinable
  public mutating func update(_ data: UnsafeRawBufferPointer) {
    #if (arch(arm) || arch(arm64)) && !os(iOS)
      self.updateARM(data)
    #else
      self.update16Byte(data)
    #endif
  }

  /// 16-byte tabular update algorithm from: https://github.com/komrad36/CRC#option-10-16-byte-tabular
  @inlinable
  internal mutating func update16Byte(_ data: UnsafeRawBufferPointer) {
    let lowByte: UInt32 = 0xFF
    let tableSize: UInt32 = 256

    var offset = 0
    let basePtr = Int(bitPattern: data.baseAddress)
    while offset < data.count, !(basePtr + offset).isMultiple(of: MemoryLayout<UInt32>.alignment) {
      state = CRC32.table[Int((state ^ UInt32(data[offset])) & lowByte)] ^ (state >> 8 as UInt32)
      offset += 1
    }
    if offset == data.count {
      return
    }

    let array32 = UnsafeRawBufferPointer(rebasing: data[offset...]).bindMemory(to: UInt32.self)
    var i = 0
    var remainingBytes = data.count - offset
    while remainingBytes >= 16 {
      state ^= array32[i]
      i += 1
      let r2 = array32[i]
      i += 1
      let r3 = array32[i]
      i += 1
      let r4 = array32[i]
      i += 1
      let t0 = CRC32.table[Int((0 as UInt32 * tableSize) + ((r4 >> 24 as UInt32) & lowByte))]
      let t1 = CRC32.table[Int((1 as UInt32 * tableSize) + ((r4 >> 16 as UInt32) & lowByte))]
      let t2 = CRC32.table[Int((2 as UInt32 * tableSize) + ((r4 >> 8 as UInt32) & lowByte))]
      let t3 = CRC32.table[Int((3 as UInt32 * tableSize) + ((r4 >> 0 as UInt32) & lowByte))]
      let t4 = CRC32.table[Int((4 as UInt32 * tableSize) + ((r3 >> 24 as UInt32) & lowByte))]
      let t5 = CRC32.table[Int((5 as UInt32 * tableSize) + ((r3 >> 16 as UInt32) & lowByte))]
      let t6 = CRC32.table[Int((6 as UInt32 * tableSize) + ((r3 >> 8 as UInt32) & lowByte))]
      let t7 = CRC32.table[Int((7 as UInt32 * tableSize) + ((r3 >> 0 as UInt32) & lowByte))]
      let t8 = CRC32.table[Int((8 as UInt32 * tableSize) + ((r2 >> 24 as UInt32) & lowByte))]
      let t9 = CRC32.table[Int((9 as UInt32 * tableSize) + ((r2 >> 16 as UInt32) & lowByte))]
      let ta = CRC32.table[Int((10 as UInt32 * tableSize) + ((r2 >> 8 as UInt32) & lowByte))]
      let tb = CRC32.table[Int((11 as UInt32 * tableSize) + ((r2 >> 0 as UInt32) & lowByte))]
      let tc = CRC32.table[Int((12 as UInt32 * tableSize) + ((state >> 24 as UInt32) & lowByte))]
      let td = CRC32.table[Int((13 as UInt32 * tableSize) + ((state >> 16 as UInt32) & lowByte))]
      let te = CRC32.table[Int((14 as UInt32 * tableSize) + ((state >> 8 as UInt32) & lowByte))]
      let tf = CRC32.table[Int((15 as UInt32 * tableSize) + ((state >> 0 as UInt32) & lowByte))]
      state = t0 ^ t1 ^ t2 ^ t3 ^ t4 ^ t5 ^ t6 ^ t7 ^ t8 ^ t9 ^ ta ^ tb ^ tc ^ td ^ te ^ tf
      remainingBytes -= 16
    }
    for byte in data[data.count - remainingBytes ..< data.count] {
      state = CRC32.table[Int((state ^ UInt32(byte)) & lowByte)] ^ (state >> 8 as UInt32)
    }
  }

  #if (arch(arm) || arch(arm64)) && !os(iOS)
    @inlinable
    internal mutating func updateARM(_ data: UnsafeRawBufferPointer) {
      var offset = 0
      let basePtr = Int(bitPattern: data.baseAddress)
      while offset < data.count, !(basePtr + offset).isMultiple(of: MemoryLayout<UInt64>.alignment) {
        state = __crc32b(state, data[offset])
        offset += 1
      }
      if offset == data.count {
        return
      }
      let array64 = UnsafeRawBufferPointer(rebasing: data[offset...]).bindMemory(to: UInt64.self)
      var remainingBytes = data.count - offset
      for val in array64 {
        state = __crc32d(state, val)
        remainingBytes -= 8
      }
      for byte in data[data.count - remainingBytes ..< data.count] {
        state = __crc32b(state, byte)
      }
    }
  #endif
}

extension CRC32 {
  @usableFromInline
  internal static let table = generateTables(16)

  private static func generateTables(_ numTables: Int) -> [UInt32] {
    var table = [UInt32](repeating: 0, count: numTables * 256)
    for i in 0 ..< 256 {
      var r = UInt32(i)
      for _ in 0 ..< 8 {
        r = ((r & 1 as UInt32) * CRC32.polynomial) ^ (r >> 1 as UInt32)
      }
      table[i] = r
    }
    for i in 256 ..< table.count {
      let value = table[i - 256]
      table[i] = table[Int(value & 0xFF as UInt32)] ^ (value >> 8 as UInt32)
    }
    return table
  }
}
