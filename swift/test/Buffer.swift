import struct Foundation.Data
import MCAP

class Buffer: IWritable, IRandomAccessReadable {
  var data = Data()

  func position() -> UInt64 {
    UInt64(data.count)
  }

  func write(_ other: Data) async {
    data.append(other)
  }

  func size() -> UInt64 {
    UInt64(data.count)
  }

  func read(offset: UInt64, length: UInt64) -> Data? {
    if Int(offset + length) > data.count {
      return nil
    }
    return data[Int(offset) ..< Int(offset + length)]
  }
}
