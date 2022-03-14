import XCTest

@testable import mcap

class Buffer: IWritable {
  var data = Data()

  func position() -> UInt64 {
    return UInt64(data.count)
  }
  func write(_ other: Data) async {
    data.append(other)
  }
}

final class MCAPTests: XCTestCase {
  func testExample() async throws {

    let buffer = Buffer()
    let writer = MCAPWriter(buffer)
    await writer.start(library: "", profile: "")
    await writer.end()
    XCTAssertEqual(buffer.data.count, 286)
  }
}

final class CRC32Tests: XCTestCase {
  func testKnownValues() {
    XCTAssertEqual(CRC32().final, 0)

    var crc = CRC32()
    crc.update(Data([1]))
    XCTAssertEqual(crc.final, 2_768_625_435)
  }
}
