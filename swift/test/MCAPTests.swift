import XCTest

import mcap

class Buffer: IWritable {
  var data = Data()

  func position() -> UInt64 {
    UInt64(data.count)
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
