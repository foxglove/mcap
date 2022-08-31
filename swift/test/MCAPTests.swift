// swiftlint:disable force_cast

import XCTest

import crc
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
  func testEmpty() async throws {
    let buffer = Buffer()
    let writer = MCAPWriter(buffer)
    await writer.start(library: "", profile: "")
    await writer.end()
    XCTAssertEqual(buffer.data.count, 286)
  }

  func testValidatesChunkCRC() async throws {
    var buffer = Data()
    buffer.append(MCAP0_MAGIC)
    Header(profile: "", library: "").serialize(to: &buffer)
    Chunk(
      messageStartTime: 0,
      messageEndTime: 0,
      uncompressedSize: 0,
      uncompressedCRC: 12345,
      compression: "",
      records: Data([1, 2, 3])
    ).serialize(to: &buffer)
    DataEnd(dataSectionCRC: 0).serialize(to: &buffer)
    Footer(summaryStart: 0, summaryOffsetStart: 0, summaryCRC: 0).serialize(to: &buffer)

    let reader = MCAPStreamedReader()
    reader.append(buffer)
    let header = try reader.nextRecord() as! Header
    XCTAssertEqual(header.profile, "")
    XCTAssertEqual(header.library, "")
    XCTAssertThrowsError(try reader.nextRecord()) {
      XCTAssertEqual($0 as! MCAPReadError, MCAPReadError.invalidCRC(expected: 12345, actual: 1_438_416_925))
    }
  }
}
