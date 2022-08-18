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
    Header(profile: "", library: "").serializeFields(to: &buffer)
    Chunk(messageStartTime: 0, messageEndTime: 0, uncompressedSize: 0, uncompressedCRC: 12345, compression: "", records: Data([1, 2, 3])).serializeFields(to: &buffer)
    DataEnd(dataSectionCRC: 0).serializeFields(to: &buffer)
    Footer(summaryStart: 0, summaryOffsetStart: 0, summaryCRC: 0).serializeFields(to: &buffer)

    let reader = MCAPStreamedReader()
    reader.append(buffer)
    XCTAssertThrowsError(try reader.nextRecord()) {
      XCTAssertEqual($0 as! MCAPReadError, MCAPReadError.invalidCRC)
    }
  }
}
