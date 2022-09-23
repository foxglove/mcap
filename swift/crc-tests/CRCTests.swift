import CRC
import XCTest

final class CRC32Tests: XCTestCase {
  func testKnownValues() {
    XCTAssertEqual(CRC32().final, 0)

    var crc = CRC32()
    crc.update(Data([1]))
    XCTAssertEqual(crc.final, 2_768_625_435)
  }

  func testUnaligned() {
    var crc = CRC32()

    for offset in 0 ..< 8 {
      let padding = [UInt8](repeating: 42, count: offset)

      crc.reset()
      Data(padding + [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17]).withUnsafeBytes { buf in
        crc.update(UnsafeRawBufferPointer(rebasing: buf[offset...]))
      }
      XCTAssertEqual(crc.final, 1_912_684_917)

      crc.reset()
      Data(padding + [1]).withUnsafeBytes { buf in
        crc.update(UnsafeRawBufferPointer(rebasing: buf[offset...]))
      }
      XCTAssertEqual(crc.final, 2_768_625_435)
    }
  }
}
