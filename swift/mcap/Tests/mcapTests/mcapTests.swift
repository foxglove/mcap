import XCTest
@testable import mcap

final class mcapTests: XCTestCase {
  func testExample() throws {
    struct Writable: IWritable {
      func write(_ data: Data) async {

      }
    }
    let writer = MCAP0Writer(Writable())
    _ = writer
  }
}
