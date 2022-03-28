import Foundation
import mcap

// swiftlint:disable force_cast
enum ReadConformanceRunner {
  static func main() async throws {
    if CommandLine.arguments.count < 3 {
      fatalError("Usage: conformance read [test-data.mcap]")
    }
    let filename = CommandLine.arguments[2]
    let file = try FileHandle(forReadingFrom: URL(fileURLWithPath: filename))

    var records: [Record] = []
    let reader = MCAPStreamedReader()
    while case let data = file.readData(ofLength: 4 * 1024), data.count != 0 {
      reader.append(data)
      while let record = try reader.nextRecord() {
        records.append(record)
      }
    }

    let data = try JSONSerialization.data(withJSONObject: records.map { (record: Record) -> [String: Any] in
      var result: [String: Any] = [:]
      let m = Mirror(reflecting: record)
      for c in m.children {
        var label = c.label!
        switch c.value {
        case let x as String:
          result[label] = x
        case let d as Data:
          result[label] = d.map { String($0) }
        case let x as UInt8:
          result[label] = String(x)
        case let x as UInt16:
          result[label] = String(x)
        case let x as UInt32:
          result[label] = String(x)
        case let x as UInt64:
          result[label] = String(x)
        case let x as [(UInt64, UInt64)]:
          result[label] = x.map { [String($0.0), String($0.1)] }
        case let x as [String: String]:
          result[label] = x
        case let x as [UInt16: UInt64]:
          result[label] = Dictionary(uniqueKeysWithValues: x.map { (String($0.key), String($0.value)) })
        default:
          fatalError("Unhandled type \(type(of: c.value))")
        }
      }
      return result
    }, options: .prettyPrinted)

    if #available(macOS 10.15.4, *) {
      try FileHandle.standardOutput.write(contentsOf: data)
    } else {
      FileHandle.standardOutput.write(data)
    }
  }
}
