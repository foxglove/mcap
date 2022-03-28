import Foundation
import mcap

private extension String {
  func camelToSnake() -> String {
    var result = ""
    var wordStart = startIndex
    while wordStart != endIndex {
      var wordEnd = index(after: wordStart)
      while wordEnd != endIndex, self[wordEnd].isUppercase {
        // handle all-uppercase words at the end of the string, e.g. schemaID and dataSectionCRC
        // (does not handle correctly if the all-uppercase word is followed by another word)
        formIndex(after: &wordEnd)
      }
      while wordEnd != endIndex, self[wordEnd].isLowercase {
        formIndex(after: &wordEnd)
      }
      if !result.isEmpty {
        result.append("_")
      }
      result.append(self[wordStart ..< wordEnd].lowercased())
      wordStart = wordEnd
    }
    return result
  }
}

private func toJson(_ record: Record) -> [String: Any] {
  let mirror = Mirror(reflecting: record)
  var fields: [String: Any] = [:]
  for child in mirror.children {
    var jsonValue: Any
    switch child.value {
    case let value as String:
      jsonValue = value
    case let value as Data:
      jsonValue = value.map { String($0) }
    case let value as UInt8:
      jsonValue = String(value)
    case let value as UInt16:
      jsonValue = String(value)
    case let value as UInt32:
      jsonValue = String(value)
    case let value as UInt64:
      jsonValue = String(value)
    case let value as [(UInt64, UInt64)]:
      jsonValue = value.map { [String($0.0), String($0.1)] }
    case let value as [String: String]:
      jsonValue = value
    case let value as [UInt16: UInt64]:
      jsonValue = Dictionary(uniqueKeysWithValues: value.map { (String($0.key), String($0.value)) })
    default:
      fatalError("Unhandled type \(type(of: child.value))")
    }
    fields[child.label!.camelToSnake()] = jsonValue
  }
  return [
    "type": String(describing: mirror.subjectType),
    "fields": fields.sorted(by: { $0.key < $1.key }).map { [$0.key, $0.value] },
  ]
}

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
        if !(record is MessageIndex) {
          records.append(record)
        }
      }
    }

    let data = try JSONSerialization.data(withJSONObject: ["records": records.map(toJson)], options: .prettyPrinted)

    if #available(macOS 10.15.4, *) {
      try FileHandle.standardOutput.write(contentsOf: data)
    } else {
      FileHandle.standardOutput.write(data)
    }
  }
}
