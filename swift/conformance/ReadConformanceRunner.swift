// swiftlint:disable force_try

import Foundation
import MCAP

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

private func readStreamed(file: FileHandle) throws -> Data {
  let reader = MCAPStreamedReader()
  var records: [Record] = []
  while case let data = file.readData(ofLength: 4 * 1024), data.count != 0 {
    reader.append(data)
    while let record = try reader.nextRecord() {
      if !(record is MessageIndex) {
        records.append(record)
      }
    }
  }
  let data = try JSONSerialization.data(withJSONObject: ["records": records.map(toJson)], options: .prettyPrinted)
  return data
}

private func readIndexed(file: FileHandle) throws -> Data {
  let reader = try MCAPRandomAccessReader(FileHandleReadable(fileHandle: file))
  let schemas: [Record] = reader.schemasById.values.map { $0 }.sorted { $0.id < $1.id }
  let channels: [Record] = reader.channelsById.values.map { $0 }.sorted { $0.id < $1.id }
  var messages: [Record] = []
  let iterator = reader.messageIterator()
  while let message = try iterator.next() {
    messages.append(message)
  }
  var statistics: [Record] = []
  if let statisticRecord = reader.statistics {
    statistics.append(statisticRecord)
  }

  let data = try JSONSerialization.data(
    withJSONObject: [
      "schemas": schemas.map(toJson),
      "channels": channels.map(toJson),
      "messages": messages.map(toJson),
      "statistics": statistics.map(toJson),
    ],
    options: .prettyPrinted
  )
  return data
}

struct FileHandleReadable: IRandomAccessReadable {
  let fileHandle: FileHandle

  func size() -> UInt64 {
    if #available(macOS 10.15.4, *) {
      return try! fileHandle.seekToEnd()
    } else {
      return fileHandle.seekToEndOfFile()
    }
  }

  func read(offset: UInt64, length: UInt64) -> Data? {
    do {
      if #available(macOS 10.15.4, *) {
        try fileHandle.seek(toOffset: offset)
      } else {
        fileHandle.seek(toFileOffset: offset)
      }
      return fileHandle.readData(ofLength: Int(length))
    } catch {
      return nil
    }
  }
}

enum ReadConformanceRunner {
  enum Mode {
    case streamed
    case indexed
  }

  static func main(mode: Mode) async throws {
    if CommandLine.arguments.count < 3 {
      fatalError("Usage: conformance read [test-data.mcap]")
    }
    let filename = CommandLine.arguments[2]
    let file = try FileHandle(forReadingFrom: URL(fileURLWithPath: filename))

    var data = Data()
    switch mode {
    case .streamed:
      data = try readStreamed(file: file)
    case .indexed:
      data = try readIndexed(file: file)
    }

    if #available(macOS 10.15.4, *) {
      try FileHandle.standardOutput.write(contentsOf: data)
    } else {
      FileHandle.standardOutput.write(data)
    }
  }
}
