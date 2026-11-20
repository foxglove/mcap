// swiftlint:disable force_cast

import CRC
import Foundation
import MCAP
import Testing

struct MCAPTests {
  @Test
  func defaultLibraryIdentifier() async throws {
    let buffer = Buffer()
    let writer = MCAPWriter(buffer)
    await writer.start()
    await writer.end()

    let reader = MCAPStreamedReader()
    reader.append(buffer.data)
    let header = try reader.nextRecord() as! Header

    #expect(header.profile == "")
    #expect(header.library == mcapLibraryIdentifier)
    #expect(header.library.hasPrefix("mcap-swift/"))
  }

  @Test
  func empty() async throws {
    let buffer = Buffer()
    let writer = MCAPWriter(buffer)
    await writer.start(library: "", profile: "")
    await writer.end()
    #expect(buffer.data.count == 286)
  }

  @Test
  func validatesChunkCRC() async throws {
    var buffer = Data()
    buffer.append(mcapMagic)
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
    #expect(header.profile == "")
    #expect(header.library == "")
    let error = #expect(throws: MCAPReadError.self) { try reader.nextRecord() }
    #expect(error == MCAPReadError.invalidCRC(expected: 12345, actual: 1_438_416_925))
  }

  /// Builds a file whose only chunk holds `chunkRecords`, then returns the opcodes the streamed reader emits.
  private func streamedOpcodes(chunkRecords: Data) throws -> [Opcode] {
    var buffer = Data()
    buffer.append(mcapMagic)
    Header(profile: "", library: "").serialize(to: &buffer)
    Chunk(
      messageStartTime: 0,
      messageEndTime: 0,
      uncompressedSize: UInt64(chunkRecords.count),
      uncompressedCRC: 0,
      compression: "",
      records: chunkRecords
    ).serialize(to: &buffer)
    DataEnd(dataSectionCRC: 0).serialize(to: &buffer)
    Footer(summaryStart: 0, summaryOffsetStart: 0, summaryCRC: 0).serialize(to: &buffer)
    buffer.append(mcapMagic)

    let reader = MCAPStreamedReader()
    reader.append(buffer)
    var opcodes: [Opcode] = []
    while let record = try reader.nextRecord() {
      opcodes.append(type(of: record).opcode)
    }
    return opcodes
  }

  /// A private record (opcode 0x80) with the given content, which readers skip.
  private func privateRecord(_ content: [UInt8]) -> Data {
    var data = Data([0x80])
    withUnsafeBytes(of: UInt64(content.count).littleEndian) { data.append(contentsOf: $0) }
    data.append(contentsOf: content)
    return data
  }

  @Test
  func readsEmptyChunk() throws {
    #expect(try streamedOpcodes(chunkRecords: Data()) == [.header, .dataEnd, .footer])
  }

  @Test
  func readsChunkEndingWithSkippedRecord() throws {
    var records = Data()
    Schema(id: 1, name: "Example", encoding: "c", data: Data([4, 5, 6])).serialize(to: &records)
    records.append(privateRecord([1, 2, 3]))
    #expect(try streamedOpcodes(chunkRecords: records) == [.header, .schema, .dataEnd, .footer])
  }

  @Test
  func readsChunkEndingWithEmptySkippedRecord() throws {
    var records = Data()
    Schema(id: 1, name: "Example", encoding: "c", data: Data([4, 5, 6])).serialize(to: &records)
    records.append(privateRecord([]))
    #expect(try streamedOpcodes(chunkRecords: records) == [.header, .schema, .dataEnd, .footer])
  }

  @Test
  func rejectsTruncatedRecordInChunk() throws {
    var records = Data()
    Schema(id: 1, name: "Example", encoding: "c", data: Data([4, 5, 6])).serialize(to: &records)
    records.append(contentsOf: [0x80, 5, 0])
    let error = #expect(throws: MCAPReadError.self) { try streamedOpcodes(chunkRecords: records) }
    #expect(error == MCAPReadError.extraneousDataInChunk(length: 3))
  }
}
