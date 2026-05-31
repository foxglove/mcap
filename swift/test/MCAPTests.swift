// swiftlint:disable force_cast

import CRC
import Foundation
import MCAP
import Testing

struct MCAPTests {
  @Test
  func empty() async throws {
    let buffer = Buffer()
    let writer = MCAPWriter(buffer)
    await writer.start(library: "", profile: "")
    await writer.end()
    #expect(buffer.data.count == 286)
  }

  @Test
  func defaultChunkSizeIsOneMiB() async throws {
    let buffer = Buffer()
    let writer = MCAPWriter(buffer)
    await writer.start(library: "", profile: "")
    let channelID = await writer.addChannel(schemaID: 0, topic: "topic", messageEncoding: "", metadata: [:])
    let data = Data(repeating: 0x42, count: 600 * 1024)
    for sequence in 0 ..< 3 {
      await writer.addMessage(
        Message(
          channelID: channelID,
          sequence: UInt32(sequence),
          logTime: UInt64(sequence),
          publishTime: UInt64(sequence),
          data: data
        )
      )
    }
    await writer.end()

    let reader = try MCAPRandomAccessReader(buffer)
    #expect(reader.chunkIndexes.count == 2)
    #expect(reader.statistics?.chunkCount == 2)
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
}
