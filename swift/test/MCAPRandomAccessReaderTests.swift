import CRC
import Foundation
import MCAP
import Testing

extension MCAPRandomAccessReader.MessageIterator {
  func readAll() throws -> [Message] {
    var messages: [Message] = []
    while let message = try next() {
      messages.append(message)
    }
    return messages
  }
}

extension Message: @retroactive Equatable {
  // swiftlint:disable:next large_tuple
  var comparisonFields: (ChannelID, UInt32, Timestamp, Timestamp, Data) {
    (channelID, sequence, logTime, publishTime, data)
  }

  public static func == (lhs: Message, rhs: Message) -> Bool {
    lhs.comparisonFields == rhs.comparisonFields
  }
}

struct MCAPRandomAccessReaderTests {
  @Test
  func readsLogTimeOrder() async throws {
    let buffer = Buffer()
    let writer = MCAPWriter(buffer)
    await writer.start(library: "lib", profile: "prof")

    let chan = await writer.addChannel(schemaID: 0, topic: "topic", messageEncoding: "enc", metadata: ["foo": "bar"])

    let message1 = Message(channelID: chan, sequence: 0, logTime: 2, publishTime: 1, data: Data([3, 4, 5]))
    let message2 = Message(channelID: chan, sequence: 0, logTime: 4, publishTime: 3, data: Data([3, 4, 5, 6]))

    await writer.addMessage(message2)
    await writer.addMessage(message1)

    await writer.end()

    let reader = try MCAPRandomAccessReader(buffer)
    let iterator = reader.messageIterator()
    let messages = try iterator.readAll()

    #expect(messages == [message1, message2])
  }

  @Test
  func filtersTopics() async throws {
    let buffer = Buffer()
    let writer = MCAPWriter(buffer)
    await writer.start(library: "lib", profile: "prof")

    let chan1 = await writer.addChannel(schemaID: 0, topic: "topic1", messageEncoding: "enc", metadata: ["foo": "bar"])
    let chan2 = await writer.addChannel(schemaID: 0, topic: "topic2", messageEncoding: "enc", metadata: ["foo": "bar"])

    let message1 = Message(channelID: chan1, sequence: 0, logTime: 2, publishTime: 1, data: Data([3, 4, 5]))
    let message2 = Message(channelID: chan2, sequence: 0, logTime: 4, publishTime: 3, data: Data([3, 4, 5, 6]))

    await writer.addMessage(message1)
    await writer.addMessage(message2)

    await writer.end()

    let reader = try MCAPRandomAccessReader(buffer)

    #expect(try reader.messageIterator().readAll() == [message1, message2])
    #expect(try reader.messageIterator(topics: ["topic1"]).readAll() == [message1])
    #expect(try reader.messageIterator(topics: ["topic2"]).readAll() == [message2])
  }

  @Test
  func filtersByTime() async throws {
    let buffer = Buffer()
    let writer = MCAPWriter(buffer)
    await writer.start(library: "lib", profile: "prof")

    let chan1 = await writer.addChannel(schemaID: 0, topic: "topic1", messageEncoding: "enc", metadata: ["foo": "bar"])
    let chan2 = await writer.addChannel(schemaID: 0, topic: "topic2", messageEncoding: "enc", metadata: ["foo": "bar"])

    let message1 = Message(channelID: chan1, sequence: 0, logTime: 2, publishTime: 1, data: Data([3, 4, 5]))
    let message2 = Message(channelID: chan2, sequence: 0, logTime: 4, publishTime: 3, data: Data([3, 4, 5, 6]))

    await writer.addMessage(message1)
    await writer.addMessage(message2)

    await writer.end()

    let reader = try MCAPRandomAccessReader(buffer)

    #expect(try reader.messageIterator().readAll() == [message1, message2])
    #expect(try reader.messageIterator(startTime: 1).readAll() == [message1, message2])
    #expect(try reader.messageIterator(startTime: 2).readAll() == [message1, message2])
    #expect(try reader.messageIterator(startTime: 3).readAll() == [message2])
    #expect(try reader.messageIterator(startTime: 4).readAll() == [message2])
    #expect(try reader.messageIterator(startTime: 5).readAll() == [])

    #expect(try reader.messageIterator(endTime: 1).readAll() == [])
    #expect(try reader.messageIterator(endTime: 2).readAll() == [message1])
    #expect(try reader.messageIterator(endTime: 3).readAll() == [message1])
    #expect(try reader.messageIterator(endTime: 4).readAll() == [message1, message2])
    #expect(try reader.messageIterator(endTime: 5).readAll() == [message1, message2])

    #expect(try reader.messageIterator(startTime: 1, endTime: 1).readAll() == [])
    #expect(try reader.messageIterator(startTime: 1, endTime: 2).readAll() == [message1])
    #expect(try reader.messageIterator(startTime: 1, endTime: 3).readAll() == [message1])
    #expect(try reader.messageIterator(startTime: 1, endTime: 4).readAll() == [message1, message2])
    #expect(try reader.messageIterator(startTime: 1, endTime: 5).readAll() == [message1, message2])

    #expect(try reader.messageIterator(startTime: 2, endTime: 2).readAll() == [message1])
    #expect(try reader.messageIterator(startTime: 2, endTime: 3).readAll() == [message1])
    #expect(try reader.messageIterator(startTime: 2, endTime: 4).readAll() == [message1, message2])
    #expect(try reader.messageIterator(startTime: 2, endTime: 5).readAll() == [message1, message2])

    #expect(try reader.messageIterator(startTime: 3, endTime: 3).readAll() == [])
    #expect(try reader.messageIterator(startTime: 3, endTime: 4).readAll() == [message2])
    #expect(try reader.messageIterator(startTime: 3, endTime: 5).readAll() == [message2])

    #expect(try reader.messageIterator(startTime: 4, endTime: 4).readAll() == [message2])
    #expect(try reader.messageIterator(startTime: 4, endTime: 5).readAll() == [message2])

    #expect(try reader.messageIterator(startTime: 5, endTime: 5).readAll() == [])
  }
}
