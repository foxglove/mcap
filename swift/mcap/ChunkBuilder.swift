import struct Foundation.Data

class ChunkBuilder {
  var buffer = Data()
  var messageCount = 0
  var messageStartTime: Timestamp = 0
  var messageEndTime: Timestamp = 0
  //FIXME: don't actually track these if disabled
  var messageIndexes: [ChannelID: MessageIndex] = [:]

  func reset() {
    buffer.removeAll(keepingCapacity: true)
    messageCount = 0
    messageStartTime = 0
    messageEndTime = 0
    messageIndexes.removeAll(keepingCapacity: true)
  }

  func addMessage(_ message: Message) {
    let record = (logTime: message.logTime, offset: UInt64(buffer.count))
    messageIndexes[
      message.channelID,
      default: MessageIndex(channelID: message.channelID, records: [])
    ].records.append(record)
    if messageCount == 0 || message.logTime < messageStartTime {
      messageStartTime = message.logTime
    }
    if messageCount == 0 || message.logTime > messageEndTime {
      messageEndTime = message.logTime
    }
    messageCount += 1
    message.serialize(to: &buffer)
  }
}
