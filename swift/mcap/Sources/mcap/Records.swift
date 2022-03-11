import struct Foundation.Data

public typealias SchemaID = UInt16
public typealias ChannelID = UInt32
public typealias Timestamp = UInt64

public enum Opcode: UInt8 {
  case Header = 0x01
  case Footer = 0x02
  case Schema = 0x03
  case Channel = 0x04
  case Message = 0x05
  case Chunk = 0x06
  case MessageIndex = 0x07
  case ChunkIndex = 0x08
  case Attachment = 0x09
  case AttachmentIndex = 0x0A
  case Statistics = 0x0B
  case Metadata = 0x0C
  case MetadataIndex = 0x0D
  case SummaryOffset = 0x0E
  case DataEnd = 0x0F
}

public protocol Record {
  static var opcode: Opcode { get }
}

func prefixedStringLength(_ str: String) -> Int {
  return MemoryLayout<UInt32>.size + str.utf8.count
}

extension Data {
  mutating func append<T>(unsafeBytesOf value: T) {
    Swift.withUnsafeBytes(of: value) {
      append($0.bindMemory(to: UInt8.self))
    }
  }
  
  mutating func appendPrefixedString(_ str: String) {
    var str = str  // withUTF8 may mutate str
    str.withUTF8 {
      append(unsafeBytesOf: UInt32($0.count).littleEndian)
      append($0)
    }
  }
}

public struct Header {
  static let opcode = Opcode.Header
  let profile: String
  let library: String
  
  func serialize(to data: inout Data) {
    data.reserveCapacity(1 + prefixedStringLength(profile) + prefixedStringLength(library))
    data.append(Self.opcode.rawValue)
    data.appendPrefixedString(profile)
    data.appendPrefixedString(library)
  }
}

public struct Footer {
  static let opcode = Opcode.Footer
  let summaryStart: UInt64
  let summaryOffsetStart: UInt64
  let summaryCRC: UInt32
  
  func serialize(to data: inout Data) {
    data.reserveCapacity(1 + MemoryLayout<UInt64>.size + MemoryLayout<UInt64>.size + MemoryLayout<UInt32>.size)
    data.append(Self.opcode.rawValue)
    data.append(unsafeBytesOf: summaryStart.littleEndian)
    data.append(unsafeBytesOf: summaryOffsetStart.littleEndian)
    data.append(unsafeBytesOf: summaryCRC.littleEndian)
  }
}

public struct Schema {
  static let opcode = Opcode.Schema
  let id: SchemaID
  let name: String
  let encoding: String
  let data: Data
  
  func serialize(to data: inout Data) {
    data.reserveCapacity(1 + MemoryLayout<SchemaID>.size + prefixedStringLength(name) + prefixedStringLength(encoding) + self.data.count)
    data.append(Self.opcode.rawValue)
    data.append(unsafeBytesOf: id.littleEndian)
    data.appendPrefixedString(name)
    data.appendPrefixedString(encoding)
    data.append(self.data)
  }
}

public struct Channel {
  static let opcode = Opcode.Channel
  let id: ChannelID
  let schemaID: SchemaID
  let topic: String
  let messageEncoding: String
  let metadata: [String: String]
  
  //unchecked
  func serialize(to data: inout Data) {
    data.reserveCapacity(1 + MemoryLayout<ChannelID>.size + MemoryLayout<SchemaID>.size + prefixedStringLength(topic) + prefixedStringLength(messageEncoding) + MemoryLayout<UInt32>.size * metadata.count)
    data.append(Self.opcode.rawValue)
    data.append(unsafeBytesOf: id.littleEndian)
    data.append(unsafeBytesOf: schemaID.littleEndian)
    data.appendPrefixedString(topic)
    data.appendPrefixedString(messageEncoding)
    data.append(unsafeBytesOf: UInt32(metadata.count).littleEndian)
    for (key, value) in metadata {
      data.appendPrefixedString(key)
      data.appendPrefixedString(value)
    }
  }
}

public struct Message {
  static let opcode = Opcode.Message
  let channelID: ChannelID
  let sequence: UInt32
  let logTime: Timestamp
  let publishTime: Timestamp
  let data: Data
  
  func serialize(to data: inout Data) {
    data.reserveCapacity(1 + MemoryLayout<ChannelID>.size + MemoryLayout<UInt32>.size + MemoryLayout<Timestamp>.size + MemoryLayout<Timestamp>.size + self.data.count)
    data.append(Self.opcode.rawValue)
    data.append(unsafeBytesOf: channelID.littleEndian)
    data.append(unsafeBytesOf: sequence.littleEndian)
    data.append(unsafeBytesOf: logTime.littleEndian)
    data.append(unsafeBytesOf: publishTime.littleEndian)
    data.append(self.data)
  }
}

public struct Chunk {
  static let opcode = Opcode.Chunk
  let messageStartTime: Timestamp
  let messageEndTime: Timestamp
  let uncompressedSize: UInt64
  let uncompressedCRC: UInt32
  let compression: String
  let records: Data
  
  func serialize(to data: inout Data) {
    data.reserveCapacity(1 + MemoryLayout<Timestamp>.size + MemoryLayout<Timestamp>.size + MemoryLayout<UInt64>.size + MemoryLayout<UInt32>.size + prefixedStringLength(compression) + self.records.count)
    data.append(Self.opcode.rawValue)
    data.append(unsafeBytesOf: messageStartTime.littleEndian)
    data.append(unsafeBytesOf: messageEndTime.littleEndian)
    data.append(unsafeBytesOf: uncompressedSize.littleEndian)
    data.append(unsafeBytesOf: uncompressedCRC.littleEndian)
    data.appendPrefixedString(compression)
    data.append(self.records)
  }
}

public struct MessageIndex {
  static let opcode = Opcode.MessageIndex
  let channelID: ChannelID
  let records: [(logTime: Timestamp, offset: UInt64)]
  
  func serialize(to data: inout Data) {
    data.reserveCapacity(1 + MemoryLayout<ChannelID>.size + MemoryLayout<UInt32>.size + MemoryLayout<Timestamp>.size + MemoryLayout<UInt64>.size * records.count)
    data.append(Self.opcode.rawValue)
    data.append(unsafeBytesOf: channelID.littleEndian)
    data.append(unsafeBytesOf: UInt32(records.count).littleEndian)
    for (logTime, offset) in records {
      data.append(unsafeBytesOf: logTime.littleEndian)
      data.append(unsafeBytesOf: offset.littleEndian)
    }
  }
}

public struct ChunkIndex {
  static let opcode = Opcode.ChunkIndex
  let messageStartTime: Timestamp
  let messageEndTime: Timestamp
  let chunkStartOffset: UInt64
  let chunkLength: UInt64
  let messageIndexOffsets: [ChannelID: UInt64]
  let messageIndexLength: UInt64
  let compression: String
  let compressedSize: UInt64
  let uncompressedSize: UInt64
  
  func serialize(to data: inout Data) {
    data.reserveCapacity(1 + MemoryLayout<Timestamp>.size + MemoryLayout<Timestamp>.size + MemoryLayout<UInt64>.size + MemoryLayout<UInt64>.size + MemoryLayout<UInt64>.size + MemoryLayout<UInt64>.size + MemoryLayout<UInt64>.size + prefixedStringLength(compression))
    data.append(Self.opcode.rawValue)
    data.append(unsafeBytesOf: messageStartTime.littleEndian)
    data.append(unsafeBytesOf: messageEndTime.littleEndian)
    data.append(unsafeBytesOf: chunkStartOffset.littleEndian)
    data.append(unsafeBytesOf: chunkLength.littleEndian)
    data.append(unsafeBytesOf: messageIndexLength.littleEndian)
    data.append(unsafeBytesOf: compressedSize.littleEndian)
    data.append(unsafeBytesOf: uncompressedSize.littleEndian)
    data.appendPrefixedString(compression)
    for (channelID, offset) in messageIndexOffsets {
      data.append(unsafeBytesOf: channelID.littleEndian)
      data.append(unsafeBytesOf: offset.littleEndian)
    }
  }
}

public struct Attachment {
  static let opcode = Opcode.Attachment
  let logTime: Timestamp
  let createTime: Timestamp
  let name: String
  let contentType: String
  let data: Data
  @available(*, unavailable, message: "TODO") let crc: UInt32
  
  func serialize(to data: inout Data) {
    data.reserveCapacity(1 + MemoryLayout<Timestamp>.size + MemoryLayout<Timestamp>.size + prefixedStringLength(name) + prefixedStringLength(contentType) + self.data.count)
    data.append(Self.opcode.rawValue)
    data.append(unsafeBytesOf: logTime.littleEndian)
    data.append(unsafeBytesOf: createTime.littleEndian)
    data.appendPrefixedString(name)
    data.appendPrefixedString(contentType)
    data.append(self.data)
  }
}

public struct AttachmentIndex {
  static let opcode = Opcode.AttachmentIndex
  let offset: UInt64
  let length: UInt64
  let logTime: Timestamp
  let createTime: Timestamp
  let dataSize: UInt64
  let name: String
  let contentType: String
  
  func serialize(to data: inout Data) {
    data.reserveCapacity(1 + MemoryLayout<UInt64>.size + MemoryLayout<UInt64>.size + MemoryLayout<Timestamp>.size + MemoryLayout<Timestamp>.size + MemoryLayout<UInt64>.size + prefixedStringLength(name) + prefixedStringLength(contentType))
    data.append(Self.opcode.rawValue)
    data.append(unsafeBytesOf: offset.littleEndian)
    data.append(unsafeBytesOf: length.littleEndian)
    data.append(unsafeBytesOf: logTime.littleEndian)
    data.append(unsafeBytesOf: createTime.littleEndian)
    data.append(unsafeBytesOf: dataSize.littleEndian)
    data.appendPrefixedString(name)
    data.appendPrefixedString(contentType)
  }
}

public struct Statistics {
  static let opcode = Opcode.Statistics
  let messageCount: UInt64
  let schemaCount: UInt16
  let channelCount: UInt32
  let attachmentCount: UInt32
  let metadataCount: UInt32
  let chunkCount: UInt32
  let messageStartTime: Timestamp
  let messageEndTime: Timestamp
  let channelMessageCounts: [ChannelID: UInt64]
  
  func serialize(to data: inout Data) {
    data.reserveCapacity(1 + MemoryLayout<UInt64>.size + MemoryLayout<UInt16>.size + MemoryLayout<UInt32>.size + MemoryLayout<UInt32>.size + MemoryLayout<UInt32>.size + MemoryLayout<UInt32>.size + MemoryLayout<Timestamp>.size + MemoryLayout<Timestamp>.size + MemoryLayout<UInt32>.size * channelMessageCounts.count)
    data.append(Self.opcode.rawValue)
    data.append(unsafeBytesOf: messageCount.littleEndian)
    data.append(unsafeBytesOf: schemaCount.littleEndian)
    data.append(unsafeBytesOf: channelCount.littleEndian)
    data.append(unsafeBytesOf: attachmentCount.littleEndian)
    data.append(unsafeBytesOf: metadataCount.littleEndian)
    data.append(unsafeBytesOf: chunkCount.littleEndian)
    data.append(unsafeBytesOf: messageStartTime.littleEndian)
    data.append(unsafeBytesOf: messageEndTime.littleEndian)
    data.append(unsafeBytesOf: UInt32(channelMessageCounts.count).littleEndian)
    for (channelID, count) in channelMessageCounts {
      data.append(unsafeBytesOf: channelID.littleEndian)
      data.append(unsafeBytesOf: count.littleEndian)
    }
  }
}

public struct Metadata {
  static let opcode = Opcode.Metadata
  let name: String
  let metadata: [String: String]
  
  func serialize(to data: inout Data) {
    data.reserveCapacity(1 + prefixedStringLength(name) + MemoryLayout<UInt32>.size + metadata.count * (MemoryLayout<UInt32>.size + prefixedStringLength(key) + prefixedStringLength(value)))
    data.append(Self.opcode.rawValue)
    data.appendPrefixedString(name)
    data.append(UInt32(metadata.count).littleEndian)
    for (key, value) in metadata {
      data.append(UInt32(key.count).littleEndian)
      data.appendPrefixedString(key)
      data.append(UInt32(value.count).littleEndian)
      data.appendPrefixedString(value)
    }
  }
}

public struct MetadataIndex {
  static let opcode = Opcode.MetadataIndex
  let offset: UInt64
  let length: UInt64
  let name: String
  
  func serialize(to data: inout Data) {
    data.reserveCapacity(1 + MemoryLayout<UInt64>.size + MemoryLayout<UInt64>.size + prefixedStringLength(name))
    data.append(Self.opcode.rawValue)
    data.append(unsafeBytesOf: offset.littleEndian)
    data.append(unsafeBytesOf: length.littleEndian)
    data.appendPrefixedString(name)
  }
}

public struct SummaryOffset {
  static let opcode = Opcode.SummaryOffset
  let groupOpcode: UInt8
  let groupStart: UInt64
  let groupLength: UInt64
  
  func serialize(to data: inout Data) {
    data.reserveCapacity(1 + MemoryLayout<UInt8>.size + MemoryLayout<UInt64>.size + MemoryLayout<UInt64>.size)
    data.append(Self.opcode.rawValue)
    data.append(groupOpcode)
    data.append(unsafeBytesOf: groupStart.littleEndian)
    data.append(unsafeBytesOf: groupLength.littleEndian)
  }
}

public struct DataEnd {
  static let opcode = Opcode.DataEnd
  let dataSectionCRC: UInt32
  
  func serialize(to data: inout Data) {
    data.reserveCapacity(1 + MemoryLayout<UInt32>.size)
    data.append(Self.opcode.rawValue)
    data.append(unsafeBytesOf: dataSectionCRC.littleEndian)
  }
}

