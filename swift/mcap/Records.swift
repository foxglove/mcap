import struct Foundation.Data

public typealias SchemaID = UInt16
public typealias ChannelID = UInt16
public typealias Timestamp = UInt64

// swift-format-ignore: AlwaysUseLowerCamelCase
public let MCAP0_MAGIC = Data([137, 77, 67, 65, 80, 48, 13, 10])

public enum Opcode: UInt8 {
  case header = 0x01
  case footer = 0x02
  case schema = 0x03
  case channel = 0x04
  case message = 0x05
  case chunk = 0x06
  case messageIndex = 0x07
  case chunkIndex = 0x08
  case attachment = 0x09
  case attachmentIndex = 0x0A
  case statistics = 0x0B
  case metadata = 0x0C
  case metadataIndex = 0x0D
  case summaryOffset = 0x0E
  case dataEnd = 0x0F
}

public protocol Record {
  static var opcode: Opcode { get }
  func serializeFields(to data: inout Data)
}

extension Record {
  func serialize(to data: inout Data) {
    data.append(Self.opcode.rawValue)
    data.append(unsafeBytesOf: UInt64(0))  // placeholder
    let fieldsStartOffset = data.count
    self.serializeFields(to: &data)
    let fieldsLength = data.count - fieldsStartOffset
    withUnsafeBytes(of: UInt64(fieldsLength).littleEndian) {
      data.replaceSubrange(
        fieldsStartOffset - MemoryLayout<UInt64>.size..<fieldsStartOffset,
        with: $0
      )
    }
  }
}

func prefixedStringLength(_ str: String) -> Int {
  return MemoryLayout<UInt32>.size + str.utf8.count
}
func prefixedMapLength(_ map: [String: String]) -> Int {
  var length = 0
  for (key, value) in map {
    length += prefixedStringLength(key) + prefixedStringLength(value)
  }
  return MemoryLayout<UInt32>.size + length
}
func prefixedMapLength<K: UnsignedInteger, V: UnsignedInteger>(_ map: [K: V]) -> Int {
  return MemoryLayout<UInt32>.size + map.count * (MemoryLayout<K>.size + MemoryLayout<V>.size)
}
func prefixedTupleArrayLength<K: UnsignedInteger, V: UnsignedInteger>(_ arr: [(K, V)]) -> Int {
  return MemoryLayout<UInt32>.size + arr.count * (MemoryLayout<K>.size + MemoryLayout<V>.size)
}

extension Data {
  mutating func append<T: UnsignedInteger>(unsafeBytesOf value: T) {
    Swift.withUnsafeBytes(of: value) {
      append($0.bindMemory(to: UInt8.self))
    }
  }

  mutating func appendUInt32PrefixedData(_ data: Data) {
    append(unsafeBytesOf: UInt32(data.count).littleEndian)
    append(data)
  }

  mutating func appendUInt64PrefixedData(_ data: Data) {
    append(unsafeBytesOf: UInt64(data.count).littleEndian)
    append(data)
  }

  mutating func appendPrefixedString(_ str: String) {
    var str = str  // withUTF8 may mutate str
    str.withUTF8 {
      append(unsafeBytesOf: UInt32($0.count).littleEndian)
      append($0)
    }
  }

  mutating func appendPrefixedMap(_ map: [String: String]) {
    let sizeOffset = self.count
    append(unsafeBytesOf: UInt32(0))  // placeholder
    for (key, value) in map {
      appendPrefixedString(key)
      appendPrefixedString(value)
    }
    Swift.withUnsafeBytes(
      of: UInt32(self.count - sizeOffset - MemoryLayout<UInt32>.size).littleEndian
    ) {
      replaceSubrange(sizeOffset..<sizeOffset + MemoryLayout<UInt32>.size, with: $0)
    }
  }

  //FIXME: all reserveCapacity calls
  //FIXME: move reserveCapacity inside append?
  mutating func appendPrefixedMap<K: UnsignedInteger, V: UnsignedInteger>(_ map: [K: V]) {
    append(
      unsafeBytesOf: UInt32(map.count * (MemoryLayout<K>.size + MemoryLayout<V>.size)).littleEndian
    )
    for (key, value) in map {
      append(unsafeBytesOf: key)
      append(unsafeBytesOf: value)
    }
  }

  // https://bugs.swift.org/browse/SR-922
  mutating func appendPrefixedTupleArray<K: UnsignedInteger, V: UnsignedInteger>(_ map: [(K, V)]) {
    append(
      unsafeBytesOf: UInt32(map.count * (MemoryLayout<K>.size + MemoryLayout<V>.size)).littleEndian
    )
    for (key, value) in map {
      append(unsafeBytesOf: key)
      append(unsafeBytesOf: value)
    }
  }
}

public struct Header: Record {
  public static let opcode = Opcode.header
  public var profile: String
  public var library: String

  public init(profile: String, library: String) {
    self.profile = profile
    self.library = library
  }

  public func serializeFields(to data: inout Data) {
    data.reserveCapacity(prefixedStringLength(profile) + prefixedStringLength(library))
    data.appendPrefixedString(profile)
    data.appendPrefixedString(library)
  }
}

public struct Footer: Record {
  public static let opcode = Opcode.footer
  public var summaryStart: UInt64
  public var summaryOffsetStart: UInt64
  public var summaryCRC: UInt32

  public init(summaryStart: UInt64, summaryOffsetStart: UInt64, summaryCRC: UInt32) {
    self.summaryStart = summaryStart
    self.summaryOffsetStart = summaryOffsetStart
    self.summaryCRC = summaryCRC
  }

  public func serializeFields(to data: inout Data) {
    data.reserveCapacity(
      MemoryLayout<UInt64>.size + MemoryLayout<UInt64>.size + MemoryLayout<UInt32>.size
    )
    data.append(unsafeBytesOf: summaryStart.littleEndian)
    data.append(unsafeBytesOf: summaryOffsetStart.littleEndian)
    data.append(unsafeBytesOf: summaryCRC.littleEndian)
  }
}

public struct Schema: Record {
  public static let opcode = Opcode.schema
  public var id: SchemaID
  public var name: String
  public var encoding: String
  public var data: Data

  public init(id: SchemaID, name: String, encoding: String, data: Data) {
    self.id = id
    self.name = name
    self.encoding = encoding
    self.data = data
  }

  public func serializeFields(to data: inout Data) {
    data.reserveCapacity(
      MemoryLayout.size(ofValue: id) + prefixedStringLength(name) + prefixedStringLength(encoding)
        + self.data.count
    )
    data.append(unsafeBytesOf: id.littleEndian)
    data.appendPrefixedString(name)
    data.appendPrefixedString(encoding)
    data.appendUInt32PrefixedData(self.data)
  }
}

public struct Channel: Record {
  public static let opcode = Opcode.channel
  public var id: ChannelID
  public var schemaID: SchemaID
  public var topic: String
  public var messageEncoding: String
  public var metadata: [String: String]

  public init(
    id: ChannelID,
    schemaID: SchemaID,
    topic: String,
    messageEncoding: String,
    metadata: [String: String]
  ) {
    self.id = id
    self.schemaID = schemaID
    self.topic = topic
    self.messageEncoding = messageEncoding
    self.metadata = metadata
  }

  public func serializeFields(to data: inout Data) {
    data.reserveCapacity(
      MemoryLayout.size(ofValue: id) + MemoryLayout.size(ofValue: schemaID)
        + prefixedStringLength(topic)
        + prefixedStringLength(messageEncoding) + prefixedMapLength(metadata)
    )
    data.append(unsafeBytesOf: id.littleEndian)
    data.append(unsafeBytesOf: schemaID.littleEndian)
    data.appendPrefixedString(topic)
    data.appendPrefixedString(messageEncoding)
    data.appendPrefixedMap(metadata)
  }
}

public struct Message: Record {
  public static let opcode = Opcode.message
  public var channelID: ChannelID
  public var sequence: UInt32
  public var logTime: Timestamp
  public var publishTime: Timestamp
  public var data: Data

  public init(
    channelID: ChannelID,
    sequence: UInt32,
    logTime: Timestamp,
    publishTime: Timestamp,
    data: Data
  ) {
    self.channelID = channelID
    self.sequence = sequence
    self.logTime = logTime
    self.publishTime = publishTime
    self.data = data
  }

  public func serializeFields(to data: inout Data) {
    data.reserveCapacity(
      MemoryLayout.size(ofValue: channelID) + MemoryLayout.size(ofValue: sequence)
        + MemoryLayout.size(ofValue: logTime) + MemoryLayout.size(ofValue: publishTime) + data.count
    )
    data.append(unsafeBytesOf: channelID.littleEndian)
    data.append(unsafeBytesOf: sequence.littleEndian)
    data.append(unsafeBytesOf: logTime.littleEndian)
    data.append(unsafeBytesOf: publishTime.littleEndian)
    data.append(self.data)
  }
}

public struct Chunk: Record {
  public static let opcode = Opcode.chunk
  public var messageStartTime: Timestamp
  public var messageEndTime: Timestamp
  public var uncompressedSize: UInt64
  public var uncompressedCRC: UInt32
  public var compression: String
  public var records: Data

  public init(
    messageStartTime: Timestamp,
    messageEndTime: Timestamp,
    uncompressedSize: UInt64,
    uncompressedCRC: UInt32,
    compression: String,
    records: Data
  ) {
    self.messageStartTime = messageStartTime
    self.messageEndTime = messageEndTime
    self.uncompressedSize = uncompressedSize
    self.uncompressedCRC = uncompressedCRC
    self.compression = compression
    self.records = records
  }

  public func serializeFields(to data: inout Data) {
    data.reserveCapacity(
      MemoryLayout.size(ofValue: messageStartTime) + MemoryLayout.size(ofValue: messageEndTime)
        + MemoryLayout.size(ofValue: uncompressedSize)
        + MemoryLayout.size(ofValue: uncompressedCRC) + prefixedStringLength(compression)
        + self.records.count
    )
    data.append(unsafeBytesOf: messageStartTime.littleEndian)
    data.append(unsafeBytesOf: messageEndTime.littleEndian)
    data.append(unsafeBytesOf: uncompressedSize.littleEndian)
    data.append(unsafeBytesOf: uncompressedCRC.littleEndian)
    data.appendPrefixedString(compression)
    data.appendUInt64PrefixedData(self.records)
  }
}

public struct MessageIndex: Record {
  public static let opcode = Opcode.messageIndex
  public var channelID: ChannelID
  public var records: [(logTime: Timestamp, offset: UInt64)]

  public init(channelID: ChannelID, records: [(logTime: Timestamp, offset: UInt64)]) {
    self.channelID = channelID
    self.records = records
  }

  public func serializeFields(to data: inout Data) {
    data.reserveCapacity(
      MemoryLayout.size(ofValue: channelID) + MemoryLayout<UInt32>.size
        + MemoryLayout<Timestamp>.size
        + MemoryLayout<UInt64>.size * records.count
    )
    data.append(unsafeBytesOf: channelID.littleEndian)
    data.appendPrefixedTupleArray(records)
  }
}

public struct ChunkIndex: Record {
  public static let opcode = Opcode.chunkIndex
  public var messageStartTime: Timestamp
  public var messageEndTime: Timestamp
  public var chunkStartOffset: UInt64
  public var chunkLength: UInt64
  public var messageIndexOffsets: [ChannelID: UInt64]
  public var messageIndexLength: UInt64
  public var compression: String
  public var compressedSize: UInt64
  public var uncompressedSize: UInt64

  public init(
    messageStartTime: Timestamp,
    messageEndTime: Timestamp,
    chunkStartOffset: UInt64,
    chunkLength: UInt64,
    messageIndexOffsets: [ChannelID: UInt64],
    messageIndexLength: UInt64,
    compression: String,
    compressedSize: UInt64,
    uncompressedSize: UInt64
  ) {
    self.messageStartTime = messageStartTime
    self.messageEndTime = messageEndTime
    self.chunkStartOffset = chunkStartOffset
    self.chunkLength = chunkLength
    self.messageIndexOffsets = messageIndexOffsets
    self.messageIndexLength = messageIndexLength
    self.compression = compression
    self.compressedSize = compressedSize
    self.uncompressedSize = uncompressedSize
  }

  public func serializeFields(to data: inout Data) {
    data.reserveCapacity(
      MemoryLayout.size(ofValue: messageStartTime) + MemoryLayout.size(ofValue: messageEndTime)
        + MemoryLayout.size(ofValue: chunkStartOffset) + MemoryLayout.size(ofValue: chunkLength)
        + MemoryLayout.size(ofValue: messageIndexLength)
        + MemoryLayout.size(ofValue: compressedSize) + MemoryLayout.size(ofValue: uncompressedSize)
        + prefixedStringLength(compression) + prefixedMapLength(messageIndexOffsets)
    )
    data.append(unsafeBytesOf: messageStartTime.littleEndian)
    data.append(unsafeBytesOf: messageEndTime.littleEndian)
    data.append(unsafeBytesOf: chunkStartOffset.littleEndian)
    data.append(unsafeBytesOf: chunkLength.littleEndian)
    data.append(unsafeBytesOf: messageIndexLength.littleEndian)
    data.append(unsafeBytesOf: compressedSize.littleEndian)
    data.append(unsafeBytesOf: uncompressedSize.littleEndian)
    data.appendPrefixedString(compression)
    data.appendPrefixedMap(messageIndexOffsets)
  }
}

public struct Attachment: Record {
  public static let opcode = Opcode.attachment
  public var logTime: Timestamp
  public var createTime: Timestamp
  public var name: String
  public var contentType: String
  public var data: Data

  public init(
    logTime: Timestamp,
    createTime: Timestamp,
    name: String,
    contentType: String,
    data: Data
  ) {
    self.logTime = logTime
    self.createTime = createTime
    self.name = name
    self.contentType = contentType
    self.data = data
  }

  public func serializeFields(to data: inout Data) {
    data.reserveCapacity(
      MemoryLayout.size(ofValue: logTime) + MemoryLayout.size(ofValue: createTime)
        + prefixedStringLength(name) + prefixedStringLength(contentType) + self.data.count
        + MemoryLayout<UInt32>.size
    )
    let fieldsStartOffset = data.count
    data.append(unsafeBytesOf: logTime.littleEndian)
    data.append(unsafeBytesOf: createTime.littleEndian)
    data.appendPrefixedString(name)
    data.appendPrefixedString(contentType)
    data.appendUInt64PrefixedData(self.data)
    var crc = CRC32()
    crc.update(data[fieldsStartOffset..<data.count])
    data.append(unsafeBytesOf: crc.final.littleEndian)
  }
}

public struct AttachmentIndex: Record {
  public static let opcode = Opcode.attachmentIndex
  public var offset: UInt64
  public var length: UInt64
  public var logTime: Timestamp
  public var createTime: Timestamp
  public var dataSize: UInt64
  public var name: String
  public var contentType: String

  public func serializeFields(to data: inout Data) {
    data.reserveCapacity(
      MemoryLayout<UInt64>.size + MemoryLayout<UInt64>.size + MemoryLayout<Timestamp>.size
        + MemoryLayout<Timestamp>.size + MemoryLayout<UInt64>.size + prefixedStringLength(name)
        + prefixedStringLength(contentType)
    )
    data.append(unsafeBytesOf: offset.littleEndian)
    data.append(unsafeBytesOf: length.littleEndian)
    data.append(unsafeBytesOf: logTime.littleEndian)
    data.append(unsafeBytesOf: createTime.littleEndian)
    data.append(unsafeBytesOf: dataSize.littleEndian)
    data.appendPrefixedString(name)
    data.appendPrefixedString(contentType)
  }
}

public struct Statistics: Record {
  public static let opcode = Opcode.statistics
  public var messageCount: UInt64 = 0
  public var schemaCount: UInt16 = 0
  public var channelCount: UInt32 = 0
  public var attachmentCount: UInt32 = 0
  public var metadataCount: UInt32 = 0
  public var chunkCount: UInt32 = 0
  public var messageStartTime: Timestamp = 0
  public var messageEndTime: Timestamp = 0
  public var channelMessageCounts: [ChannelID: UInt64] = [:]

  public func serializeFields(to data: inout Data) {
    data.reserveCapacity(
      MemoryLayout.size(ofValue: messageCount) + MemoryLayout.size(ofValue: schemaCount)
        + MemoryLayout.size(ofValue: channelCount) + MemoryLayout.size(ofValue: attachmentCount)
        + MemoryLayout.size(ofValue: metadataCount) + MemoryLayout.size(ofValue: chunkCount)
        + MemoryLayout.size(ofValue: messageStartTime) + MemoryLayout.size(ofValue: messageEndTime)
        + prefixedMapLength(channelMessageCounts)
    )
    data.append(unsafeBytesOf: messageCount.littleEndian)
    data.append(unsafeBytesOf: schemaCount.littleEndian)
    data.append(unsafeBytesOf: channelCount.littleEndian)
    data.append(unsafeBytesOf: attachmentCount.littleEndian)
    data.append(unsafeBytesOf: metadataCount.littleEndian)
    data.append(unsafeBytesOf: chunkCount.littleEndian)
    data.append(unsafeBytesOf: messageStartTime.littleEndian)
    data.append(unsafeBytesOf: messageEndTime.littleEndian)
    data.appendPrefixedMap(channelMessageCounts)
  }
}

public struct Metadata: Record {
  public static let opcode = Opcode.metadata
  public var name: String
  public var metadata: [String: String]

  public func serializeFields(to data: inout Data) {
    data.reserveCapacity(
      prefixedStringLength(name) + MemoryLayout<UInt32>.size + prefixedMapLength(metadata)
    )
    data.appendPrefixedString(name)
    data.appendPrefixedMap(metadata)
  }
}

public struct MetadataIndex: Record {
  public static let opcode = Opcode.metadataIndex
  public var offset: UInt64
  public var length: UInt64
  public var name: String

  public func serializeFields(to data: inout Data) {
    data.reserveCapacity(
      MemoryLayout.size(ofValue: offset) + MemoryLayout.size(ofValue: length)
        + prefixedStringLength(name)
    )
    data.append(unsafeBytesOf: offset.littleEndian)
    data.append(unsafeBytesOf: length.littleEndian)
    data.appendPrefixedString(name)
  }
}

public struct SummaryOffset: Record {
  public static let opcode = Opcode.summaryOffset
  public var groupOpcode: UInt8
  public var groupStart: UInt64
  public var groupLength: UInt64

  public func serializeFields(to data: inout Data) {
    data.reserveCapacity(
      MemoryLayout.size(ofValue: groupOpcode) + MemoryLayout.size(ofValue: groupStart)
        + MemoryLayout.size(ofValue: groupLength)
    )
    data.append(groupOpcode)
    data.append(unsafeBytesOf: groupStart.littleEndian)
    data.append(unsafeBytesOf: groupLength.littleEndian)
  }
}

public struct DataEnd: Record {
  public static let opcode = Opcode.dataEnd
  public var dataSectionCRC: UInt32

  public func serializeFields(to data: inout Data) {
    data.reserveCapacity(MemoryLayout.size(ofValue: dataSectionCRC))
    data.append(unsafeBytesOf: dataSectionCRC.littleEndian)
  }
}
