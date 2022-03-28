import struct Foundation.Data

public typealias SchemaID = UInt16
public typealias ChannelID = UInt16
public typealias Timestamp = UInt64

// swiftlint:disable:next identifier_name
public let MCAP0_MAGIC = Data([137, 77, 67, 65, 80, 48, 13, 10])

public enum MCAPReadError: Error {
  case invalidMagic
  case readBeyondBounds
  case stringLengthBeyondBounds
  case dataLengthBeyondBounds
  case invalidCRC
  case extraneousDataInChunk
}

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
  init(deserializingFieldsFrom buffer: UnsafeRawBufferPointer) throws
  func serializeFields(to data: inout Data)
}

extension Record {
  func serialize(to data: inout Data) {
    data.append(Self.opcode.rawValue)
    data.append(littleEndian: UInt64(0)) // placeholder
    let fieldsStartOffset = data.count
    self.serializeFields(to: &data)
    let fieldsLength = data.count - fieldsStartOffset
    withUnsafeBytes(of: UInt64(fieldsLength).littleEndian) {
      data.replaceSubrange(
        fieldsStartOffset - MemoryLayout<UInt64>.size ..< fieldsStartOffset,
        with: $0
      )
    }
  }
}

func prefixedStringLength(_ str: String) -> Int {
  MemoryLayout<UInt32>.size + str.utf8.count
}

func prefixedMapLength(_ map: [String: String]) -> Int {
  var length = 0
  for (key, value) in map {
    length += prefixedStringLength(key) + prefixedStringLength(value)
  }
  return MemoryLayout<UInt32>.size + length
}

func prefixedMapLength<K: UnsignedInteger, V: UnsignedInteger>(_ map: [K: V]) -> Int {
  MemoryLayout<UInt32>.size + map.count * (MemoryLayout<K>.size + MemoryLayout<V>.size)
}

func prefixedTupleArrayLength<K: UnsignedInteger, V: UnsignedInteger>(_ arr: [(K, V)]) -> Int {
  MemoryLayout<UInt32>.size + arr.count * (MemoryLayout<K>.size + MemoryLayout<V>.size)
}

private extension Data {
  mutating func append<T: FixedWidthInteger & UnsignedInteger>(littleEndian value: T) {
    Swift.withUnsafeBytes(of: value.littleEndian) {
      append($0.bindMemory(to: UInt8.self))
    }
  }

  mutating func appendUInt32PrefixedData(_ data: Data) {
    append(littleEndian: UInt32(data.count))
    append(data)
  }

  mutating func appendUInt64PrefixedData(_ data: Data) {
    append(littleEndian: UInt64(data.count))
    append(data)
  }

  mutating func appendPrefixedString(_ str: String) {
    var str = str // withUTF8 may mutate str
    str.withUTF8 {
      append(littleEndian: UInt32($0.count))
      append($0)
    }
  }

  mutating func appendPrefixedMap(_ map: [String: String]) {
    let sizeOffset = self.count
    append(littleEndian: UInt32(0)) // placeholder
    for (key, value) in map {
      appendPrefixedString(key)
      appendPrefixedString(value)
    }
    Swift.withUnsafeBytes(
      of: UInt32(self.count - sizeOffset - MemoryLayout<UInt32>.size).littleEndian
    ) {
      replaceSubrange(sizeOffset ..< sizeOffset + MemoryLayout<UInt32>.size, with: $0)
    }
  }

  mutating func appendPrefixedMap<
    K: FixedWidthInteger & UnsignedInteger,
    V: FixedWidthInteger & UnsignedInteger
  >(_ map: [K: V]) {
    append(
      littleEndian: UInt32(map.count * (MemoryLayout<K>.size + MemoryLayout<V>.size))
    )
    for (key, value) in map {
      append(littleEndian: key)
      append(littleEndian: value)
    }
  }

  // https://bugs.swift.org/browse/SR-922
  mutating func appendPrefixedTupleArray<
    K: FixedWidthInteger & UnsignedInteger,
    V: FixedWidthInteger & UnsignedInteger
  >(_ map: [(K, V)]) {
    append(
      littleEndian: UInt32(map.count * (MemoryLayout<K>.size + MemoryLayout<V>.size))
    )
    for (key, value) in map {
      append(littleEndian: key)
      append(littleEndian: value)
    }
  }
}

private extension UnsafeRawBufferPointer {
  func read<T: FixedWidthInteger & UnsignedInteger>(littleEndian _: T.Type, from offset: inout Int) throws -> T {
    if offset + MemoryLayout<T>.size > self.count {
      throw MCAPReadError.readBeyondBounds
    }
    defer { offset += MemoryLayout<T>.size }
    var rawValue: T = 0
    withUnsafeMutableBytes(of: &rawValue) {
      $0.copyMemory(from: UnsafeRawBufferPointer(rebasing: self[offset ..< offset + MemoryLayout<T>.size]))
    }
    return T(littleEndian: rawValue)
  }

  func readPrefixedString(from offset: inout Int) throws -> String {
    let length = try read(littleEndian: UInt32.self, from: &offset)
    if offset + Int(length) > self.count {
      throw MCAPReadError.stringLengthBeyondBounds
    }
    defer { offset += Int(length) }
    return String(decoding: self[offset ..< offset + Int(length)], as: UTF8.self)
  }

  func readUInt32PrefixedData(from offset: inout Int) throws -> Data {
    let length = try read(littleEndian: UInt32.self, from: &offset)
    if offset + Int(length) > self.count {
      throw MCAPReadError.dataLengthBeyondBounds
    }
    defer { offset += Int(length) }
    return Data(self[offset ..< offset + Int(length)])
  }

  func readUInt64PrefixedData(from offset: inout Int) throws -> Data {
    let length = try read(littleEndian: UInt64.self, from: &offset)
    if offset + Int(length) > self.count {
      throw MCAPReadError.dataLengthBeyondBounds
    }
    defer { offset += Int(length) }
    return Data(self[offset ..< offset + Int(length)])
  }

  func readPrefixedStringMap(from offset: inout Int) throws -> [String: String] {
    let size = try read(littleEndian: UInt32.self, from: &offset)
    var result: [String: String] = [:]
    let subrange = UnsafeRawBufferPointer(rebasing: self[offset ..< offset + Int(size)])
    var subrangeOffset = 0
    while subrangeOffset < subrange.count {
      let key = try subrange.readPrefixedString(from: &subrangeOffset)
      let value = try subrange.readPrefixedString(from: &subrangeOffset)
      result[key] = value
    }
    offset += subrangeOffset
    return result
  }

  func readPrefixedTupleArray(from offset: inout Int) throws -> [(UInt64, UInt64)] {
    let size = try read(littleEndian: UInt32.self, from: &offset)
    var result: [(UInt64, UInt64)] = []
    let subrange = UnsafeRawBufferPointer(rebasing: self[offset ..< offset + Int(size)])
    var subrangeOffset = 0
    while subrangeOffset < subrange.count {
      let key = try subrange.read(littleEndian: UInt64.self, from: &subrangeOffset)
      let value = try subrange.read(littleEndian: UInt64.self, from: &subrangeOffset)
      result.append((key, value))
    }
    offset += subrangeOffset
    return result
  }

  func readPrefixedMap(from offset: inout Int) throws -> [UInt16: UInt64] {
    let size = try read(littleEndian: UInt32.self, from: &offset)
    var result: [UInt16: UInt64] = [:]
    let subrange = UnsafeRawBufferPointer(rebasing: self[offset ..< offset + Int(size)])
    var subrangeOffset = 0
    while subrangeOffset < subrange.count {
      let key = try subrange.read(littleEndian: UInt16.self, from: &subrangeOffset)
      let value = try subrange.read(littleEndian: UInt64.self, from: &subrangeOffset)
      result[key] = value
    }
    offset += subrangeOffset
    return result
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

  public init(deserializingFieldsFrom buffer: UnsafeRawBufferPointer) throws {
    var offset = 0
    profile = try buffer.readPrefixedString(from: &offset)
    library = try buffer.readPrefixedString(from: &offset)
  }

  public func serializeFields(to data: inout Data) {
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

  public init(deserializingFieldsFrom buffer: UnsafeRawBufferPointer) throws {
    var offset = 0
    summaryStart = try buffer.read(littleEndian: UInt64.self, from: &offset)
    summaryOffsetStart = try buffer.read(littleEndian: UInt64.self, from: &offset)
    summaryCRC = try buffer.read(littleEndian: UInt32.self, from: &offset)
  }

  public func serializeFields(to data: inout Data) {
    data.append(littleEndian: summaryStart)
    data.append(littleEndian: summaryOffsetStart)
    data.append(littleEndian: summaryCRC)
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

  public init(deserializingFieldsFrom buffer: UnsafeRawBufferPointer) throws {
    var offset = 0
    id = try buffer.read(littleEndian: SchemaID.self, from: &offset)
    name = try buffer.readPrefixedString(from: &offset)
    encoding = try buffer.readPrefixedString(from: &offset)
    data = try buffer.readUInt32PrefixedData(from: &offset)
  }

  public func serializeFields(to data: inout Data) {
    data.append(littleEndian: id)
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

  public init(deserializingFieldsFrom buffer: UnsafeRawBufferPointer) throws {
    var offset = 0
    id = try buffer.read(littleEndian: ChannelID.self, from: &offset)
    schemaID = try buffer.read(littleEndian: SchemaID.self, from: &offset)
    topic = try buffer.readPrefixedString(from: &offset)
    messageEncoding = try buffer.readPrefixedString(from: &offset)
    metadata = try buffer.readPrefixedStringMap(from: &offset)
  }

  public func serializeFields(to data: inout Data) {
    data.append(littleEndian: id)
    data.append(littleEndian: schemaID)
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

  public init(deserializingFieldsFrom buffer: UnsafeRawBufferPointer) throws {
    var offset = 0
    channelID = try buffer.read(littleEndian: ChannelID.self, from: &offset)
    sequence = try buffer.read(littleEndian: UInt32.self, from: &offset)
    logTime = try buffer.read(littleEndian: Timestamp.self, from: &offset)
    publishTime = try buffer.read(littleEndian: Timestamp.self, from: &offset)
    data = Data(buffer.suffix(from: offset))
  }

  public func serializeFields(to data: inout Data) {
    data.append(littleEndian: channelID)
    data.append(littleEndian: sequence)
    data.append(littleEndian: logTime)
    data.append(littleEndian: publishTime)
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

  public init(deserializingFieldsFrom buffer: UnsafeRawBufferPointer) throws {
    var offset = 0
    messageStartTime = try buffer.read(littleEndian: Timestamp.self, from: &offset)
    messageEndTime = try buffer.read(littleEndian: Timestamp.self, from: &offset)
    uncompressedSize = try buffer.read(littleEndian: UInt64.self, from: &offset)
    uncompressedCRC = try buffer.read(littleEndian: UInt32.self, from: &offset)
    compression = try buffer.readPrefixedString(from: &offset)
    records = try buffer.readUInt64PrefixedData(from: &offset)
  }

  public func serializeFields(to data: inout Data) {
    data.append(littleEndian: messageStartTime)
    data.append(littleEndian: messageEndTime)
    data.append(littleEndian: uncompressedSize)
    data.append(littleEndian: uncompressedCRC)
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

  public init(deserializingFieldsFrom buffer: UnsafeRawBufferPointer) throws {
    var offset = 0
    channelID = try buffer.read(littleEndian: ChannelID.self, from: &offset)
    records = try buffer.readPrefixedTupleArray(from: &offset)
  }

  public func serializeFields(to data: inout Data) {
    data.append(littleEndian: channelID)
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

  public init(deserializingFieldsFrom buffer: UnsafeRawBufferPointer) throws {
    var offset = 0
    messageStartTime = try buffer.read(littleEndian: Timestamp.self, from: &offset)
    messageEndTime = try buffer.read(littleEndian: Timestamp.self, from: &offset)
    chunkStartOffset = try buffer.read(littleEndian: UInt64.self, from: &offset)
    chunkLength = try buffer.read(littleEndian: UInt64.self, from: &offset)
    messageIndexOffsets = try buffer.readPrefixedMap(from: &offset)
    messageIndexLength = try buffer.read(littleEndian: UInt64.self, from: &offset)
    compression = try buffer.readPrefixedString(from: &offset)
    compressedSize = try buffer.read(littleEndian: UInt64.self, from: &offset)
    uncompressedSize = try buffer.read(littleEndian: UInt64.self, from: &offset)
  }

  public func serializeFields(to data: inout Data) {
    data.append(littleEndian: messageStartTime)
    data.append(littleEndian: messageEndTime)
    data.append(littleEndian: chunkStartOffset)
    data.append(littleEndian: chunkLength)
    data.appendPrefixedMap(messageIndexOffsets)
    data.append(littleEndian: messageIndexLength)
    data.appendPrefixedString(compression)
    data.append(littleEndian: compressedSize)
    data.append(littleEndian: uncompressedSize)
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

  public init(deserializingFieldsFrom buffer: UnsafeRawBufferPointer) throws {
    var offset = 0
    logTime = try buffer.read(littleEndian: Timestamp.self, from: &offset)
    createTime = try buffer.read(littleEndian: Timestamp.self, from: &offset)
    name = try buffer.readPrefixedString(from: &offset)
    contentType = try buffer.readPrefixedString(from: &offset)
    data = try buffer.readUInt64PrefixedData(from: &offset)
    let crcEndOffset = offset
    let expectedCRC = try buffer.read(littleEndian: UInt32.self, from: &offset)
    if expectedCRC != 0 {
      var crc = CRC32()
      crc.update(buffer[..<crcEndOffset])
      if expectedCRC != crc.final {
        throw MCAPReadError.invalidCRC
      }
    }
  }

  public func serializeFields(to data: inout Data) {
    let fieldsStartOffset = data.count
    data.append(littleEndian: logTime)
    data.append(littleEndian: createTime)
    data.appendPrefixedString(name)
    data.appendPrefixedString(contentType)
    data.appendUInt64PrefixedData(self.data)
    var crc = CRC32()
    crc.update(data[fieldsStartOffset ..< data.count])
    data.append(littleEndian: crc.final)
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

  public init(
    offset: UInt64,
    length: UInt64,
    logTime: Timestamp,
    createTime: Timestamp,
    dataSize: UInt64,
    name: String,
    contentType: String
  ) {
    self.offset = offset
    self.length = length
    self.logTime = logTime
    self.createTime = createTime
    self.dataSize = dataSize
    self.name = name
    self.contentType = contentType
  }

  public init(deserializingFieldsFrom buffer: UnsafeRawBufferPointer) throws {
    var offset = 0
    self.offset = try buffer.read(littleEndian: UInt64.self, from: &offset)
    length = try buffer.read(littleEndian: UInt64.self, from: &offset)
    logTime = try buffer.read(littleEndian: Timestamp.self, from: &offset)
    createTime = try buffer.read(littleEndian: Timestamp.self, from: &offset)
    dataSize = try buffer.read(littleEndian: UInt64.self, from: &offset)
    name = try buffer.readPrefixedString(from: &offset)
    contentType = try buffer.readPrefixedString(from: &offset)
  }

  public func serializeFields(to data: inout Data) {
    data.append(littleEndian: offset)
    data.append(littleEndian: length)
    data.append(littleEndian: logTime)
    data.append(littleEndian: createTime)
    data.append(littleEndian: dataSize)
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

  public init(
    messageCount: UInt64 = 0,
    schemaCount: UInt16 = 0,
    channelCount: UInt32 = 0,
    attachmentCount: UInt32 = 0,
    metadataCount: UInt32 = 0,
    chunkCount: UInt32 = 0,
    messageStartTime: Timestamp = 0,
    messageEndTime: Timestamp = 0,
    channelMessageCounts: [ChannelID: UInt64] = [:]
  ) {
    self.messageCount = messageCount
    self.schemaCount = schemaCount
    self.channelCount = channelCount
    self.attachmentCount = attachmentCount
    self.metadataCount = metadataCount
    self.chunkCount = chunkCount
    self.messageStartTime = messageStartTime
    self.messageEndTime = messageEndTime
    self.channelMessageCounts = channelMessageCounts
  }

  public init(deserializingFieldsFrom buffer: UnsafeRawBufferPointer) throws {
    var offset = 0
    messageCount = try buffer.read(littleEndian: UInt64.self, from: &offset)
    schemaCount = try buffer.read(littleEndian: UInt16.self, from: &offset)
    channelCount = try buffer.read(littleEndian: UInt32.self, from: &offset)
    attachmentCount = try buffer.read(littleEndian: UInt32.self, from: &offset)
    metadataCount = try buffer.read(littleEndian: UInt32.self, from: &offset)
    chunkCount = try buffer.read(littleEndian: UInt32.self, from: &offset)
    messageStartTime = try buffer.read(littleEndian: Timestamp.self, from: &offset)
    messageEndTime = try buffer.read(littleEndian: Timestamp.self, from: &offset)
    channelMessageCounts = try buffer.readPrefixedMap(from: &offset)
  }

  public func serializeFields(to data: inout Data) {
    data.append(littleEndian: messageCount)
    data.append(littleEndian: schemaCount)
    data.append(littleEndian: channelCount)
    data.append(littleEndian: attachmentCount)
    data.append(littleEndian: metadataCount)
    data.append(littleEndian: chunkCount)
    data.append(littleEndian: messageStartTime)
    data.append(littleEndian: messageEndTime)
    data.appendPrefixedMap(channelMessageCounts)
  }
}

public struct Metadata: Record {
  public static let opcode = Opcode.metadata
  public var name: String
  public var metadata: [String: String]

  public init(name: String, metadata: [String: String]) {
    self.name = name
    self.metadata = metadata
  }

  public init(deserializingFieldsFrom buffer: UnsafeRawBufferPointer) throws {
    var offset = 0
    name = try buffer.readPrefixedString(from: &offset)
    metadata = try buffer.readPrefixedStringMap(from: &offset)
  }

  public func serializeFields(to data: inout Data) {
    data.appendPrefixedString(name)
    data.appendPrefixedMap(metadata)
  }
}

public struct MetadataIndex: Record {
  public static let opcode = Opcode.metadataIndex
  public var offset: UInt64
  public var length: UInt64
  public var name: String

  public init(offset: UInt64, length: UInt64, name: String) {
    self.offset = offset
    self.length = length
    self.name = name
  }

  public init(deserializingFieldsFrom buffer: UnsafeRawBufferPointer) throws {
    var offset = 0
    self.offset = try buffer.read(littleEndian: UInt64.self, from: &offset)
    length = try buffer.read(littleEndian: UInt64.self, from: &offset)
    name = try buffer.readPrefixedString(from: &offset)
  }

  public func serializeFields(to data: inout Data) {
    data.append(littleEndian: offset)
    data.append(littleEndian: length)
    data.appendPrefixedString(name)
  }
}

public struct SummaryOffset: Record {
  public static let opcode = Opcode.summaryOffset
  public var groupOpcode: UInt8
  public var groupStart: UInt64
  public var groupLength: UInt64

  public init(groupOpcode: UInt8, groupStart: UInt64, groupLength: UInt64) {
    self.groupOpcode = groupOpcode
    self.groupStart = groupStart
    self.groupLength = groupLength
  }

  public init(deserializingFieldsFrom buffer: UnsafeRawBufferPointer) throws {
    var offset = 0
    groupOpcode = try buffer.read(littleEndian: UInt8.self, from: &offset)
    groupStart = try buffer.read(littleEndian: UInt64.self, from: &offset)
    groupLength = try buffer.read(littleEndian: UInt64.self, from: &offset)
  }

  public func serializeFields(to data: inout Data) {
    data.append(groupOpcode)
    data.append(littleEndian: groupStart)
    data.append(littleEndian: groupLength)
  }
}

public struct DataEnd: Record {
  public static let opcode = Opcode.dataEnd
  public var dataSectionCRC: UInt32

  public init(dataSectionCRC: UInt32) {
    self.dataSectionCRC = dataSectionCRC
  }

  public init(deserializingFieldsFrom buffer: UnsafeRawBufferPointer) throws {
    var offset = 0
    dataSectionCRC = try buffer.read(littleEndian: UInt32.self, from: &offset)
  }

  public func serializeFields(to data: inout Data) {
    data.append(littleEndian: dataSectionCRC)
  }
}
