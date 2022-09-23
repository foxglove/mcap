import Algorithms
import Collections
import CRC
import struct Foundation.Data

public protocol IRandomAccessReadable {
  /**
   - Returns: The total length of the MCAP data.
   */
  func size() -> UInt64

  /**
   - Returns: The data in the range `offset ..< offset + length`, or `nil` if the requested range is not readable.
   */
  func read(offset: UInt64, length: UInt64) -> Data?
}

private extension IRandomAccessReadable {
  func checkedRead(offset: UInt64, length: UInt64) throws -> Data {
    guard let data = self.read(offset: offset, length: length) else {
      throw MCAPReadError.readBeyondBounds(offset: offset, length: length)
    }
    if UInt64(data.count) != length {
      throw MCAPReadError.readFailed(offset: offset, expectedLength: length, actualLength: UInt64(data.count))
    }
    return data
  }
}

class MessageIndexCursor: Comparable {
  let channelID: ChannelID
  var index: Int
  let records: [(logTime: Timestamp, offset: UInt64)]
  init(channelID: ChannelID, index: Int, records: [(logTime: Timestamp, offset: UInt64)]) {
    self.channelID = channelID
    self.index = index
    self.records = records
  }

  static func == (lhs: MessageIndexCursor, rhs: MessageIndexCursor) -> Bool {
    lhs.records[lhs.index].logTime == rhs.records[rhs.index].logTime
  }

  static func < (lhs: MessageIndexCursor, rhs: MessageIndexCursor) -> Bool {
    lhs.records[lhs.index].logTime < rhs.records[rhs.index].logTime
  }
}

class ChunkCursor: Comparable {
  let chunkIndex: ChunkIndex
  private let startTime: UInt64?
  private let endTime: UInt64?
  private let relevantChannels: Set<ChannelID>?
  private var messageIndexCursors: Heap<MessageIndexCursor>?

  init(chunkIndex: ChunkIndex, startTime: UInt64?, endTime: UInt64?, relevantChannels: Set<ChannelID>?) {
    self.chunkIndex = chunkIndex
    self.startTime = startTime
    self.endTime = endTime
    self.relevantChannels = relevantChannels
  }

  static func == (lhs: ChunkCursor, rhs: ChunkCursor) -> Bool {
    lhs.chunkIndex.chunkStartOffset == rhs.chunkIndex.chunkStartOffset && lhs.sortTime == rhs.sortTime
  }

  static func < (lhs: ChunkCursor, rhs: ChunkCursor) -> Bool {
    let lhsTime = lhs.sortTime
    let rhsTime = rhs.sortTime
    if lhsTime < rhsTime {
      return true
    } else if lhsTime == rhsTime {
      return lhs.chunkIndex.chunkStartOffset < rhs.chunkIndex.chunkStartOffset
    }
    return false
  }

  var sortTime: UInt64 {
    if let messageIndexCursors = messageIndexCursors {
      let cursor = messageIndexCursors.min()!
      return cursor.records[cursor.index].logTime
    }
    return chunkIndex.messageStartTime
  }

  var hasMessageIndexes: Bool {
    messageIndexCursors != nil
  }

  func loadMessageIndexes(_ readable: IRandomAccessReadable) throws {
    var messageIndexCursors = Heap<MessageIndexCursor>()
    defer {
      self.messageIndexCursors = messageIndexCursors
    }

    guard let messageIndexStart = chunkIndex.messageIndexOffsets.values.min() else {
      return
    }
    let messageIndexes = try readable.checkedRead(offset: messageIndexStart, length: chunkIndex.messageIndexLength)
    let reader = RecordReader(messageIndexes)
    while let record = try reader.nextRecord() {
      switch record {
      case let record as MessageIndex:
        guard relevantChannels?.contains(record.channelID) ?? true else {
          continue
        }
        let records = record.records.sorted { $0.logTime < $1.logTime }
        let startIndex: Int
        if let startTime = startTime {
          startIndex = records.partitioningIndex { $0.logTime >= startTime }
        } else {
          startIndex = 0
        }
        if startIndex >= records.count {
          continue
        }
        if let endTime = endTime, records[startIndex].logTime > endTime {
          continue
        }

        messageIndexCursors.insert(MessageIndexCursor(channelID: record.channelID, index: startIndex, records: records))
      default: break
      }
    }
  }

  var hasMoreMessages: Bool {
    !messageIndexCursors!.isEmpty
  }

  func popMessage() -> (logTime: Timestamp, offset: UInt64) {
    let cursor = messageIndexCursors!.popMin()!
    let record = cursor.records[cursor.index]
    cursor.index += 1
    if cursor.index < cursor.records.count {
      messageIndexCursors!.insert(cursor)
    }
    return record
  }
}

/**
 A reader that parses MCAP data from a random-access data source. This reader uses the summary and
 index data to seek in the file and read messages in log-time order, and requires the entire file to
 be accessible via byte ranges.

 ```swift
 let readable = // readable
 let reader = try MCAPRandomAccessReader(readable)
 let iterator = reader.messageIterator(topics: ["foo", "bar"])
 while let message = try iterator.next() {
   // process a message...
 }
 ```
 */
public class MCAPRandomAccessReader {
  public let header: Header
  public let footer: Footer
  public let chunkIndexes: [ChunkIndex]
  public let attachmentIndexes: [AttachmentIndex]
  public let metadataIndexes: [MetadataIndex]
  public let channelsById: [ChannelID: Channel]
  public let schemasById: [SchemaID: Schema]
  public let statistics: Statistics?
  public let summaryOffsetsByOpcode: [Opcode.RawValue: SummaryOffset]

  private let readable: IRandomAccessReadable
  private let recordReader = RecordReader()
  private var chunkReader: RecordReader?
  private var readHeaderMagic = false
  private var decompressHandlers: DecompressHandlers

  /**
   Create a random access reader.
   - Parameter readable: A random-access data source from which to read an MCAP file.
   - Parameter decompressHandlers: A user-specified collection of functions to be used to decompress
   chunks in the MCAP file. When a chunk is encountered, its `compression` field is used as the
   key to select one of the functions in `decompressHandlers`. If a decompress handler is not
   available for the chunk's `compression`, a `MCAPReadError.unsupportedCompression` will be
   thrown.
   - Throws: Any error encountered when reading the file's summary and index data.
   */
  public init(_ readable: IRandomAccessReadable, decompressHandlers: DecompressHandlers = [:]) throws {
    self.readable = readable
    self.decompressHandlers = decompressHandlers

    let readableSize = readable.size()

    var chunkIndexes: [ChunkIndex] = []
    var attachmentIndexes: [AttachmentIndex] = []
    var metadataIndexes: [MetadataIndex] = []
    var channelsById: [ChannelID: Channel] = [:]
    var schemasById: [SchemaID: Schema] = [:]
    var summaryOffsetsByOpcode: [Opcode.RawValue: SummaryOffset] = [:]

    // Read leading magic and header
    let magicAndHeaderLength: UInt64
    do {
      let magicAndHeader = try readable.checkedRead(
        offset: 0,
        length: UInt64(MCAP0_MAGIC.count) + 1 /* header opcode */ + 8 /* header record length */
      )
      let magic = magicAndHeader.prefix(MCAP0_MAGIC.count)
      if !magic.elementsEqual(MCAP0_MAGIC) {
        throw MCAPReadError.invalidMagic(actual: Array(magic))
      }
      if magicAndHeader[MCAP0_MAGIC.count] != Opcode.header.rawValue {
        throw MCAPReadError.missingHeader(actualOpcode: magicAndHeader[MCAP0_MAGIC.count])
      }
      var offset = MCAP0_MAGIC.count + 1
      let headerLength = try magicAndHeader.withUnsafeBytes {
        try $0.read(littleEndian: UInt64.self, from: &offset)
      }
      magicAndHeaderLength = UInt64(offset) + headerLength
      let headerData = try readable.checkedRead(offset: UInt64(offset), length: headerLength)
      self.header = try Header.deserializingFields(from: headerData)
    }

    // Read trailing magic and footer
    let recordPrefixLength = 1 /* opcode */ + 8 /* record length */
    let footerLengthWithoutCRC = 8 /* summaryStart */ + 8 /* summaryOffsetStart */
    let footerLength = footerLengthWithoutCRC + 4 /* crc */
    let footerAndMagic: Data
    let footerOffset: UInt64
    do {
      let footerAndMagicLength = UInt64(recordPrefixLength + footerLength + MCAP0_MAGIC.count)
      if readableSize < magicAndHeaderLength + footerAndMagicLength {
        throw MCAPReadError.readBeyondBounds(offset: magicAndHeaderLength, length: footerAndMagicLength)
      }

      footerOffset = readableSize - footerAndMagicLength
      footerAndMagic = try readable.checkedRead(
        offset: footerOffset,
        length: footerAndMagicLength
      )
      let magic = footerAndMagic.suffix(MCAP0_MAGIC.count)
      if !magic.elementsEqual(MCAP0_MAGIC) {
        throw MCAPReadError.invalidMagic(actual: Array(magic))
      }

      self.footer = try Footer.deserializing(from: footerAndMagic)
    }

    if footer.summaryStart == 0 {
      throw MCAPReadError.missingSummary
    }

    // Read summary
    let summaryData = try readable.checkedRead(offset: footer.summaryStart, length: footerOffset - footer.summaryStart)
    if footer.summaryCRC != 0 {
      var crc = CRC32()
      crc.update(summaryData)
      crc.update(footerAndMagic.prefix(recordPrefixLength + footerLengthWithoutCRC))
      if footer.summaryCRC != crc.final {
        throw MCAPReadError.invalidCRC(expected: footer.summaryCRC, actual: crc.final)
      }
    }

    let reader = RecordReader(summaryData)
    var statistics: Statistics?
    while let record = try reader.nextRecord() {
      switch record {
      case let record as Schema:
        schemasById[record.id] = record
      case let record as Channel:
        channelsById[record.id] = record
      case let record as ChunkIndex:
        chunkIndexes.append(record)
      case let record as AttachmentIndex:
        attachmentIndexes.append(record)
      case let record as MetadataIndex:
        metadataIndexes.append(record)
      case let record as Statistics:
        statistics = record
      case let summaryOffset as SummaryOffset:
        summaryOffsetsByOpcode[summaryOffset.groupOpcode] = summaryOffset
      default:
        break
      }
    }
    if !reader.isDone {
      throw MCAPReadError.extraneousDataInSummary(length: reader.bytesRemaining)
    }

    self.statistics = statistics
    self.chunkIndexes = chunkIndexes
    self.attachmentIndexes = attachmentIndexes
    self.metadataIndexes = metadataIndexes
    self.channelsById = channelsById
    self.schemasById = schemasById
    self.summaryOffsetsByOpcode = summaryOffsetsByOpcode
  }

  private func loadChunkData(_ chunkIndex: ChunkIndex) throws -> Data {
    let chunk = try Chunk
      .deserializing(from: readable.checkedRead(offset: chunkIndex.chunkStartOffset, length: chunkIndex.chunkLength))

    let decompressedData: Data
    if chunk.compression.isEmpty {
      decompressedData = chunk.records
    } else if let decompress = self.decompressHandlers[chunk.compression] {
      decompressedData = try decompress(chunk.records, chunk.uncompressedSize)
    } else {
      throw MCAPReadError.unsupportedCompression(chunk.compression)
    }

    if chunk.uncompressedCRC != 0 {
      var crc = CRC32()
      crc.update(decompressedData)
      if chunk.uncompressedCRC != crc.final {
        throw MCAPReadError.invalidCRC(expected: chunk.uncompressedCRC, actual: crc.final)
      }
    }

    return decompressedData
  }

  /**
   Create an iterator to read messages matching the given parameters.
   - Parameter topics: Topic names to include. If `nil` (the default), all topics will be read.
   - Parameter startTime: Lower bound on ``Message/logTime`` (inclusive). If `nil`, messages will be
     read with no lower time bound.
   - Parameter endTime: Upper bound on ``Message/logTime`` (inclusive). If `nil`, messages will be
     read with no upper time bound.
   */
  public func messageIterator(
    topics: [String]? = nil,
    startTime: UInt64? = nil,
    endTime: UInt64? = nil
  ) -> MessageIterator {
    MessageIterator(self, topics: topics, startTime: startTime, endTime: endTime)
  }

  /**
   An iterator that reads messages from a ``MCAPRandomAccessReader``.
   */
  public class MessageIterator {
    private var reader: MCAPRandomAccessReader
    private var chunkCursors: Heap<ChunkCursor>
    private var chunkDataByStartOffset: [UInt64: Data] = [:]

    fileprivate init(_ reader: MCAPRandomAccessReader, topics: [String]?, startTime: UInt64?, endTime: UInt64?) {
      self.reader = reader
      var relevantChannels: Set<ChannelID>?
      if let topics = topics.map(Set.init) {
        relevantChannels = Set(reader.channelsById.lazy.filter { topics.contains($1.topic) }.map(\.key))
      }
      self.chunkCursors = Heap<ChunkCursor>(reader.chunkIndexes.compactMap {
        if let startTime = startTime, $0.messageEndTime < startTime {
          return nil
        }
        if let endTime = endTime, $0.messageStartTime > endTime {
          return nil
        }
        return ChunkCursor(chunkIndex: $0, startTime: startTime, endTime: endTime, relevantChannels: relevantChannels)
      })
    }

    /**
     Retrieve the next message from the file.
     - Returns: A ``Message``, or `nil` if reading is complete.
     - Throws: Any error encountered during reading, decompression, or parsing.
     */
    public func next() throws -> Message? {
      var minCursor = chunkCursors.popMin()
      while let cursor = minCursor, !cursor.hasMessageIndexes {
        try cursor.loadMessageIndexes(reader.readable)
        if cursor.hasMoreMessages {
          chunkCursors.insert(cursor)
        }
        minCursor = chunkCursors.popMin()
      }
      guard let minCursor = minCursor else {
        return nil
      }
      let chunkData: Data
      if let data = chunkDataByStartOffset[minCursor.chunkIndex.chunkStartOffset] {
        chunkData = data
      } else {
        chunkData = try reader.loadChunkData(minCursor.chunkIndex)
        chunkDataByStartOffset[minCursor.chunkIndex.chunkStartOffset] = chunkData
      }

      let (logTime: _, offset) = minCursor.popMessage()
      if offset >= chunkData.count {
        throw MCAPReadError.invalidMessageIndexEntry(
          offset: offset,
          chunkStartOffset: minCursor.chunkIndex.chunkStartOffset,
          chunkLength: minCursor.chunkIndex.chunkLength
        )
      }
      if minCursor.hasMoreMessages {
        chunkCursors.insert(minCursor)
      } else {
        chunkDataByStartOffset[minCursor.chunkIndex.chunkStartOffset] = nil
      }
      return try Message.deserializing(from: chunkData, at: Int(offset))
    }
  }
}
