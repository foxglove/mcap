import Collections
import crc
import struct Foundation.Data

public protocol IRandomAccessReadable {
  func size() -> UInt64
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

/**
 A reader that parses MCAP data from a random-access data source. This reader can use the summary
 or index data to seek in the file or read in log-time order, and requires the entire file to be accessible via
 byte ranges.

 Call ``append(_:)`` when new data is available to add it to the reader's internal buffer. Then,
 call ``nextRecord()`` repeatedly to consume records that are fully parseable.

 ```
 let readable = // readable
 let reader = MCAPIndexedReader(readable)
 for async let record in reader.readMessages() {
   reader.append(data)
   while let record = try reader.nextRecord() {
     // process a record...
   }
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
   - Parameter readable: A data source from which to read an MCAP file.
   - Parameter decompressHandlers: A user-specified collection of functions to be used to decompress
     chunks in the MCAP file. When a chunk is encountered, its `compression` field is used as the
     key to select one of the functions in `decompressHandlers`. If a decompress handler is not
     available for the chunk's `compression`, a `MCAPReadError.unsupportedCompression` will be
     thrown.
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
    let footerLengthWithoutCRC = 8 /* summaryStart */ + 8 /* summaryOffsetStart */ + 4 /* crc */
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

      self.footer = try footerAndMagic.withUnsafeBytes { buf in
        try Footer(deserializingFieldsFrom: UnsafeRawBufferPointer(rebasing: buf[recordPrefixLength ..<
            recordPrefixLength + footerLength]))
      }
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

  private func _decompress(_ chunk: Chunk) throws -> Data {
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

  func readMessages(topics _: [String]? = nil, startTime _: UInt64? = nil,
                    endTime _: UInt64? = nil) -> AnyIterator<Message>
  {
    struct MessageIndexCursor {
      let channelID: ChannelID
      let index: Int
      let records: [(logTime: Timestamp, offset: UInt64)]
    }
    struct ChunkCursor: Comparable {
      let chunkIndex: ChunkIndex
      let messageIndexCursors: Heap<MessageIndexCursor>

      static func == (_: ChunkCursor, _: ChunkCursor) -> Bool {}

      static func < (_: ChunkCursor, _: ChunkCursor) -> Bool {}
    }
    var chunkCursors = Heap<ChunkCursor>()

    return AnyIterator {
      return nil
    }
  }
}
