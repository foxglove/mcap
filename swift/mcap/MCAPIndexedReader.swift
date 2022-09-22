import crc
import struct Foundation.Data

public protocol IRandomAccessReadable {
  func size() -> UInt64
  func read(offset: UInt64, length: UInt64) -> Data?
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

//    let headerPrefix = try self._checkedRead(offset: 0, length: UInt64(MCAP0_MAGIC.count) + 1 /* header opcode */ + 8 /* header record length */)
//    let headerMagic = headerPrefix.prefix(MCAP0_MAGIC.count)
//    if !headerMagic.elementsEqual(MCAP0_MAGIC) {
//      throw MCAPReadError.invalidMagic(actual: Array(headerMagic))
//    }
//    if headerPrefix[MCAP0_MAGIC.count] != Opcode.header.rawValue {
//      throw MCAPReadError.missingHeader(actualOpcode: headerPrefix[MCAP0_MAGIC.count])
//    }
//    var offset = MCAP0_MAGIC.count + 1
//    let headerLength = try headerPrefix.withUnsafeBytes {
//      try $0.read(littleEndian: UInt64.self, from: &offset)
//    }
//
//    let headerData = try self._checkedRead(offset: UInt64(offset), length: headerLength)
//    try Header.deserializingFields(from: headerData)
  }

  private func _checkedRead(offset: UInt64, length: UInt64) throws -> Data {
    guard let data = self.readable.read(offset: offset, length: length) else {
      throw MCAPReadError.readBeyondBounds(offset: offset, length: length)
    }
    if UInt64(data.count) != length {
      throw MCAPReadError.readFailed(offset: offset, expectedLength: length, actualLength: UInt64(data.count))
    }
    return data
  }

  public func readMessages() -> AnySequence {
    return AnySequence(
  }

  public func nextRecord() throws -> Record? {
    if !readHeaderMagic {
      if try !recordReader.readMagic() {
        return nil
      }
      readHeaderMagic = true
    }

    if chunkReader == nil {
      let record = try recordReader.nextRecord()
      switch record {
      case let chunk as Chunk:
        chunkReader = RecordReader(try _decompress(chunk))
      default:
        return record
      }
    }

    if let chunkReader = chunkReader {
      defer {
        if chunkReader.isDone {
          self.chunkReader = nil
        }
      }
      if let record = try chunkReader.nextRecord() {
        return record
      }
      throw MCAPReadError.extraneousDataInChunk
    }

    return nil
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
}

private class RecordReader {
  private var buffer: Data
  private var offset = 0

  init(_ data: Data = Data()) {
    buffer = data
  }

  func append(_ data: Data) {
    _trim()
    buffer.append(data)
  }

  var isDone: Bool {
    offset == buffer.count
  }

  private func _trim() {
    buffer.removeSubrange(..<offset)
    offset = 0
  }

  public func readMagic() throws -> Bool {
    if offset + 8 < buffer.count {
      let prefix = buffer[offset ..< offset + 8]
      if !MCAP0_MAGIC.elementsEqual(prefix) {
        throw MCAPReadError.invalidMagic(actual: Array(prefix))
      }
      offset += 8
      return true
    }
    return false
  }

  public func nextRecord() throws -> Record? {
    try buffer.withUnsafeBytes { buf in
      while offset + 9 < buf.count {
        let op = buf[offset]
        var recordLength: UInt64 = 0
        withUnsafeMutableBytes(of: &recordLength) { rawLength in
          _ = buf.copyBytes(to: rawLength, from: offset + 1 ..< offset + 9)
        }
        recordLength = UInt64(littleEndian: recordLength)
        guard offset + 9 + Int(recordLength) <= buf.count else {
          return nil
        }
        offset += 9
        defer {
          offset += Int(recordLength)
        }
        guard let op = Opcode(rawValue: op) else {
          continue
        }
        let recordBuffer = UnsafeRawBufferPointer(rebasing: buf[offset ..< offset + Int(recordLength)])
        switch op {
        case .header:
          return try Header(deserializingFieldsFrom: recordBuffer)
        case .footer:
          return try Footer(deserializingFieldsFrom: recordBuffer)
        case .schema:
          return try Schema(deserializingFieldsFrom: recordBuffer)
        case .channel:
          return try Channel(deserializingFieldsFrom: recordBuffer)
        case .message:
          return try Message(deserializingFieldsFrom: recordBuffer)
        case .chunk:
          return try Chunk(deserializingFieldsFrom: recordBuffer)
        case .messageIndex:
          return try MessageIndex(deserializingFieldsFrom: recordBuffer)
        case .chunkIndex:
          return try ChunkIndex(deserializingFieldsFrom: recordBuffer)
        case .attachment:
          return try Attachment(deserializingFieldsFrom: recordBuffer)
        case .attachmentIndex:
          return try AttachmentIndex(deserializingFieldsFrom: recordBuffer)
        case .statistics:
          return try Statistics(deserializingFieldsFrom: recordBuffer)
        case .metadata:
          return try Metadata(deserializingFieldsFrom: recordBuffer)
        case .metadataIndex:
          return try MetadataIndex(deserializingFieldsFrom: recordBuffer)
        case .summaryOffset:
          return try SummaryOffset(deserializingFieldsFrom: recordBuffer)
        case .dataEnd:
          return try DataEnd(deserializingFieldsFrom: recordBuffer)
        }
      }
      return nil
    }
  }
}
