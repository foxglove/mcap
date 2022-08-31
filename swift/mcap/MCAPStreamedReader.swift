import crc
import struct Foundation.Data

public typealias DecompressHandlers =
  [String: (_ compressedData: Data, _ decompressedSize: UInt64) throws -> Data]

/**
 A reader that parses MCAP data from a stream. Rather than expecting the entire MCAP file to be
 available up front, this reader emits records as they are encountered. This means it does not use
 the summary or index data to read the file, and can be used when only some of the data is available
 (such as when streaming over the network).

 Call ``append(_:)`` when new data is available to add it to the reader's internal buffer. Then,
 call ``nextRecord()`` repeatedly to consume records that are fully parseable.

 ```
 let reader = MCAPStreamedReader()
 while let data = readSomeData() {
   reader.append(data)
   while let record = try reader.nextRecord() {
     // process a record...
   }
 }
 ```
 */
public class MCAPStreamedReader {
  private let recordReader = RecordReader()
  private var chunkReader: RecordReader?
  private var readHeaderMagic = false
  private var decompressHandlers: DecompressHandlers

  /**
   Create a streamed reader.

   - Parameter decompressHandlers: A user-specified collection of functions to be used to decompress
     chunks in the MCAP file. When a chunk is encountered, its `compression` field is used as the
     key to select one of the functions in `decompressHandlers`. If a decompress handler is not
     available for the chunk's `compression`, a `MCAPReadError.unsupportedCompression` will be
     thrown.
   */
  public init(decompressHandlers: DecompressHandlers = [:]) {
    self.decompressHandlers = decompressHandlers
  }

  public func append(_ data: Data) {
    recordReader.append(data)
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
