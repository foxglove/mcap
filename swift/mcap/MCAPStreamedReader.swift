import struct Foundation.Data

public class MCAPStreamedReader {
  private let recordReader = RecordReader()
  private var chunkReader: RecordReader?
  private var readHeaderMagic = false

  public init() {}

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
        chunkReader = RecordReader(chunk.records)
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
}

fileprivate class RecordReader {
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
      if !MCAP0_MAGIC.elementsEqual(buffer[offset..<offset+8]) {
        throw MCAPReadError.invalidMagic
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
        offset += 1
        var recordLength: UInt64 = 0
        withUnsafeMutableBytes(of: &recordLength) { rawLength in
          _ = buf.copyBytes(to: rawLength, from: offset..<offset+8)
        }
        recordLength = UInt64(littleEndian: recordLength)
        offset += 8
        guard offset + Int(recordLength) <= buf.count else {
          return nil
        }
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
