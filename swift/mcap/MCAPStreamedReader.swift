import struct Foundation.Data

public class MCAPStreamedReader {
  private var buffer = Data()
  private var offset = 0
  private var readHeaderMagic = false

  public init() {}

  public func append(_ data: Data) {
    _trim()
    buffer.append(data)
  }

  private func _trim() {
    buffer.removeSubrange(..<offset)
    offset = 0
  }

  public func nextRecord() throws -> Record? {
    try buffer.withUnsafeBytes { buf in
      if !readHeaderMagic && offset + 8 < buf.count {
        if !MCAP0_MAGIC.elementsEqual(buf[offset..<offset+8]) {
          throw MCAPReadError.invalidMagic
        }
        readHeaderMagic = true
        offset += 8
      }
      while offset + 9 < buf.count {
        let op = buf[offset]
        offset += 1
        var recordLength: UInt64 = 0
        withUnsafeMutableBytes(of: &recordLength) { rawLength in
          _ = buf.copyBytes(to: rawLength, from: offset..<offset+8)
        }
        recordLength = UInt64(littleEndian: recordLength)
        offset += 8
        if offset + Int(recordLength) >= buf.count {
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
