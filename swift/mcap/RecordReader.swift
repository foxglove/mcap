import struct Foundation.Data

class RecordReader {
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
    bytesRemaining == 0
  }

  var bytesRemaining: Int {
    buffer.count - offset
  }

  private func _trim() {
    buffer.removeSubrange(..<offset)
    offset = 0
  }

  func readMagic() throws -> Bool {
    if offset + 8 < buffer.count {
      let prefix = buffer[offset ..< offset + 8]
      if !mcapMagic.elementsEqual(prefix) {
        throw MCAPReadError.invalidMagic(actual: Array(prefix))
      }
      offset += 8
      return true
    }
    return false
  }

  func nextRecord() throws -> Record? {
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
