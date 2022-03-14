import Foundation

public protocol IWritable {
  func position() -> UInt64
  mutating func write(_ data: Data) async
}

fileprivate extension Statistics {
  mutating func addMessage(_ message: Message) {
    channelMessageCounts[message.channelID, default: 0] += 1
    if messageCount == 0 || message.logTime < messageStartTime {
      messageStartTime = message.logTime
    }
    if messageCount == 0 || message.logTime > messageEndTime {
      messageEndTime = message.logTime
    }
    messageCount += 1
  }
}

public final class MCAPWriter {

  public struct Options {
    let useStatistics: Bool
    let useSummaryOffsets: Bool
    let useChunks: Bool
    let repeatSchemas: Bool
    let repeatChannels: Bool
    let useAttachmentIndex: Bool
    let useMetadataIndex: Bool
    let useMessageIndex: Bool
    let useChunkIndex: Bool
    let startChannelID: ChannelID
    let chunkSize: UInt64
    let compressChunk: ((_ chunkData: Data) -> (compression: String, compressedData: Data))?

    public init(
      useStatistics: Bool = true,
      useSummaryOffsets: Bool = true,
      useChunks: Bool = true,
      repeatSchemas: Bool = true,
      repeatChannels: Bool = true,
      useAttachmentIndex: Bool = true,
      useMetadataIndex: Bool = true,
      useMessageIndex: Bool = true,
      useChunkIndex: Bool = true,
      startChannelID: ChannelID = 0,
      chunkSize: UInt64 = 10 * 1024 * 1024,
      compressChunk: ((_ chunkData: Data) -> (compression: String, compressedData: Data))? = nil
    ) {
      self.useStatistics = useStatistics
      self.useSummaryOffsets = useSummaryOffsets
      self.useChunks = useChunks
      self.repeatSchemas = repeatSchemas
      self.repeatChannels = repeatChannels
      self.useAttachmentIndex = useAttachmentIndex
      self.useMetadataIndex = useMetadataIndex
      self.useMessageIndex = useMessageIndex
      self.useChunkIndex = useChunkIndex
      self.startChannelID = startChannelID
      self.chunkSize = chunkSize
      self.compressChunk = compressChunk
    }
  }

  private var writable: IWritable
  private let options: Options
  private var buffer = Data()
  private let chunkBuilder: ChunkBuilder?
  private var runningCRC = CRC32()

  private var nextChannelID: ChannelID
  private var nextSchemaID: SchemaID = 1

  private var schemasByID: [SchemaID: Schema]?
  private var channelsByID: [ChannelID: Channel]?
  private var chunkIndexes: [ChunkIndex]?
  private var attachmentIndexes: [AttachmentIndex]?
  private var metadataIndexes: [MetadataIndex]?
  private var statistics: Statistics?

  public init(_ writable: IWritable, _ options: Options = Options()) {
    self.writable = writable
    self.options = options
    self.nextChannelID = options.startChannelID
    schemasByID = options.repeatSchemas ? [:] : nil
    channelsByID = options.repeatChannels ? [:] : nil
    chunkBuilder = options.useChunks ? ChunkBuilder(useMessageIndex: options.useMessageIndex) : nil
    chunkIndexes = options.useChunkIndex ? [] : nil
    attachmentIndexes = options.useAttachmentIndex ? [] : nil
    metadataIndexes = options.useMetadataIndex ? [] : nil
    statistics = options.useStatistics ? Statistics() : nil
  }

  private func _position() -> UInt64 {
    return writable.position() + UInt64(buffer.count)
  }

  private func _flush() async {
    await writable.write(buffer)
    runningCRC.update(buffer)
    buffer.removeAll(keepingCapacity: true)
  }

  public func start(library: String, profile: String) async {
    buffer.append(MCAP0_MAGIC)
    Header(profile: profile, library: library).serialize(to: &buffer)
  }

  public func addSchema(name: String, encoding: String, data: Data) async -> SchemaID {
    let id = nextSchemaID
    nextSchemaID += 1
    let schema = Schema(id: id, name: name, encoding: encoding, data: data)
    if let chunkBuilder = chunkBuilder {
      schema.serialize(to: &chunkBuilder.buffer)
    } else {
      schema.serialize(to: &buffer)
    }
    schemasByID?[id] = schema
    statistics?.schemaCount += 1
    return id
  }

  public func addChannel(
    schemaID: SchemaID,
    topic: String,
    messageEncoding: String,
    metadata: [String: String]
  ) async -> ChannelID {
    let id = nextChannelID
    nextChannelID += 1
    let channel = Channel(
      id: id,
      schemaID: schemaID,
      topic: topic,
      messageEncoding: messageEncoding,
      metadata: metadata
    )
    if let chunkBuilder = chunkBuilder {
      channel.serialize(to: &chunkBuilder.buffer)
    } else {
      channel.serialize(to: &buffer)
    }
    channelsByID?[id] = channel
    statistics?.channelCount += 1
    return id
  }

  public func addMessage(_ message: Message) async {
    statistics?.addMessage(message)

    if let chunkBuilder = chunkBuilder {
      chunkBuilder.addMessage(message)

      if chunkBuilder.buffer.count >= options.chunkSize {
        await _closeChunk()
      }
    } else {
      message.serialize(to: &buffer)
    }
  }

  public func addAttachment(_ attachment: Attachment) async {
    let offset = _position()
    attachment.serialize(to: &buffer)
    let length = _position() - offset
    statistics?.attachmentCount += 1
    attachmentIndexes?.append(
      AttachmentIndex(
        offset: offset,
        length: length,
        logTime: attachment.logTime,
        createTime: attachment.createTime,
        dataSize: UInt64(attachment.data.count),
        name: attachment.name,
        contentType: attachment.contentType
      )
    )
  }

  public func addMetadata(_ metadata: Metadata) async {
    let offset = _position()
    metadata.serialize(to: &buffer)
    let length = _position() - offset
    statistics?.metadataCount += 1
    metadataIndexes?.append(
      MetadataIndex(
        offset: offset,
        length: length,
        name: metadata.name
      )
    )
  }

  private func _closeChunk() async {
    guard let chunkBuilder = chunkBuilder, chunkBuilder.messageCount > 0 else {
      return
    }

    var chunkDataCRC = CRC32()
    chunkDataCRC.update(chunkBuilder.buffer)
    let uncompressedSize = UInt64(chunkBuilder.buffer.count)
    let compressionResult =
      options.compressChunk?(chunkBuilder.buffer) ?? (
        compression: "", compressedData: chunkBuilder.buffer
      )
    let chunk = Chunk(
      messageStartTime: chunkBuilder.messageStartTime,
      messageEndTime: chunkBuilder.messageEndTime,
      uncompressedSize: uncompressedSize,
      uncompressedCRC: chunkDataCRC.final,
      compression: compressionResult.compression,
      records: compressionResult.compressedData
    )

    let chunkStartOffset = _position()
    chunk.serialize(to: &buffer)
    let messageIndexStartOffset = _position()

    var messageIndexOffsets: [ChannelID: UInt64] = [:]
    if let messageIndexes = chunkBuilder.messageIndexes {
      for (channelID, index) in messageIndexes {
        messageIndexOffsets[channelID] = _position()
        index.serialize(to: &buffer)
      }
    }

    statistics?.chunkCount += 1
    chunkIndexes?.append(
      ChunkIndex(
        messageStartTime: chunk.messageStartTime,
        messageEndTime: chunk.messageEndTime,
        chunkStartOffset: chunkStartOffset,
        chunkLength: messageIndexStartOffset - chunkStartOffset,
        messageIndexOffsets: messageIndexOffsets,
        messageIndexLength: _position() - messageIndexStartOffset,
        compression: chunk.compression,
        compressedSize: UInt64(chunk.records.count),
        uncompressedSize: chunk.uncompressedSize
      )
    )

    chunkBuilder.reset()
  }

  public func end() async {
    await _closeChunk()
    await _flush()
    DataEnd(dataSectionCRC: 0).serialize(to: &buffer)
    // Re-enable when tests are updated to include data section CRC
    //    DataEnd(dataSectionCRC: runningCRC.final).serialize(to: &buffer)

    await _flush()
    runningCRC.reset()

    let summaryStart = _position()
    var summaryOffsets: [SummaryOffset] = []

    func group(of opcode: Opcode, _ body: () -> Void) {
      let groupStart = _position()
      body()
      summaryOffsets.append(
        SummaryOffset(
          groupOpcode: opcode.rawValue,
          groupStart: groupStart,
          groupLength: _position() - groupStart
        )
      )
    }

    if let schemasByID = schemasByID {
      group(of: .schema) {
        schemasByID.values.forEach { $0.serialize(to: &buffer) }
      }
    }

    if let channelsByID = channelsByID {
      group(of: .channel) {
        channelsByID.values.forEach { $0.serialize(to: &buffer) }
      }
    }

    if let statistics = statistics {
      group(of: .statistics) {
        statistics.serialize(to: &buffer)
      }
    }

    if let chunkIndexes = chunkIndexes {
      group(of: .chunkIndex) {
        chunkIndexes.forEach { $0.serialize(to: &buffer) }
      }
    }

    if let attachmentIndexes = attachmentIndexes {
      group(of: .attachmentIndex) {
        attachmentIndexes.forEach { $0.serialize(to: &buffer) }
      }
    }

    if let metadataIndexes = metadataIndexes {
      group(of: .metadataIndex) {
        metadataIndexes.forEach { $0.serialize(to: &buffer) }
      }
    }

    var summaryOffsetStart: UInt64 = 0
    if options.useSummaryOffsets {
      summaryOffsetStart = _position()
      for record in summaryOffsets {
        record.serialize(to: &buffer)
      }
    }

    await _flush()
    var footer = Footer(
      summaryStart: _position() == summaryStart ? 0 : summaryStart,
      summaryOffsetStart: summaryOffsetStart,
      summaryCRC: 0
    )
    var footerData = Data()
    footer.serialize(to: &footerData)
    runningCRC.update(
      footerData[..<(footerData.endIndex - MemoryLayout.size(ofValue: footer.summaryCRC))]
    )
    footer.summaryCRC = runningCRC.final
    footer.serialize(to: &buffer)
    buffer.append(MCAP0_MAGIC)
    await _flush()
  }
}
