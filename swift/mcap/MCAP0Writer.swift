import Foundation

public protocol IWritable {
  func position() -> UInt64
  mutating func write(_ data: Data) async
}

public final class MCAP0Writer {

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
  private var runningCRC = CRC32()

  private var nextChannelID: ChannelID
  private var nextSchemaID: SchemaID = 1
  private var schemasByID: [SchemaID: Schema] = [:]
  private var channelsByID: [ChannelID: Channel] = [:]

  //FIXME: don't actually track these if disabled
  private var chunkIndexes: [ChunkIndex] = []
  private var attachmentIndexes: [AttachmentIndex] = []
  private var metadataIndexes: [MetadataIndex] = []
  private var statistics = Statistics()

  public init(_ writable: IWritable, _ options: Options = Options()) {
    self.writable = writable
    self.options = options
    self.nextChannelID = options.startChannelID
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
    schema.serialize(to: &buffer)
    schemasByID[id] = schema
    statistics.schemaCount += 1
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
    channel.serialize(to: &buffer)
    channelsByID[id] = channel
    statistics.channelCount += 1
    return id
  }

  public func addMessage(_ message: Message) async {
    message.serialize(to: &buffer)
    statistics.channelMessageCounts[message.channelID, default: 0] += 1
    if statistics.messageCount == 0 || message.logTime < statistics.messageStartTime {
      statistics.messageStartTime = message.logTime
    }
    if statistics.messageCount == 0 || message.logTime > statistics.messageEndTime {
      statistics.messageEndTime = message.logTime
    }
    statistics.messageCount += 1
  }

  public func addAttachment(_ attachment: Attachment) async {
    let offset = _position()
    attachment.serialize(to: &buffer)
    let length = _position() - offset
    statistics.attachmentCount += 1
    attachmentIndexes.append(
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
    statistics.metadataCount += 1
    metadataIndexes.append(
      MetadataIndex(
        offset: offset,
        length: length,
        name: metadata.name
      )
    )
  }

  public func end() async {
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

    if options.repeatSchemas {
      group(of: .schema) {
        schemasByID.values.forEach { $0.serialize(to: &buffer) }
      }
    }

    if options.repeatChannels {
      group(of: .channel) {
        channelsByID.values.forEach { $0.serialize(to: &buffer) }
      }
    }

    if options.useStatistics {
      group(of: .statistics) {
        statistics.serialize(to: &buffer)
      }
    }

    if options.useChunkIndex {
      group(of: .chunkIndex) {
        chunkIndexes.forEach { $0.serialize(to: &buffer) }
      }
    }

    if options.useAttachmentIndex {
      group(of: .attachmentIndex) {
        attachmentIndexes.forEach { $0.serialize(to: &buffer) }
      }
    }

    if options.useMetadataIndex {
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
    runningCRC.update(footerData[..<(footerData.endIndex - 4)])
    footer.summaryCRC = runningCRC.final
    footer.serialize(to: &buffer)
    buffer.append(MCAP0_MAGIC)
    await _flush()
  }
}
