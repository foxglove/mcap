import Foundation

public protocol IWritable {
  func write(_ data: Data) async
}

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
  let startChannelId: ChannelID
  let chunkSize: UInt64
  let compressChunk: Optional<(_ chunkData: Data) -> (compression: String, compressedData: Data)>

  public init(useStatistics: Bool = true, useSummaryOffsets: Bool = true, useChunks: Bool = true, repeatSchemas: Bool = true, repeatChannels: Bool = true, useAttachmentIndex: Bool = true, useMetadataIndex: Bool = true, useMessageIndex: Bool = true, useChunkIndex: Bool = true, startChannelId: ChannelID = 0, chunkSize: UInt64 = 10 * 1024 * 1024, compressChunk: Optional<(_ chunkData: Data) -> (compression: String, compressedData: Data)> = nil) {
    self.useStatistics = useStatistics
    self.useSummaryOffsets = useSummaryOffsets
    self.useChunks = useChunks
    self.repeatSchemas = repeatSchemas
    self.repeatChannels = repeatChannels
    self.useAttachmentIndex = useAttachmentIndex
    self.useMetadataIndex = useMetadataIndex
    self.useMessageIndex = useMessageIndex
    self.useChunkIndex = useChunkIndex
    self.startChannelId = startChannelId
    self.chunkSize = chunkSize
    self.compressChunk = compressChunk
  }
}

public final class MCAP0Writer {

  private let writable: IWritable
  private let options: Options

  public init(_ writable: IWritable, _ options: Options = Options()) {
    self.writable = writable
    self.options = options
  }

  func start(library: String, profile: String) async {
//    writable.write(<#T##data: Data##Data#>)
  }
}
