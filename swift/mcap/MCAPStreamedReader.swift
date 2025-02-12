import CRC
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

 ```swift
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

  /**
    Add data to the reader's internal buffer.
   */
  public func append(_ data: Data) {
    recordReader.append(data)
  }

  /**
   Retrieve the next record from the reader, if possible
   - Returns: The next record, or `nil` if not enough data was available to parse a record.
   - Throws: Any error encountered during reading, decompression, or parsing.
   */
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
        chunkReader = try RecordReader(_decompress(chunk))
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
      throw MCAPReadError.extraneousDataInChunk(length: chunkReader.bytesRemaining)
    }

    return nil
  }

  private func _decompress(_ chunk: Chunk) throws -> Data {
    let decompressedData: Data
    if chunk.compression.isEmpty {
      decompressedData = chunk.records
    } else if let decompress = decompressHandlers[chunk.compression] {
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
