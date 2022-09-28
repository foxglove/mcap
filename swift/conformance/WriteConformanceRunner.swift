import Foundation
import MCAP

class Buffer: IWritable {
  var data = Data()
  func position() -> UInt64 {
    UInt64(data.count)
  }

  func write(_ other: Data) async {
    data.append(other)
  }
}

enum TestRecord {
  case header(Header)
  case attachment(Attachment)
  case metadata(Metadata)
  case schema(Schema)
  case channel(Channel)
  case message(Message)
  case dataEnd
}

// swiftlint:disable force_cast
enum WriteConformanceRunner {
  static func main() async throws {
    if CommandLine.arguments.count < 3 {
      fatalError("Usage: conformance write [test-data.json]")
    }
    let filename = CommandLine.arguments[2]
    let data = try Data(contentsOf: URL(fileURLWithPath: filename))

    let testData = try JSONSerialization.jsonObject(with: data) as! [String: Any]
    let features =
      ((testData["meta"] as! [String: Any])["variant"] as! [String: Any])["features"] as! [String]

    let testRecords: [TestRecord] = (testData["records"] as! [[String: Any]]).compactMap { record in
      let fields = record["fields"] as! [[Any]]
      let fieldsByName = Dictionary(uniqueKeysWithValues: fields.map { ($0[0] as! String, $0[1]) })
      switch record["type"] as! String {
      case "Header":
        return .header(
          Header(
            profile: fieldsByName["profile"] as! String,
            library: fieldsByName["library"] as! String
          )
        )
      case "Schema":
        return .schema(
          Schema(
            id: SchemaID(fieldsByName["id"] as! String)!,
            name: fieldsByName["name"] as! String,
            encoding: fieldsByName["encoding"] as! String,
            data: Data((fieldsByName["data"] as! [String]).map { UInt8($0)! })
          )
        )
      case "Channel":
        return .channel(
          Channel(
            id: ChannelID(fieldsByName["id"] as! String)!,
            schemaID: SchemaID(fieldsByName["schema_id"] as! String)!,
            topic: fieldsByName["topic"] as! String,
            messageEncoding: fieldsByName["message_encoding"] as! String,
            metadata: fieldsByName["metadata"] as! [String: String]
          )
        )
      case "Message":
        return .message(
          Message(
            channelID: ChannelID(fieldsByName["channel_id"] as! String)!,
            sequence: UInt32(fieldsByName["sequence"] as! String)!,
            logTime: Timestamp(fieldsByName["log_time"] as! String)!,
            publishTime: Timestamp(fieldsByName["publish_time"] as! String)!,
            data: Data((fieldsByName["data"] as! [String]).map { UInt8($0)! })
          )
        )
      case "Attachment":
        return .attachment(
          Attachment(
            logTime: Timestamp(fieldsByName["log_time"] as! String)!,
            createTime: Timestamp(fieldsByName["create_time"] as! String)!,
            name: fieldsByName["name"] as! String,
            mediaType: fieldsByName["media_type"] as! String,
            data: Data((fieldsByName["data"] as! [String]).map { UInt8($0)! })
          )
        )
      case "Metadata":
        return .metadata(
          Metadata(
            name: fieldsByName["name"] as! String,
            metadata: fieldsByName["metadata"] as! [String: String]
          )
        )
      case "DataEnd":
        return .dataEnd
      default:
        return nil
      }
    }

    let buffer = Buffer()
    let writer = MCAPWriter(
      buffer,
      MCAPWriter.Options(
        useStatistics: features.contains("st"),
        useSummaryOffsets: features.contains("sum"),
        useChunks: features.contains("ch"),
        repeatSchemas: features.contains("rsh"),
        repeatChannels: features.contains("rch"),
        useAttachmentIndex: features.contains("ax"),
        useMetadataIndex: features.contains("mdx"),
        useMessageIndex: features.contains("mx"),
        useChunkIndex: features.contains("chx"),
        startChannelID: 1
      )
    )
    for record in testRecords {
      switch record {
      case let .header(header):
        await writer.start(library: header.library, profile: header.profile)
      case let .schema(schema):
        _ = await writer.addSchema(name: schema.name, encoding: schema.encoding, data: schema.data)
      case let .channel(channel):
        _ = await writer.addChannel(
          schemaID: channel.schemaID,
          topic: channel.topic,
          messageEncoding: channel.messageEncoding,
          metadata: channel.metadata
        )
      case let .message(message):
        await writer.addMessage(message)
      case let .attachment(attachment):
        await writer.addAttachment(attachment)
      case let .metadata(metadata):
        await writer.addMetadata(metadata)
      case .dataEnd:
        await writer.end()
      }
    }

    if #available(macOS 10.15.4, *) {
      try FileHandle.standardOutput.write(contentsOf: buffer.data)
    } else {
      FileHandle.standardOutput.write(buffer.data)
    }
  }
}
