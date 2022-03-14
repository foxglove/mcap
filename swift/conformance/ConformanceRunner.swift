import Foundation
import mcap

//extension Header: Decodable {
//  public init(from decoder: Decoder) throws {
//    var library: String
//    var profile: String
//    for field in try decoder.unkeyedContainer().decode(<#T##type: String.Type##String.Type#>)
//  }
//}
//
//
//enum TestRecord: Decodable {
//  case header(Header)
//  case attachment(Attachment)
//  case schema(Schema)
//  case channel(Channel)
//  case message(Message)
//  case dataEnd(DataEnd)
//  case ignored
//
//  enum CodingKeys: String, CodingKey {
//    case type
//    case fields
//  }
//
//  init(from decoder: Decoder) throws {
//    let container = try decoder.container(keyedBy: CodingKeys.self)
//    switch try container.decode(String.self, forKey: .type) {
//    case "Header":
//      self = .header(try container.decode(Header.self, forKey: .fields))
//    case "Attachment":
//    case "Schema":
//    case "Channel":
//    case "Message":
//    case "DataEnd":
//      self = .dataEnd(<#T##DataEnd#>)
//    default:
//      self = .ignored
//    }
//  }
//}
//
//struct TestData: Decodable {
//  let records: [TestRecord]
//}

class Buffer: IWritable {
  var data = Data()
  func position() -> UInt64 {
    return UInt64(data.count)
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

@main
enum ConformanceRunner {
  static func main() async throws {
    if CommandLine.arguments.count < 2 {
      fatalError("Usage: conformance [test-data.json]")
    }
    let filename = CommandLine.arguments[1]
    let data = try Data(contentsOf: URL(fileURLWithPath: filename))

    let testData = try JSONSerialization.jsonObject(with: data) as! [String: Any]
    let features =
      (((testData["meta"] as! [String: Any])["variant"] as! [String: Any]))["features"] as! [String]

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
            contentType: fieldsByName["content_type"] as! String,
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
      case .header(let header):
        await writer.start(library: header.library, profile: header.profile)
      case .schema(let schema):
        _ = await writer.addSchema(name: schema.name, encoding: schema.encoding, data: schema.data)
      case .channel(let channel):
        _ = await writer.addChannel(
          schemaID: channel.schemaID,
          topic: channel.topic,
          messageEncoding: channel.messageEncoding,
          metadata: channel.metadata
        )
      case .message(let message):
        await writer.addMessage(message)
      case .attachment(let attachment):
        await writer.addAttachment(attachment)
      case .metadata(let metadata):
        await writer.addMetadata(metadata)
      case .dataEnd:
        await writer.end()
      }
    }

    buffer.data.withUnsafeBytes { bytes in
      let ret = fwrite(bytes.baseAddress, 1, bytes.count, stdout)
      if ret != bytes.count {
        fatalError("Only wrote \(ret) of \(bytes.count) bytes")
      }
    }
  }
}
