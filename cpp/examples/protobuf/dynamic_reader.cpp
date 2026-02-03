// Example code for reading Protobuf messages from an MCAP file. Uses the proto definition
// from within the MCAP, with no dependency on generated headers.
// Try it out by generating some PointCloud messages with the protobuf writer example,
// and running this executable with the resulting MCAP file.
#include <google/protobuf/descriptor.pb.h>
#include <google/protobuf/descriptor_database.h>
#include <google/protobuf/dynamic_message.h>

#include <mcap/reader.hpp>
#include <memory>
#include <vector>

namespace gp = google::protobuf;

// Loads the FileDescriptorSet from a protobuf schema definition into a SimpleDescriptorDatabase.
bool LoadSchema(const mcap::SchemaPtr schema, gp::SimpleDescriptorDatabase* protoDb) {
  gp::FileDescriptorSet fdSet;
  if (!fdSet.ParseFromArray(schema->data.data(), static_cast<int>(schema->data.size()))) {
    std::cerr << "failed to parse schema data" << std::endl;
    return false;
  }
  gp::FileDescriptorProto unused;
  for (int i = 0; i < fdSet.file_size(); ++i) {
    const auto& file = fdSet.file(i);
    if (!protoDb->FindFileByName(file.name(), &unused)) {
      if (!protoDb->Add(file)) {
        std::cerr << "failed to add definition " << file.name() << "to protoDB" << std::endl;
        return false;
      }
    }
  }
  return true;
}

int main(int argc, char** argv) {
  if (argc != 2) {
    std::cerr << "Usage: " << argv[0] << " <input.mcap>" << std::endl;
    return 1;
  }
  const char* inputFilename = argv[1];

  mcap::McapReader reader;
  {
    const auto res = reader.open(inputFilename);
    if (!res.ok()) {
      std::cerr << "Failed to open " << inputFilename << " for reading: " << res.message
                << std::endl;
      return 1;
    }
  }

  auto messageView = reader.readMessages();
  gp::SimpleDescriptorDatabase protoDb;
  gp::DescriptorPool protoPool(&protoDb);
  gp::DynamicMessageFactory protoFactory(&protoPool);

  std::cout << "topic\t\ttype\t\t\ttimestamp\t\tfields" << std::endl;

  for (auto it = messageView.begin(); it != messageView.end(); it++) {
    // skip any non-protobuf-encoded messages.
    if (it->schema->encoding != "protobuf") {
      continue;
    }
    if (it->channel->messageEncoding != "protobuf") {
      std::cerr << "expected channel with ID " << it->channel->id << " and schema ID "
                << it->schema->id << "to use message encoding 'protobuf', but message encoding is "
                << it->channel->messageEncoding << std::endl;
      reader.close();
      return 1;
    }
    const gp::Descriptor* descriptor = protoPool.FindMessageTypeByName(it->schema->name);
    // If the proto descriptor is not yet loaded, load it.
    if (descriptor == nullptr) {
      if (!LoadSchema(it->schema, &protoDb)) {
        reader.close();
        return 1;
      }
      descriptor = protoPool.FindMessageTypeByName(it->schema->name);
      if (descriptor == nullptr) {
        std::cerr << "failed to find descriptor after loading pool" << std::endl;
        reader.close();
        return 1;
      }
    }
    auto message = std::unique_ptr<gp::Message>(protoFactory.GetPrototype(descriptor)->New());
    if (!message->ParseFromArray(it->message.data, static_cast<int>(it->message.dataSize))) {
      std::cerr << "failed to parse message using included foxglove.PointCloud schema" << std::endl;
      reader.close();
      return 1;
    }

    std::vector<const gp::FieldDescriptor*> fields;
    message->GetReflection()->ListFields(*message, &fields);
    std::cout << it->channel->topic << "\t(" << it->schema->name << ")\t[" << it->message.logTime
              << "]:\t{ ";
    for (const auto field : fields) {
      std::cout << field->name() << " ";
    }
    std::cout << "}" << std::endl;
  }
  reader.close();
  return 0;
}
