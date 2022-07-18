// Example code for reading Protobuf messages from an MCAP file. Uses the proto definition
// from within the MCAP, with no dependency on generated headers.
// Try it out by generating some PointCloud messages with the protobuf writer example,
// and running this executable with the resulting MCAP file.
#define MCAP_IMPLEMENTATION
#include <google/protobuf/descriptor.pb.h>
#include <google/protobuf/dynamic_message.h>

#include "mcap/reader.hpp"
#include <functional>
#include <unordered_set>
#include <vector>

namespace gp = google::protobuf;

bool WriteFileDescriptorsToPool(const gp::FileDescriptorSet* fdSet, gp::DescriptorPool* pool) {
  // The FileDescriptorSet structure contains a repeated field of FileDescriptorProto instances.
  // Adding these to the DescriptorPool should be as simple as calling pool->BuildFile,
  // however BuildFile fails if any of a FileDescriptorProto's dependencies have not already been
  // added.
  // Therefore, do a topological sort first to put the FileDescriptorProtos in dependency order.
  // we use the technique described here:
  // https://en.wikipedia.org/wiki/Topological_sorting#Depth-first_search
  std::vector<const gp::FileDescriptorProto*> sorted;
  std::unordered_set<std::string> marked;
  std::unordered_set<std::string> added;

  std::function<bool(const gp::FileDescriptorProto*)> visit;
  visit = [&](const gp::FileDescriptorProto* fd) {
    if (added.find(fd->name()) != added.end()) {
      return true;
    }
    if (marked.find(fd->name()) != marked.end()) {
      std::cerr << "encountered circular descriptor dependency including name " << fd->name()
                << std::endl;
      return false;
    }
    marked.insert(fd->name());

    for (int i = 0; i < fd->dependency_size(); ++i) {
      const auto dependency_name = fd->dependency(i);
      const gp::FileDescriptorProto* dependencyProto = nullptr;
      for (int j = 0; j < fdSet->file_size(); ++j) {
        if (fdSet->file(j).name() == dependency_name) {
          dependencyProto = &fdSet->file(j);
        }
      }
      if (dependencyProto == nullptr) {
        std::cerr << "could not find proto descriptor by name: " << dependency_name << std::endl;
        return false;
      }
      if (!visit(dependencyProto)) {
        return false;
      }
    }
    sorted.push_back(fd);
    marked.erase(fd->name());
    added.insert(fd->name());
    return true;
  };

  while (sorted.size() < size_t(fdSet->file_size())) {
    for (int i = 0; i < fdSet->file_size(); ++i) {
      const gp::FileDescriptorProto* next = &fdSet->file(i);
      if (marked.find(next->name()) == marked.end()) {
        if (!visit(next)) {
          return false;
        }
      }
    }
  }
  for (const auto* descriptorProto : sorted) {
    if (pool->BuildFile(*descriptorProto) == nullptr) {
      std::cerr << "failed to insert descriptor into pool: " << descriptorProto->name()
                << std::endl;
      return false;
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
  gp::DescriptorPool protoPool;
  gp::DynamicMessageFactory protoFactory(&protoPool);

  std::cout << "topic\t\ttype\t\t\ttimestamp\t\tfields" << std::endl;

  for (auto it = messageView.begin(); it != messageView.end(); it++) {
    // skip any non-protobuf-encoded messages.
    if (it->schema->encoding != "protobuf") {
      continue;
    }
    // If the proto descriptor is not yet loaded, load it.
    if (protoPool.FindMessageTypeByName(it->schema->name) == nullptr) {
      gp::FileDescriptorSet fdSet;
      size_t size = it->schema->data.size();
      if (!fdSet.ParseFromArray(static_cast<const void*>(&(it->schema->data[0])), size)) {
        std::cerr << "failed to parse schema data" << std::endl;
        reader.close();
        return 1;
      }
      if (!WriteFileDescriptorsToPool(&fdSet, &protoPool)) {
        std::cerr << "failed to insert file descriptor set for type " << it->schema->name
                  << " into descriptor pool" << std::endl;
        reader.close();
        return 1;
      }
    }
    auto descriptor = protoPool.FindMessageTypeByName(it->schema->name);
    if (descriptor == nullptr) {
      std::cerr << "failed to find descriptor after loading pool" << std::endl;
      reader.close();
      return 1;
    }
    gp::Message* message = protoFactory.GetPrototype(descriptor)->New();
    if (!message->ParseFromArray(static_cast<const void*>(it->message.data),
                                 it->message.dataSize)) {
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
