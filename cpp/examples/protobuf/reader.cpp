
// Example code for writing Protobuf messages from an MCAP file. Uses the proto definition
// from within the MCAP, with no dependency on generated headers.
#define MCAP_IMPLEMENTATION
#include "mcap/reader.hpp"

#include <google/protobuf/descriptor.pb.h>
#include <google/protobuf/dynamic_message.h>

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

  bool poolLoaded = false;
  for (auto it = messageView.begin(); it != messageView.end(); it++) {
    // skip any messages that aren't PointCloud messages.
    if ((it->schema->encoding != "protobuf") || (it->schema->name != "foxglove.PointCloud")) {
      continue;
    }
    // The first time we encounter a PointCloud message, we load its schema into the
    // DescriptorPool to create Message instances with its type.
    if (!poolLoaded) {
      gp::FileDescriptorSet fdSet;
      size_t size = it->schema->data.size();
      if (!fdSet.ParseFromArray(static_cast<const void*>(&(it->schema->data[0])), size)) {
        std::cerr << "failed to parse schema data" << std::endl;
        reader.close();
        return 1;
      }
      if (!WriteFileDescriptorsToPool(&fdSet, &protoPool)) {
        std::cerr << "failed to insert proto schema into descriptor pool" << std::endl;
        reader.close();
        return 1;
      }
      poolLoaded = true;
    }
    auto descriptor = protoPool.FindMessageTypeByName("foxglove.PointCloud");
    if (descriptor == nullptr) {
      std::cerr << "failed to create PointCloud desriptor after loading pool" << std::endl;
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
    auto reflection = message->GetReflection();
    auto frameIdDescriptor = descriptor->FindFieldByName("frame_id");
    std::cout << it->message.logTime << ": pointcloud message with frame_id: "
              << reflection->GetString(*message, frameIdDescriptor) << std::endl;
  }
  reader.close();
  return 0;
}
