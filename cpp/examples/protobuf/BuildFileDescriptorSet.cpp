#include "BuildFileDescriptorSet.h"

#include <queue>
#include <unordered_set>

namespace foxglove {

/// Builds a FileDescriptorSet of this descriptor and all transitive dependencies, for use as a
/// channel schema.
google::protobuf::FileDescriptorSet BuildFileDescriptorSet(
  const google::protobuf::Descriptor* toplevelDescriptor) {
  google::protobuf::FileDescriptorSet fdSet;
  std::queue<const google::protobuf::FileDescriptor*> toAdd;
  toAdd.push(toplevelDescriptor->file());
  std::unordered_set<std::string> seenDependencies;
  while (!toAdd.empty()) {
    const google::protobuf::FileDescriptor* next = toAdd.front();
    toAdd.pop();
    next->CopyTo(fdSet.add_file());
    for (int i = 0; i < next->dependency_count(); ++i) {
      const auto& dep = next->dependency(i);
      if (seenDependencies.emplace(dep->name()).second) {
        toAdd.push(dep);
      }
    }
  }
  return fdSet;
}

}  // namespace foxglove
