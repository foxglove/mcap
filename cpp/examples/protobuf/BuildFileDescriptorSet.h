#pragma once

#include <google/protobuf/descriptor.h>
#include <google/protobuf/descriptor.pb.h>

#include <string>

namespace foxglove {

/// Builds a FileDescriptorSet of this descriptor and all transitive dependencies, for use as a
/// channel schema.
google::protobuf::FileDescriptorSet BuildFileDescriptorSet(
  const google::protobuf::Descriptor* toplevelDescriptor);

}  // namespace foxglove
