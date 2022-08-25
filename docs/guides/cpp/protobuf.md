# Reading and writing Protobuf

!!! info

    Adapted from the original: [Recording Robocar Data with MCAP](https://foxglove.dev/blog/recording-robocar-data-with-mcap)

## Reading

To read Protobuf messages from an MCAP file using C++, we have two options:

- **Static** – Use statically generated class definitions to deserialize the MCAP file

  Best when there is existing code that uses these Protobuf classes. For example, if you have a simulation that drives a planning module with recorded messages, you already have generated class definitions. It makes sense to take advantage of Protobuf's existing [compatibility mechanisms](https://developers.google.com/protocol-buffers/docs/cpptutorial?hl=en#extending-a-protocol-buffer) and use those definitions to deserialize the MCAP data.

- **Dynamic** – Dynamically read fields using the schema definitions in the MCAP file

  Preferred for inspecting and debugging message content. For example, when building a [visualization tool](https://studio.foxglove.dev), we want to provide a full view of all fields in a message as it was originally recorded. We can use Protobuf's [`DynamicMessage`](https://developers.google.com/protocol-buffers/docs/reference/cpp/google.protobuf.dynamic_message) class to enumerate and inspect message fields in this way.

### Statically generated class definitions

First, we generate our class definitions and include the relevant header:

```cpp
#include "foxglove/PosesInFrame.pb.h"
```

We also include the MCAP reader implementation:

```cpp
#define MCAP_IMPLEMENTATION
#include "mcap/reader.hpp"
```

Use the `mcap::McapReader::open()` method to open an MCAP file for reading:

```cpp
mcap::McapReader reader;
{
  const auto res = reader.open(inputFilename);
  if (!res.ok()) {
    std::cerr << "Failed to open " << inputFilename << " for reading: " << res.message
              << std::endl;
    return 1;
  }
}
```

Use a `mcap::MessageView` to iterate through all of the messages in the MCAP file:

```cpp
auto messageView = reader.readMessages();
for (auto it = messageView.begin(); it != messageView.end(); it++) {
  // skip messages that we can't use
  if ((it->schema->encoding != "protobuf") || it->schema->name != "foxglove.PosesInFrame") {
    continue;
  }
  foxglove::PosesInFrame path;
  if (!path.ParseFromArray(static_cast<const void*>(it->message.data),
                                  it->message.dataSize)) {
    std::cerr << "could not parse PosesInFrame" << std::endl;
    return 1;
  }
  std::cout << "Found message: " << path.ShortDebugString() << std::endl;
  // print out the message
}
```

Finally, we close the reader:

```cpp
reader.close();
```

### Dynamically read fields

To read message fields dynamically, we must first include the relevant headers:

```cpp
#include <google/protobuf/descriptor.pb.h>
#include <google/protobuf/descriptor_database.h>
#include <google/protobuf/dynamic_message.h>

#define MCAP_IMPLEMENTATION
#include "mcap/reader.hpp"

namespace gp = google::protobuf;
```

Then, we construct our `mcap::McapReader` and `mcap::MessageView` in the same way as before:

```cpp
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
```

#### Load schema definitions

We build a `DynamicMessageFactory`, using a `google::Protobuf::SimpleDescriptorDatabase` as the underlying descriptor database. By constructing this ourselves and retaining a reference to the database, we can more easily load that database with definitions from the MCAP file.

```cpp
gp::SimpleDescriptorDatabase protoDb;
gp::DescriptorPool protoPool(&protoDb);
gp::DynamicMessageFactory protoFactory(&protoPool);
```

Now we're ready to iterate through the messages in the MCAP file. We want to load every message's `FileDescriptorSet` into the `DescriptorDatabase`, if it hasn't been already:

```cpp
for (auto it = messageView.begin(); it != messageView.end(); it++) {
  const gp::Descriptor* descriptor = protoPool.FindMessageTypeByName(it->schema->name);
  if (descriptor == nullptr) {
    if (!LoadSchema(it->schema, &protoDb)) {
      reader.close();
      return 1;
    }
```

Next, let's define our `LoadSchema()` helper function:

```cpp
bool LoadSchema(const mcap::SchemaPtr schema, gp::SimpleDescriptorDatabase* protoDb) {
  gp::FileDescriptorSet fdSet;
  if (!fdSet.ParseFromArray(static_cast<const void*>(schema->data.data()), schema->data.size())) {
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
```

#### Print messages

Once the `FileDescriptorSet` is loaded, we can get the descriptor by name:

```cpp
descriptor = protoPool.FindMessageTypeByName(it->schema->name);
```

We can use this descriptor to parse our message:

```cpp
gp::Message* message = protoFactory.GetPrototype(descriptor)->New();
if (!message->ParseFromArray(static_cast<const void*>(it->message.data),
                              it->message.dataSize)) {
  std::cerr << "failed to parse message using included schema" << std::endl;
  reader.close();
  return 1;
}
std::cout << message->ShortDebugString() << std::endl;
```

Finally, we close the reader:

```cpp
reader.close();
```

## Writing

Create an MCAP writer to start writing your Protobuf messages:

```cpp
mcap::McapWriter writer;
mcap::McapWriterOptions opts("protobuf");
auto s = writer.open("output.mcap");
if (!s.ok) {
  std::cerr << "Failed to open mcap writer: " << status.message << "\n";
  throw std::runtime_error("could not open mcap writer");
}
```

Configure it to your desired specifications using `McapWriterOptions`. For example, `opts.compressionLevel = mcap::CompressionLevel::Fast` will customize your writer to use a faster compression level.

### Register schema

Before we can write messages, we need to register a schema.

You must use the fully-qualified name of the message type (e.g. `foxglove.PosesInFrame`) and provide a serialized `google::protobuf::FileDescriptorSet` for the schema itself. Generated Protobuf messages will contain enough information to reconstruct this `FileDescriptorSet` schema at runtime:

```cpp
// Recursively adds all `fd` dependencies to `fd_set`.
void fdSetInternal(google::protobuf::FileDescriptorSet& fd_set,
                   std::unordered_set<std::string>& files,
                   const google::protobuf::FileDescriptor* fd) {
  for (int i = 0; i < fd->dependency_count(); ++i) {
    const auto* dep = fd->dependency(i);
    auto [_, inserted] = files.insert(dep->name());
    if (!inserted) continue;
    fdSetInternal(fd_set, files, fd->dependency(i));
  }
  fd->CopyTo(fd_set.add_file());
}

// Returns a serialized google::protobuf::FileDescriptorSet containing
// the necessary google::protobuf::FileDescriptor's to describe d.
std::string fdSet(const google::protobuf::Descriptor* d) {
  std::string res;
  std::unordered_set<std::string> files;
  google::protobuf::FileDescriptorSet fd_set;
  fdSetInternal(fd_set, files, d->file());
  return fd_set.SerializeAsString();
}

mcap::Schema createSchema(const google::protobuf::Descriptor* d) {
  mcap::Schema schema(d->full_name(), "protobuf", fdSet(d));
  return schema;
}

// Create a schema for the foxglove.PosesInFrame message.
mcap::Schema path_schema = createSchema(foxglove::PosesInFrame::descriptor());
writer.addSchema(path_schema);  // Assigned schema id is written to path_schema.id
```

### Register channel

Next, we'll register a channel to write our messages to:

```cpp
mcap::Channel path_channel("/planner/path", "protobuf", path_schema.id);
mcap.addChannel(path_channel);  // Assigned channel id written to path_channel.id
```

### Write messages

We can now finally write messages to the channel via its id:

```cpp
foxglove::PosesInFrame poses_msg;
// Fill in path_msg.
uint64_t timestamp_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(
                                std::chrono::system_clock::now().time_since_epoch())
                                .count();
poses_msg.mutable_timestamp()->set_seconds(timestamp_ns / 1'000'000'000ull)
poses_msg.mutable_timestamp()->set_nanos(timestamp_ns % 1'000'000'000ull)
poses_msg.set_frame_id("base_link")
// Example path in a straight line down the X axis
for (int i = 0; i < 10; ++i) {
  auto pose = poses_msg.add_poses();
  pose->mutable_position()->set_x(i);
  pose->mutable_position()->set_y(0);
  pose->mutable_position()->set_z(0);
  pose->mutable_orientation()->set_x(0);
  pose->mutable_orientation()->set_y(0);
  pose->mutable_orientation()->set_z(0);
  pose->mutable_orientation()->set_w(1);
}

std::string data = poses_msg.SerializeAsString();
mcap::Message msg;
msg.channelId = path_channel.id;
msg.logTime = timestamp_ns;
msg.publishTime = msg.logTime;
msg.data = reinterpret_cast<const std::byte*>(data.data());
msg.dataSize = data.size();

writer.write(msg);
```

Don’t forget to close the writer when you’re done:

```cpp
writer.close();
```

### Inspect MCAP file

Now, we can inspect our output MCAP file's messages. Use the _Data source_ dialog in [Foxglove Studio](https://studio.foxglove.dev) to “Open local file”.

Add a few relevant panels ([Plot](https://foxglove.dev/docs/studio/panels/plot), [Image](https://foxglove.dev/docs/studio/panels/image), [Raw Messages](https://foxglove.dev/docs/studio/panels/raw-messages), [3D](https://foxglove.dev/docs/studio/panels/3d)) to visualize the robot's performance.

## Important links

- [Example code](https://github.com/foxglove/mcap/tree/main/cpp/examples/protobuf)
- [Foxglove schemas](https://github.com/foxglove/schemas)
