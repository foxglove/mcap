# Writing Protobuf to MCAP

!!! info

    From [Recording Robocar Data with MCAP](https://foxglove.dev/blog/recording-robocar-data-with-mcap)

### Creating a writer

```cpp
mcap::McapWriter writer;
mcap::McapWriterOptions opts("protobuf");
auto s = writer.open("output.mcap");
if (!s.ok) {
  std::cerr << "Failed to open mcap writer: " << status.message << "\n";
  throw std::runtime_error("could not open mcap writer");
}
```

Configure your writer to your desired specifications using `McapWriterOptions`. For example, `opts.compressionLevel = mcap::CompressionLevel::Fast` customizes your writer to use a faster compression level.

### Register schema

Before we can write messages, we need to register a schema and a channel to write our messages to.

To register a schema in Protobuf, you must use the fully-qualified name of the message type (e.g. `ros.nav_msgs.Path`) and provide a serialized `google::protobuf::FileDescriptorSet` for the schema itself. Generated Protobuf messages will contain enough information to reconstruct this `FileDescriptorSet` schema at runtime.

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

// Create a schema for the ros.nav_msgs.Path message.
mcap::Schema path_schema = createSchema(ros::nav_msgs::Path::descriptor());
writer.addSchema(path_schema);  // Assigned schema id is written to path_schema.id
```

### Register channel

```cpp
mcap::Channel path_channel("/planner/path", "protobuf", path_schema.id);
mcap.addChannel(path_channel);  // Assigned channel id written to path_channel.id
```

### Write messages

We can now finally write messages to the channel via its id:

```cpp
ros::nav_msgs::Path path_msg;
// Fill in path_msg.
std::string data = path_msg.SerializeAsString();

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

Now, we can inspect our output MCAP file's messages. I used the _Data source_ dialog in Foxglove Studio to “Open local file”.

I then added a few relevant panels ([Plot](/docs/studio/panels/plot), [Image](/docs/studio/panels/image), [Raw Messages](/docs/studio/panels/raw-messages), [3D](/docs/studio/panels/3d)) to visualize my robot's performance.

## Important links

- [Example code](https://github.com/foxglove/mcap/tree/main/cpp/examples/protobuf)
