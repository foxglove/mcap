// Example code for reading JSON messages from an MCAP file.
#define MCAP_IMPLEMENTATION
#include <mcap/reader.hpp>

#include <nlohmann/json.hpp>

#include <string_view>

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

  std::cout << "topic\ttype\ttimestamp\tfields" << std::endl;

  for (auto it = messageView.begin(); it != messageView.end(); it++) {
    // skip any non-json-encoded messages.
    if (it->channel->messageEncoding != "json") {
      continue;
    }
    std::string_view asString(reinterpret_cast<const char*>(it->message.data),
                              it->message.dataSize);

    auto parsed = nlohmann::json::parse(asString, nullptr, false);

    if (parsed.is_discarded()) {
      std::cerr << "failed to parse JSON: " << asString << std::endl;
      reader.close();
      return 1;
    }
    if (!parsed.is_object()) {
      std::cerr << "unexpected non-object message: " << asString << std::endl;
    }
    std::cout << it->channel->topic << "\t(" << it->schema->name << ")\t[" << it->message.logTime
              << "]:\t{ ";

    for (auto kv : parsed.items()) {
      std::cout << kv.key() << " ";
    }
    std::cout << "}" << std::endl;
  }
  reader.close();
  return 0;
}
