#include <mcap/mcap.hpp>

#include <nlohmann/json.hpp>

#include <iostream>

using json = nlohmann::json;

int main(int argc, char** argv) {
  if (argc != 2) {
    std::cerr << "Usage: " << argv[0] << " <input.mcap>\n";
    return 1;
  }

  json recordsJson = json::array();

  const std::string inputFile = argv[1];
  std::ifstream input(inputFile, std::ios::binary);
  mcap::FileStreamReader dataSource{input};
  mcap::TypedRecordReader reader{dataSource, 8};

  reader.onHeader = [&](const mcap::Header& header) {
    recordsJson.push_back(json::object({
      {"type", "Header"},
      {"fields", {{"library", header.library}, {"profile", header.profile}}},
    }));
  };

  while (reader.next()) {
  }

  json output = {{"records", recordsJson}};
  std::cout << output.dump(2) << "\n";
  return 0;
}
