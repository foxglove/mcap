#include <mcap/mcap.hpp>

#include <fstream>
#include <iostream>
#include <string>

int main(int argc, char* argv[]) {
  if (argc != 2) {
    std::cerr << "Usage: " << argv[0] << " <input.mcap>\n";
    return 1;
  }

  const std::string inputFile = argv[1];

  mcap::McapReader reader;

  mcap::McapReaderOptions options{};

  std::ifstream input(inputFile, std::ios::binary);
  auto status = reader.open(input, options);
  if (!status.ok()) {
    std::cerr << "Failed to open input file: " << status.message << "\n";
    reader.close();
    return 1;
  }

  std::cout << inputFile << ": " << reader.dataSource()->size()
            << " bytes. library=" << reader.header()->library
            << ", profile=" << reader.header()->profile << "\n";

  reader.close();
  return 0;
}
