// swift-tools-version:5.5

import PackageDescription

let package = Package(
  name: "mcap",
  platforms: [.macOS(.v10_15)], // for async/await
  products: [
    .library(name: "MCAP", targets: ["MCAP"]),
  ],
  dependencies: [
    .package(url: "https://github.com/apple/swift-docc-plugin", from: "1.0.0"),
    .package(url: "https://github.com/apple/swift-algorithms", from: "1.0.0"),

    // Use pre-release version for Heap
    .package(url: "https://github.com/apple/swift-collections", revision: "418378107c87a4b312e29a51f773ce0e4e12e199"),
  ],
  targets: [
    .target(
      name: "MCAP",
      dependencies: [
        "CRC",
        .product(name: "HeapModule", package: "swift-collections"),
        .product(name: "Algorithms", package: "swift-algorithms"),
      ],
      path: "swift/mcap"
    ),
    .testTarget(name: "unit-tests", dependencies: ["MCAP"], path: "swift/test"),
    .executableTarget(name: "conformance", dependencies: ["MCAP"], path: "swift/conformance"),

    .target(name: "CRC", dependencies: [], path: "swift/crc"),
    .testTarget(name: "crc-tests", dependencies: ["CRC"], path: "swift/crc-tests"),
  ]
)
