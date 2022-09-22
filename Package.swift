// swift-tools-version:5.5
// The swift-tools-version declares the minimum version of Swift required to build this package.

import PackageDescription

let package = Package(
  name: "mcap",
  platforms: [.macOS(.v10_15)], // for async/await
  products: [
    // Products define the executables and libraries a package produces, and make them visible to other packages.
    .library(name: "mcap", targets: ["mcap"]),
    .library(name: "crc", targets: ["crc", "crc-tests"]),
    .executable(name: "conformance", targets: ["conformance"]),
  ],
  dependencies: [
    // Dependencies declare other packages that this package depends on.
    .package(url: "https://github.com/apple/swift-docc-plugin", from: "1.0.0"),

    // Use pre-release version for Heap
    .package(url: "https://github.com/apple/swift-collections", revision: "418378107c87a4b312e29a51f773ce0e4e12e199"),
  ],
  targets: [
    // Targets are the basic building blocks of a package. A target can define a module or a test suite.
    // Targets can depend on other targets in this package, and on products in packages this package depends on.
    .target(
      name: "mcap",
      dependencies: ["crc", .product(name: "Collections", package: "swift-collections")],
      path: "swift/mcap"
    ),
    .testTarget(name: "unit-tests", dependencies: ["mcap"], path: "swift/test"),
    .executableTarget(name: "conformance", dependencies: ["mcap"], path: "swift/conformance"),

    .target(name: "crc", dependencies: [], path: "swift/crc"),
    .testTarget(name: "crc-tests", dependencies: ["crc"], path: "swift/crc-tests"),
  ]
)
