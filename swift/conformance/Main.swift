@main
enum Conformance {
  static func main() async throws {
    if CommandLine.arguments.count < 2 {
      fatalError("Usage: conformance [read-streamed|read-indexed|write] ...")
    }
    switch CommandLine.arguments[1] {
    case "read-streamed":
      try await ReadConformanceRunner.main(mode: .streamed)
    case "read-indexed":
      try await ReadConformanceRunner.main(mode: .indexed)
    case "write":
      try await WriteConformanceRunner.main()
    default:
      fatalError("Usage: conformance [read-streamed|read-indexed|write] ...")
    }
  }
}
