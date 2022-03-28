@main
enum Conformance {
  static func main() async throws {
    if CommandLine.arguments.count < 2 {
      fatalError("Usage: conformance [read|write] ...")
    }
    switch CommandLine.arguments[1] {
    case "read":
      try await ReadConformanceRunner.main()
    case "write":
      try await WriteConformanceRunner.main()
    default:
      fatalError("Usage: conformance [read|write] ...")
    }
  }
}
