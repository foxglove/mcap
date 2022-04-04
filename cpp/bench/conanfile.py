from conans import ConanFile, CMake


class McapBenchmarksConan(ConanFile):
    settings = "os", "compiler", "build_type", "arch"
    generators = "cmake"
    requires = "benchmark/1.6.0", "mcap/0.1.1"

    def build(self):
        cmake = CMake(self)
        cmake.configure()
        cmake.build()
