from conans import ConanFile, CMake


class McapBenchmarksConan(ConanFile):
    settings = "os", "compiler", "build_type", "arch"
    generators = "cmake"
    requires = "benchmark/1.7.0", "mcap/2.1.2"

    def build(self):
        cmake = CMake(self)
        cmake.configure()
        cmake.build()
