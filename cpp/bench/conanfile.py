from conans import ConanFile, CMake


class McapBenchmarksConan(ConanFile):
    settings = "os", "compiler", "build_type", "arch"
    generators = "cmake"
    requires = "benchmark/1.7.0", "mcap/1.5.0"

    def build(self):
        cmake = CMake(self)
        cmake.configure()
        cmake.build()
